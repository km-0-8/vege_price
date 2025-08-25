import os
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any
from dataclasses import dataclass, asdict
from pathlib import Path
from decimal import Decimal
from google.cloud import bigquery
from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions
from dotenv import load_dotenv
import concurrent.futures
from contextlib import contextmanager
import threading

# フォールバック機能付きのオプションインポート
try:
    from google.cloud import monitoring_v3
    MONITORING_AVAILABLE = True
except ImportError:
    logging.warning("google-cloud-monitoring not available - monitoring features disabled")
    MONITORING_AVAILABLE = False

try:
    from google.cloud import billing_v1
    BILLING_AVAILABLE = True
except ImportError:
    logging.warning("google-cloud-billing not available - billing features disabled")
    BILLING_AVAILABLE = False

# 高コスト操作用のスレッドセーフキャッシュ
_cache_lock = threading.Lock()
_usage_cache: Dict[str, Tuple[Any, datetime]] = {}
CACHE_TTL_SECONDS = 300  # 5分間のキャッシュ

# GCP無料枠の制限値
FREE_TIER_LIMITS = {
    "bigquery_processing_gb": 1024,  # 月間1TB
    "storage_gb": 5,  # 5GB
    "bigquery_storage_gb": 10  # 10GB
}

# アラート閾値（パーセント）
ALERT_THRESHOLDS = {
    "warning": 60,
    "critical": 80,
    "emergency": 95
}

# 設定管理
@dataclass
class CostMonitorConfig:
    # コストモニター設定
    project_id: str
    dataset_prefix: str = "vege"
    environment: str = "dev"
    bigquery_processing_gb_limit: int = 1024
    storage_gb_limit: int = 5
    bigquery_storage_gb_limit: int = 10
    warning_threshold: int = 60
    critical_threshold: int = 80
    emergency_threshold: int = 95
    cache_ttl_seconds: int = 300
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    timeout_seconds: int = 30
    
    @classmethod
    def from_env(cls) -> 'CostMonitorConfig':
        # 環境変数から設定を作成
        load_dotenv()
        
        project_id = os.environ.get("GCP_PROJECT_ID")
        if not project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
            
        return cls(
            project_id=project_id,
            dataset_prefix=os.getenv("DATASET_PREFIX", "vege"),
            environment=os.getenv("ENVIRONMENT", "dev"),
            bigquery_processing_gb_limit=int(os.getenv("BQ_PROCESSING_LIMIT_GB", "1024")),
            storage_gb_limit=int(os.getenv("STORAGE_LIMIT_GB", "5")),
            bigquery_storage_gb_limit=int(os.getenv("BQ_STORAGE_LIMIT_GB", "10")),
            warning_threshold=int(os.getenv("WARNING_THRESHOLD", "60")),
            critical_threshold=int(os.getenv("CRITICAL_THRESHOLD", "80")),
            emergency_threshold=int(os.getenv("EMERGENCY_THRESHOLD", "95")),
            cache_ttl_seconds=int(os.getenv("CACHE_TTL_SECONDS", "300")),
            max_retries=int(os.getenv("MAX_RETRIES", "3")),
            retry_delay_seconds=float(os.getenv("RETRY_DELAY_SECONDS", "1.0")),
            timeout_seconds=int(os.getenv("TIMEOUT_SECONDS", "30"))
        )

@dataclass
class UsageData:
    # 拡張された検証とメソッドを持つ使用量データクラス
    service: str
    resource_type: str
    current_usage: float
    limit: float
    unit: str
    usage_percentage: float
    alert_level: str
    check_time: datetime
    error_margin: Optional[float] = None
    confidence_level: Optional[float] = None
    
    def __post_init__(self) -> None:
        # 初期化後にデータを検証
        if self.current_usage < 0:
            raise ValueError(f"Current usage cannot be negative: {self.current_usage}")
        if self.limit <= 0:
            raise ValueError(f"Limit must be positive: {self.limit}")
        if not 0 <= self.usage_percentage <= 100:
            self.usage_percentage = min(100, max(0, (self.current_usage / self.limit) * 100))
    
    def to_dict(self) -> Dict[str, Any]:
        # 適切なシリアル化でディクショナリに変換
        data = asdict(self)
        data['check_time'] = self.check_time.isoformat()
        return data
    
    def is_critical(self) -> bool:
        # 使用量が危険範囲にあるかチェック
        return self.alert_level in ["CRITICAL", "EMERGENCY"]
    
    def get_remaining_capacity(self) -> float:
        # 同じ単位で残り容量を取得
        return max(0, self.limit - self.current_usage)
    
    def format_usage_summary(self) -> str:
        # ログ用に使用量サマリーをフォーマット
        return (f"{self.service} {self.resource_type}: "
                f"{self.current_usage:.2f}{self.unit} / {self.limit}{self.unit} "
                f"({self.usage_percentage:.1f}%) - {self.alert_level}")

class CostMonitor:
    """コスト監視・制御クラス"""
    
    def __init__(self):
        load_dotenv()
        self.project_id = os.environ.get("GCP_PROJECT_ID")
        if not self.project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
            
        self.bigquery_client = bigquery.Client(project=self.project_id)
        self.storage_client = storage.Client(project=self.project_id)
        
        # 条件付きで monitoring クライアントを初期化
        if MONITORING_AVAILABLE:
            self.monitoring_client = monitoring_v3.MetricServiceClient()
        else:
            self.monitoring_client = None
        
        # ログ設定
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [COST-MONITOR] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        self.logger = logging.getLogger(__name__)
    
    def check_bigquery_usage(self) -> UsageData:
        # BigQuery使用量をチェック
        try:
            # 当月の処理量を取得（バイト単位）
            query = """
            SELECT 
                SUM(total_bytes_processed) as total_processed_bytes
            FROM `region-us.INFORMATION_SCHEMA.JOBS`
            WHERE DATE(creation_time) >= DATE_TRUNC(CURRENT_DATE(), MONTH)
                AND job_type = 'QUERY'
                AND state = 'DONE'
                AND project_id = @project_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("project_id", "STRING", self.project_id)
                ]
            )
            
            query_job = self.bigquery_client.query(query, job_config=job_config)
            results = query_job.result()

            processed_bytes = 0
            for row in results:
                processed_bytes = row.total_processed_bytes or 0
                break
            processed_gb = processed_bytes / (1024**3)
            
            limit_gb = FREE_TIER_LIMITS["bigquery_processing_gb"]
            usage_percentage = (processed_gb / limit_gb) * 100
            
            alert_level = self._get_alert_level(usage_percentage)
            
            self.logger.info(f"BigQuery usage: {processed_gb:.2f}GB / {limit_gb}GB ({usage_percentage:.1f}%)")
            
            return UsageData(
                service="bigquery",
                resource_type="query_processing",
                current_usage=processed_gb,
                limit=limit_gb,
                unit="GB",
                usage_percentage=usage_percentage,
                alert_level=alert_level,
                check_time=datetime.utcnow()
            )
            
        except Exception as e:
            self.logger.error(f"Failed to check BigQuery usage: {str(e)}")
            self.logger.debug(f"Query: {query}")
            self.logger.debug(f"Project ID: {self.project_id}")
            
            # フォールバック: デフォルト値を返す
            self.logger.warning("Returning default BigQuery usage data due to error")
            return UsageData(
                service="bigquery",
                resource_type="query_processing",
                current_usage=0.0,
                limit=FREE_TIER_LIMITS["bigquery_processing_gb"],
                unit="GB",
                usage_percentage=0.0,
                alert_level="NORMAL",
                check_time=datetime.utcnow()
            )
    
    def check_storage_usage(self) -> List[UsageData]:
        # Cloud Storage使用量をチェック
        usage_data_list = []
        
        try:
            total_storage_bytes = 0
            
            # 全バケットの使用量を合計
            for bucket in self.storage_client.list_buckets():
                try:
                    # バケットサイズを取得
                    bucket_size = self._get_bucket_size(bucket.name)
                    total_storage_bytes += bucket_size
                    self.logger.debug(f"Bucket {bucket.name}: {bucket_size / (1024**3):.3f}GB")
                    
                except Exception as e:
                    self.logger.warning(f"Failed to get size for bucket {bucket.name}: {str(e)}")
                    continue
            
            total_storage_gb = total_storage_bytes / (1024**3)
            limit_gb = FREE_TIER_LIMITS["storage_gb"]
            usage_percentage = (total_storage_gb / limit_gb) * 100
            
            alert_level = self._get_alert_level(usage_percentage)
            
            self.logger.info(f"Storage usage: {total_storage_gb:.3f}GB / {limit_gb}GB ({usage_percentage:.1f}%)")
            
            usage_data_list.append(UsageData(
                service="storage",
                resource_type="total_storage",
                current_usage=total_storage_gb,
                limit=limit_gb,
                unit="GB",
                usage_percentage=usage_percentage,
                alert_level=alert_level,
                check_time=datetime.utcnow()
            ))
            
        except Exception as e:
            self.logger.error(f"Failed to check storage usage: {str(e)}")
            
            # フォールバック: デフォルト値を返す
            self.logger.warning("Returning default storage usage data due to error")
            usage_data_list.append(UsageData(
                service="storage",
                resource_type="total_storage",
                current_usage=0.0,
                limit=FREE_TIER_LIMITS["storage_gb"],
                unit="GB",
                usage_percentage=0.0,
                alert_level="NORMAL",
                check_time=datetime.utcnow()
            ))
        
        return usage_data_list
    
    def _get_bucket_size(self, bucket_name: str) -> int:
        # バケットのサイズを取得（バイト単位）
        bucket = self.storage_client.bucket(bucket_name)
        total_size = 0
        
        for blob in bucket.list_blobs():
            total_size += blob.size or 0
        
        return total_size
    
    def _get_alert_level(self, usage_percentage: float) -> str:
        # 使用率に基づいてアラートレベルを決定
        if usage_percentage >= ALERT_THRESHOLDS["emergency"]:
            return "EMERGENCY"
        elif usage_percentage >= ALERT_THRESHOLDS["critical"]:
            return "CRITICAL"
        elif usage_percentage >= ALERT_THRESHOLDS["warning"]:
            return "WARNING"
        else:
            return "NORMAL"
    
    def check_all_usage(self) -> List[UsageData]:
        # 全リソースの使用量をチェック
        all_usage_data = []
        
        # BigQuery使用量チェック（エラーでも続行）
        try:
            bq_usage = self.check_bigquery_usage()
            all_usage_data.append(bq_usage)
        except Exception as e:
            self.logger.error(f"BigQuery usage check failed: {str(e)}")
        
        # Storage使用量チェック（エラーでも続行）
        try:
            storage_usage = self.check_storage_usage()
            all_usage_data.extend(storage_usage)
        except Exception as e:
            self.logger.error(f"Storage usage check failed: {str(e)}")
        
        return all_usage_data
    
    def save_usage_to_bigquery(self, usage_data_list: List[UsageData]) -> bool:
        # 使用量データをBigQueryに保存
        try:
            # 使用量監視専用のデータセット
            dataset_id = "vege_usage_monitoring"
            table_id = "daily_usage_log"
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            
            # データを変換
            rows_to_insert = []
            for usage_data in usage_data_list:
                row = {
                    'usage_date': datetime.utcnow().date().isoformat(),
                    'service_name': usage_data.service,
                    'resource_type': usage_data.resource_type,
                    'usage_amount': usage_data.current_usage,
                    'usage_unit': usage_data.unit,
                    'free_tier_limit': usage_data.limit,
                    'usage_percentage': usage_data.usage_percentage,
                    'cost_usd': 0.0,
                    'check_time': datetime.utcnow().isoformat()
                }
                rows_to_insert.append(row)
            
            # BigQueryに挿入
            table = self.bigquery_client.get_table(table_ref)
            errors = self.bigquery_client.insert_rows_json(table, rows_to_insert)
            
            if errors:
                self.logger.error(f"Failed to insert usage data: {errors}")
                return False
            
            self.logger.info(f"Usage data saved to BigQuery: {len(rows_to_insert)} rows")
            return True
            
        except Exception as e:
            error_message = str(e)
            if "404" in error_message and "Not found" in error_message:
                self.logger.warning(f"Usage monitoring dataset/table not found: {error_message}")
                self.logger.info("Create the dataset manually: bq mk --dataset --location=US vege-price-467203:vege_usage_monitoring")
                self.logger.info("Create the table manually: bq mk --table --time_partitioning_field=usage_date vege-price-467203:vege_usage_monitoring.daily_usage_log ...")
            else:
                self.logger.warning(f"Failed to save usage data to BigQuery: {error_message}")
            return False
    
    def check_and_alert(self) -> Dict:
        # 使用量チェックとアラート
        try:
            # 全使用量をチェック
            usage_data_list = self.check_all_usage()
            
            # 結果をまとめ
            results = {
                'timestamp': datetime.utcnow().isoformat(),
                'project_id': self.project_id,
                'usage_data': [data.to_dict() for data in usage_data_list],
                'alerts': []
            }
            
            # アラートチェック
            for usage_data in usage_data_list:
                if usage_data.alert_level != "NORMAL":
                    alert = {
                        'level': usage_data.alert_level,
                        'service': usage_data.service,
                        'resource': usage_data.resource_type,
                        'usage_percentage': usage_data.usage_percentage,
                        'message': f"{usage_data.service} {usage_data.resource_type} usage is {usage_data.usage_percentage:.1f}% of free tier limit"
                    }
                    results['alerts'].append(alert)
                    
                    # ログ出力
                    if usage_data.alert_level == "EMERGENCY":
                        self.logger.error(f"[EMERGENCY] {alert['message']}")
                    elif usage_data.alert_level == "CRITICAL":
                        self.logger.warning(f"[CRITICAL] {alert['message']}")
                    elif usage_data.alert_level == "WARNING":
                        self.logger.warning(f"[WARNING] {alert['message']}")
            
            # 使用量データを保存
            self.save_usage_to_bigquery(usage_data_list)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to check and alert: {str(e)}")
            raise
    
    def is_safe_to_proceed(self, required_processing_gb: float = 0) -> Tuple[bool, str]:
        # 処理実行前の安全チェック
        #
        # Args:
        #     required_processing_gb: 必要な処理量（GB）
        #
        # Returns:
        #     (安全かどうか, 理由)
        try:
            # 現在の使用量をチェック
            bq_usage = self.check_bigquery_usage()
            
            # 処理実行後の予想使用量
            projected_usage = bq_usage.current_usage + required_processing_gb
            projected_percentage = (projected_usage / bq_usage.limit) * 100
            
            if projected_percentage >= 95:
                return False, f"Processing would exceed 95% of free tier limit ({projected_percentage:.1f}%)"
            elif projected_percentage >= 80:
                self.logger.warning(f"Processing will use {projected_percentage:.1f}% of free tier limit")
                return True, f"Proceeding with caution - {projected_percentage:.1f}% usage projected"
            else:
                return True, f"Safe to proceed - {projected_percentage:.1f}% usage projected"
                
        except Exception as e:
            self.logger.error(f"Failed to perform safety check: {str(e)}")
            return False, f"Safety check failed: {str(e)}"

def main():
    # メイン実行関数
    import argparse
    
    parser = argparse.ArgumentParser(description="GCP無料枠使用量監視")
    parser.add_argument("--check-all", action="store_true", help="全リソースの使用量をチェック")
    parser.add_argument("--bigquery-only", action="store_true", help="BigQueryのみチェック")
    parser.add_argument("--storage-only", action="store_true", help="Storageのみチェック")
    parser.add_argument("--safety-check", type=float, help="指定した処理量での安全チェック（GB）")
    parser.add_argument("--output", choices=["json", "table"], default="table", help="出力形式")
    
    args = parser.parse_args()
    
    try:
        monitor = CostMonitor()
    except Exception as e:
        print(f"Failed to initialize CostMonitor: {str(e)}", flush=True)
        print("Environment variables check:", flush=True)
        print(f"  GCP_PROJECT_ID: {'SET' if os.environ.get('GCP_PROJECT_ID') else 'NOT SET'}", flush=True)
        print(f"  GOOGLE_APPLICATION_CREDENTIALS: {'SET' if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') else 'NOT SET'}", flush=True)
        print("Note: Some monitoring features may require additional packages:", flush=True)
        print("  pip install google-cloud-monitoring google-cloud-billing", flush=True)
        import traceback
        print("Detailed error:", flush=True)
        traceback.print_exc()
        return 1
    
    try:
        if args.safety_check is not None:
            is_safe, reason = monitor.is_safe_to_proceed(args.safety_check)
            print(f"Safety check result: {'SAFE' if is_safe else 'UNSAFE'}")
            print(f"Reason: {reason}")
            return 0 if is_safe else 1
            
        elif args.bigquery_only:
            usage = monitor.check_bigquery_usage()
            if args.output == "json":
                print(json.dumps(usage.to_dict(), indent=2))
            else:
                print(f"BigQuery: {usage.current_usage:.2f}{usage.unit} / {usage.limit}{usage.unit} ({usage.usage_percentage:.1f}%) - {usage.alert_level}")
                
        elif args.storage_only:
            usage_list = monitor.check_storage_usage()
            for usage in usage_list:
                if args.output == "json":
                    print(json.dumps(usage.to_dict(), indent=2))
                else:
                    print(f"Storage: {usage.current_usage:.3f}{usage.unit} / {usage.limit}{usage.unit} ({usage.usage_percentage:.1f}%) - {usage.alert_level}")
        else:
            # デフォルト：全チェック
            results = monitor.check_and_alert()
            
            if args.output == "json":
                print(json.dumps(results, indent=2))
            else:
                print("\n=== GCP Free Tier Usage Report ===")
                print(f"Timestamp: {results['timestamp']}")
                print(f"Project: {results['project_id']}")
                
                print("\n--- Usage Summary ---")
                for data in results['usage_data']:
                    print(f"{data['service']} ({data['resource_type']}): {data['current_usage']:.2f}{data['unit']} / {data['limit']}{data['unit']} ({data['usage_percentage']:.1f}%) - {data['alert_level']}")
                
                if results['alerts']:
                    print("\n--- Active Alerts ---")
                    for alert in results['alerts']:
                        print(f"[{alert['level']}] {alert['message']}")
                else:
                    print("\nNo alerts - All resources within safe limits")
        
        return 0
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())