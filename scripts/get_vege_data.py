import re
import io
import os
import requests
import pandas as pd
import logging
import time
import random
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any
from dataclasses import dataclass, asdict
from pathlib import Path
from bs4 import BeautifulSoup
from google.cloud import storage, bigquery
from google.api_core import exceptions as gcp_exceptions
from dotenv import load_dotenv
import concurrent.futures
import hashlib
import psutil

# コスト監視機能をインポート
try:
    from cost_monitor import CostMonitor
    COST_MONITORING_ENABLED = True
except ImportError as e:
    logging.warning(f"Cost monitoring module not available: {str(e)}")
    logging.info("Install additional packages for cost monitoring: pip install google-cloud-monitoring google-cloud-billing")
    COST_MONITORING_ENABLED = False

# 統合ログ設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class MarketDataConfig:
    # 市場データ取得設定
    project_id: str
    gcs_bucket: str
    gcs_prefix: str
    bq_dataset: str
    bq_table: str
    base_url: str = "https://www.shijou.metro.tokyo.lg.jp/torihiki/geppo"
    sleep_range: Tuple[float, float] = (1.0, 3.0)
    max_retry: int = 5
    backoff_factor: float = 2.0
    request_timeout: int = 120
    max_workers: int = 4
    chunk_size: int = 1000
    
    @classmethod
    def from_env(cls) -> 'MarketDataConfig':
        # 環境変数から設定を作成
        load_dotenv()
        
        required_vars = ["GCP_PROJECT_ID", "GCS_BUCKET", "BQ_DATASET", "BQ_TABLE"]
        for var in required_vars:
            if not os.environ.get(var):
                raise ValueError(f"{var} environment variable is required")
        
        gcs_prefix = os.getenv("GCS_PREFIX", "").strip().rstrip("/")
        
        return cls(
            project_id=os.environ["GCP_PROJECT_ID"],
            gcs_bucket=os.environ["GCS_BUCKET"],
            gcs_prefix=gcs_prefix,
            bq_dataset=os.environ["BQ_DATASET"],
            bq_table=os.environ["BQ_TABLE"],
            max_workers=int(os.getenv("MAX_WORKERS", "4")),
            chunk_size=int(os.getenv("CHUNK_SIZE", "1000"))
        )

@dataclass
class ProcessingResult:
    # 処理結果データ
    success: bool
    filename: str
    message: str
    records_processed: int = 0
    processing_time_seconds: float = 0.0
    data_quality_score: Optional[float] = None
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

class EnhancedMarketDataProcessor:
    # 市場データ処理クラス
    
    def __init__(self, config: MarketDataConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # GCPクライアント初期化
        try:
            self.storage_client = storage.Client(project=config.project_id)
            self.bucket = self.storage_client.bucket(config.gcs_bucket)
            self.bq_client = bigquery.Client(project=config.project_id)
            self.table_id = f"{config.project_id}.{config.bq_dataset}.{config.bq_table}"
        except Exception as e:
            self.logger.error(f"GCPクライアント初期化エラー: {e}")
            raise
        
        # セッション設定
        self.session = self._create_session()
        
        # パフォーマンス監視
        self.processed_files = 0
        self.total_records = 0
        self.errors_count = 0
    
    def _create_session(self) -> requests.Session:
        # 最適化されたHTTPセッションを作成
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ja,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': 'https://www.shijou.metro.tokyo.lg.jp/',
        })
        return session

    def _sleep_jitter(self) -> None:
        # ランダムな遅延でレート制限を回避
        time.sleep(random.uniform(*self.config.sleep_range))
    
    def _safe_request(self, url: str, timeout: Optional[int] = None, max_retries: Optional[int] = None) -> requests.Response:
        # 指数バックオフ付き安全な HTTP リクエスト
        timeout = timeout or self.config.request_timeout
        max_retries = max_retries or self.config.max_retry
        
        for retry in range(max_retries):
            try:
                self._sleep_jitter()
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                self.logger.debug(f"リクエスト成功: {url}")
                return response
                
            except requests.exceptions.Timeout as e:
                self.logger.warning(f"タイムアウト {retry+1}/{max_retries}: {url} - {e}")
            except requests.exceptions.ConnectionError as e:
                self.logger.warning(f"接続エラー {retry+1}/{max_retries}: {url} - {e}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:  # Too Many Requests
                    self.logger.warning(f"レート制限 {retry+1}/{max_retries}: {url}")
                elif e.response.status_code >= 500:  # Server Error
                    self.logger.warning(f"サーバーエラー {retry+1}/{max_retries}: {url} - {e}")
                else:
                    raise  # 4xx errors should not be retried
            except Exception as e:
                self.logger.warning(f"予期しないエラー {retry+1}/{max_retries}: {url} - {e}")
            
            if retry < max_retries - 1:
                wait_time = (self.config.backoff_factor ** retry) + random.uniform(1, 3)
                self.logger.info(f"リトライまで {wait_time:.1f}秒待機")
                time.sleep(wait_time)
        
        raise Exception(f"{max_retries}回のリトライ後もリクエスト失敗: {url}")

    def _check_existing_file(self, blob_path: str) -> bool:
        # ファイルの存在確認
        try:
            blob = self.bucket.blob(blob_path)
            exists = blob.exists()
            self.logger.debug(f"ファイル存在確認: {blob_path} -> {'exists' if exists else 'not found'}")
            return exists
        except Exception as e:
            self.logger.error(f"ファイル存在確認エラー: {blob_path} - {e}")
            return False

    def get_file_links(self) -> List[Dict[str, Union[int, str]]]:
        # 市場データファイルのリンク一覧を取得
        self.logger.info(f"ファイルリンク取得開始: {self.config.base_url}")
        
        try:
            response = self._safe_request(self.config.base_url)
            soup = BeautifulSoup(response.text, "html.parser")
            
            # ファイルリンクのパターンマッチング
            link_pattern = re.compile(
                r"/documents/d/shijou/"
                r"(?P<kind>[y])"
                r"(?P<year>\d{4})"
                r"(?P<month>\d{2})?"
                r"meisai(?:_[0-9]{6,8})?"
                r"(?:\.xlsx)?$"
            )
            
            files = []
            link_count = 0
            
            for tag in soup.find_all("a", href=True):
                link_count += 1
                match = link_pattern.fullmatch(tag["href"])
                if not match:
                    continue
                
                try:
                    year = int(match.group("year"))
                    month = int(match.group("month")) if match.group("month") else None
                    kind = match.group("kind")
                    
                    # ファイル情報を追加
                    file_info = {
                        "year": year,
                        "month": month,
                        "kind": kind,
                        "url": "https://www.shijou.metro.tokyo.lg.jp" + tag["href"],
                        "link_text": tag.get_text(strip=True)
                    }
                    files.append(file_info)
                    
                except (ValueError, AttributeError) as e:
                    self.logger.warning(f"リンク解析エラー: {tag.get('href', 'unknown')} - {e}")
                    continue
            
            # 年度ファイルがある場合は月別ファイルを除外
            annual_years = {f["year"] for f in files if f["month"] is None}
            if annual_years:
                files = [f for f in files if not (f["year"] in annual_years and f["month"] is not None)]
                self.logger.info(f"年度ファイルが存在する年: {sorted(annual_years)}")
            
            self.logger.info(f"ファイルリンク取得完了: {len(files)}件 (総リンク数: {link_count})")
            return files
            
        except Exception as e:
            self.logger.error(f"ファイルリンク取得エラー: {e}")
            raise

    def process_file(self, file_record: Dict[str, Union[int, str]], force_update: bool = False) -> ProcessingResult:
        # 単一ファイルを処理
        start_time = time.time()
        
        filename = (
            f"{file_record['year']}{file_record['month']:02}.csv"
            if file_record["month"] is not None
            else f"{file_record['year']}.csv"
        )
        
        blob_path = f"{self.config.gcs_prefix + '/' if self.config.gcs_prefix else ''}{filename}"
        
        try:
            # 既存ファイルチェック
            if not force_update and self._check_existing_file(blob_path):
                self.logger.info(f"既存ファイルスキップ: {filename}")
                return ProcessingResult(
                    success=True,
                    filename=filename,
                    message="skipped",
                    processing_time_seconds=time.time() - start_time
                )
            
            self.logger.info(f"ファイル処理開始: {filename} <- {file_record['url']}")
            
            # データダウンロードと前処理
            response = self._safe_request(file_record["url"])
            df = pd.read_excel(io.BytesIO(response.content), sheet_name=0, engine="openpyxl")
            
            # データ検証と変換
            processed_df, quality_score = self._process_and_validate_data(df)
            records_count = len(processed_df)
            
            # GCSアップロード
            csv_content = processed_df.to_csv(index=False, encoding="utf-8")
            blob = self.bucket.blob(blob_path)
            self.logger.info(f"GCSアップロード開始: {blob_path} ({records_count}行)")
            blob.upload_from_string(csv_content, content_type="text/csv")
            
            # BigQueryの既存データ削除
            self._delete_existing_bq_data(file_record)
            
            # BigQueryへアップロード
            self._upload_to_bigquery(blob_path)
            
            processing_time = time.time() - start_time
            self.logger.info(f"ファイル処理完了: {filename} -> {self.table_id} ({processing_time:.2f}秒)")
            
            # 統計更新
            self.processed_files += 1
            self.total_records += records_count
            
            return ProcessingResult(
                success=True,
                filename=filename,
                message="processed",
                records_processed=records_count,
                processing_time_seconds=processing_time,
                data_quality_score=quality_score
            )
            
        except Exception as e:
            self.errors_count += 1
            error_msg = f"ファイル処理エラー ({filename}): {e}"
            self.logger.error(error_msg)
            
            return ProcessingResult(
                success=False,
                filename=filename,
                message=error_msg,
                processing_time_seconds=time.time() - start_time,
                errors=[str(e)]
            )
    
    def _process_and_validate_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
        # データ処理と品質検証
        try:
            # カラム名変換
            rename_map = {
                "年月(yyyy/m) ": "year_month_slash",
                "年月(yyyymm) ": "year_month_num",
                "年": "year",
                "月": "month",
                "市場コード": "market_code",
                "市場": "market",
                "大分類コード": "major_category_code",
                "大分類": "major_category",
                "中分類コード": "sub_category_code",
                "中分類": "sub_category",
                "品目コード": "item_code",
                "品目名称": "item_name",
                "産地コード": "origin_code",
                "産地": "origin",
                "国内外区分": "domestic_foreign",
                "数量(kg)": "quantity_kg",
                "金額(円)": "amount_yen"
            }
            
            processed_df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            
            # データ品質スコア計算
            quality_score = self._calculate_data_quality_score(processed_df)
            
            # 基本的なデータクリーニング
            if 'quantity_kg' in processed_df.columns:
                processed_df['quantity_kg'] = pd.to_numeric(processed_df['quantity_kg'], errors='coerce').fillna(0)
            if 'amount_yen' in processed_df.columns:
                processed_df['amount_yen'] = pd.to_numeric(processed_df['amount_yen'], errors='coerce').fillna(0)
            
            self.logger.debug(f"データ処理完了: {len(processed_df)}行, 品質スコア: {quality_score:.2f}")
            return processed_df, quality_score
            
        except Exception as e:
            self.logger.error(f"データ処理エラー: {e}")
            raise
    
    def _delete_existing_bq_data(self, file_record: Dict[str, Union[int, str]]) -> None:
        # BigQueryの既存データを削除
        try:
            if file_record["month"] is not None:
                delete_query = f"""
                    DELETE FROM `{self.table_id}`
                    WHERE year = {file_record['year']}
                    AND month = {file_record['month']}
                """
            else:
                delete_query = f"""
                    DELETE FROM `{self.table_id}`
                    WHERE year = {file_record['year']}
                """
            
            query_job = self.bq_client.query(delete_query)
            query_job.result()
            self.logger.info(f"既存データ削除完了: {file_record['year']}/{file_record.get('month', '年間')}")
            
        except Exception as e:
            self.logger.error(f"既存データ削除エラー: {e}")
            raise
    
    def _upload_to_bigquery(self, blob_path: str) -> None:
        # BigQueryへデータアップロード
        try:
            schema = [
                bigquery.SchemaField("year_month_slash", "STRING"),
                bigquery.SchemaField("year_month_num", "STRING"),
                bigquery.SchemaField("year", "INTEGER"),
                bigquery.SchemaField("month", "INTEGER"),
                bigquery.SchemaField("market_code", "STRING"),
                bigquery.SchemaField("market", "STRING"),
                bigquery.SchemaField("major_category_code", "STRING"),
                bigquery.SchemaField("major_category", "STRING"),
                bigquery.SchemaField("sub_category_code", "STRING"),
                bigquery.SchemaField("sub_category", "STRING"),
                bigquery.SchemaField("item_code", "STRING"),
                bigquery.SchemaField("item_name", "STRING"),
                bigquery.SchemaField("origin_code", "STRING"),
                bigquery.SchemaField("origin", "STRING"),
                bigquery.SchemaField("domestic_foreign", "STRING"),
                bigquery.SchemaField("quantity_kg", "INTEGER"),
                bigquery.SchemaField("amount_yen", "INTEGER"),
            ]
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition="WRITE_APPEND",
                encoding="UTF-8",
                autodetect=False,
                schema=schema,
            )
            
            uri = f"gs://{self.config.gcs_bucket}/{blob_path}"
            load_job = self.bq_client.load_table_from_uri(
                uri, self.table_id, job_config=job_config
            )
            load_job.result()
            
            self.logger.debug(f"BigQueryアップロード完了: {uri}")
            
        except Exception as e:
            self.logger.error(f"BigQueryアップロードエラー: {e}")
            raise
    
    def _calculate_data_quality_score(self, df: pd.DataFrame) -> float:
        # データ品質スコアを計算
        try:
            total_cells = df.size
            if total_cells == 0:
                return 0.0
            
            # 欠損値の割合
            missing_ratio = df.isnull().sum().sum() / total_cells
            
            # 重複行の割合
            duplicate_ratio = df.duplicated().sum() / len(df) if len(df) > 0 else 0
            
            # 品質スコア計算 (0-100)
            quality_score = max(0, 100 - (missing_ratio * 50) - (duplicate_ratio * 30))
            
            return quality_score
            
        except Exception as e:
            self.logger.warning(f"品質スコア計算エラー: {e}")
            return 50.0  # デフォルト値
    
    def process_all_files(self, files: List[Dict[str, Union[int, str]]], force_update: bool = False) -> List[ProcessingResult]:
        # 複数ファイルを並列処理
        results = []
        total_files = len(files)
        start_time = datetime.now()
        
        self.logger.info(f"バッチ処理開始: {total_files}ファイル")
        
        # メモリ監視
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        if self.config.max_workers == 1:
            # シーケンシャル処理
            for i, file_record in enumerate(files, 1):
                self.logger.info(f"=== ファイル {i}/{total_files} ===")
                result = self.process_file(file_record, force_update)
                results.append(result)
                
                # プログレス報告
                if i % 5 == 0 or i == total_files:
                    elapsed = datetime.now() - start_time
                    progress = i / total_files * 100
                    current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                    self.logger.info(
                        f"進捗: {i}/{total_files} ({progress:.1f}%) - "
                        f"経過時間: {elapsed}, メモリ使用量: {current_memory:.1f}MB"
                    )
        else:
            # 並列処理
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                futures = {
                    executor.submit(self.process_file, file_record, force_update): file_record 
                    for file_record in files
                }
                
                for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
                    try:
                        result = future.result()
                        results.append(result)
                        
                        if i % 5 == 0 or i == total_files:
                            elapsed = datetime.now() - start_time
                            progress = i / total_files * 100
                            self.logger.info(f"進捗: {i}/{total_files} ({progress:.1f}%) - 経過時間: {elapsed}")
                            
                    except Exception as e:
                        self.logger.error(f"並列処理エラー: {e}")
                        file_record = futures[future]
                        results.append(ProcessingResult(
                            success=False,
                            filename=f"{file_record['year']}.csv",
                            message=f"並列処理エラー: {e}",
                            errors=[str(e)]
                        ))
        
        # 最終レポート
        self._generate_processing_report(results, datetime.now() - start_time, initial_memory)
        
        return results
    
    def _generate_processing_report(self, results: List[ProcessingResult], elapsed_time: timedelta, initial_memory: float) -> None:
        # 処理レポートを生成
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        skipped = [r for r in results if r.success and r.message == "skipped"]
        processed = [r for r in results if r.success and r.message == "processed"]
        
        total_records = sum(r.records_processed for r in successful)
        avg_quality = sum(r.data_quality_score or 0 for r in successful) / len(successful) if successful else 0
        current_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        self.logger.info("=== 処理完了レポート ===")
        self.logger.info(f"総ファイル数: {len(results)}")
        self.logger.info(f"成功: {len(successful)} (処理: {len(processed)}, スキップ: {len(skipped)})")
        self.logger.info(f"失敗: {len(failed)}")
        self.logger.info(f"総レコード数: {total_records:,}")
        self.logger.info(f"平均品質スコア: {avg_quality:.1f}")
        self.logger.info(f"処理時間: {elapsed_time}")
        self.logger.info(f"メモリ使用量: {initial_memory:.1f}MB → {current_memory:.1f}MB")
        
        if failed:
            self.logger.warning("失敗したファイル:")
            for result in failed[:5]:  # 最初の5つのエラーのみ表示
                self.logger.warning(f"  - {result.filename}: {result.message}")

    def filter_files_by_mode(self, files: List[Dict[str, Union[int, str]]], mode: str, 
                           start_year: Optional[int] = None, end_year: Optional[int] = None) -> List[Dict[str, Union[int, str]]]:
        # モードに応じてファイルをフィルタリング
        if not files:
            return []
        
        if mode == "latest-only":
            latest_rec = max(files, key=lambda x: (x["year"], x["month"] or 0))
            return [latest_rec]
        elif mode == "backfill":
            today = date.today()
            start_year = start_year or (today.year - 5)
            end_year = end_year or today.year
            return [f for f in files if start_year <= f["year"] <= end_year]
        elif mode == "all":
            return files
        else:
            raise ValueError(f"Unknown mode: {mode}")
    
    def estimate_processing_metrics(self, files: List[Dict[str, Union[int, str]]]) -> Dict[str, Union[int, float]]:
        # 処理メトリクスを推定
        file_count = len(files)
        time_per_file = 15.0
        size_per_file = 2.5 * 1024 * 1024
        
        total_time = file_count * time_per_file
        total_size = file_count * size_per_file
        
        return {
            "files": file_count,
            "estimated_time_seconds": total_time,
            "estimated_time_minutes": total_time / 60,
            "estimated_size_bytes": total_size,
            "estimated_size_mb": total_size / 1024 / 1024,
            "estimated_size_gb": total_size / 1024 / 1024 / 1024
        }

def main():
    # メイン実行関数
    parser = argparse.ArgumentParser(description="東京都中央卸売市場データ取得（野菜取引分析向け）")
    
    parser.add_argument("--mode", choices=["latest-only", "backfill", "all"], default="latest-only",
                       help="取得モード: latest-only=最新ファイルのみ, backfill=指定期間, all=全ファイル")
    parser.add_argument("--start-year", type=int, help="開始年")
    parser.add_argument("--end-year", type=int, help="終了年")
    parser.add_argument("--force-update", action="store_true", help="既存ファイルも強制更新")
    parser.add_argument("--dry-run", action="store_true", help="実行せずに見積もりのみ表示")
    parser.add_argument("--skip-cost-check", action="store_true", help="コスト監視をスキップ")
    parser.add_argument("--max-workers", type=int, help="並列処理数（デフォルト: 4）")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("東京都中央卸売市場データ取得システム（高性能版）")
    print("=" * 80)
    
    try:
        # 設定読み込み
        config = MarketDataConfig.from_env()
        
        # コマンドライン引数で上書き
        if args.max_workers:
            config.max_workers = args.max_workers
        
        # プロセッサー初期化
        processor = EnhancedMarketDataProcessor(config)
        
        # コスト監視チェック
        if COST_MONITORING_ENABLED and not args.skip_cost_check:
            try:
                print("\n[CHECK] 無料枠使用量チェック中...")
                monitor = CostMonitor()
                
                # 予想処理量（市場データは軽量なので0.01GB程度）
                estimated_processing_gb = 0.01
                is_safe, reason = monitor.is_safe_to_proceed(estimated_processing_gb)
                
                if not is_safe:
                    print(f"[ERROR] コスト制御により実行を停止: {reason}")
                    print("[INFO] --skip-cost-check フラグで強制実行可能（非推奨）")
                    return 1
                else:
                    print(f"[OK] 実行安全確認済み: {reason}")
                    
            except Exception as e:
                if args.dry_run:
                    print(f"[WARNING] コスト監視エラー（ドライランのため続行）: {str(e)}")
                else:
                    print(f"[ERROR] コスト監視エラー: {str(e)}")
                    print("[INFO] --skip-cost-check フラグで監視をスキップ可能")
                    return 1
        
        print(f"実行モード: {args.mode}")
        print(f"増分更新: {'無効 (強制更新)' if args.force_update else '有効'}")
        print(f"並列処理数: {config.max_workers}")
        print()
        
        # ファイルリンク取得
        all_files = processor.get_file_links()
        
        if not all_files:
            logger.error("リンク収集 0 件：サイト構造が変わった可能性")
            return 1
        
        # ファイルフィルタリング
        target_files = processor.filter_files_by_mode(all_files, args.mode, args.start_year, args.end_year)
        
        # 処理見積もり
        metrics = processor.estimate_processing_metrics(target_files)
        
        print("処理見積もり:")
        print(f"  対象ファイル数: {metrics['files']}")
        print(f"  推定処理時間: {metrics['estimated_time_minutes']:.1f}分")
        print(f"  推定データサイズ: {metrics['estimated_size_mb']:.0f}MB ({metrics['estimated_size_gb']:.2f}GB)")
        print()
        
        print("対象ファイル:")
        for i, rec in enumerate(target_files[:10], 1):
            fname = (
                f"{rec['year']}{rec['month']:02}.csv"
                if rec["month"] is not None
                else f"{rec['year']}.csv"
            )
            print(f"  {i}. {fname} ({rec['year']}/{rec['month'] or '年間'})")
        if len(target_files) > 10:
            print(f"  ... 他{len(target_files)-10}ファイル")
        print()
        
        if not args.dry_run and not args.force_update:
            print("増分更新モード:")
            print("  - 既存のGCSファイルが存在する場合はスキップします")
            print("  - 新規ファイルのみ処理されます")
            print("  - 強制更新が必要な場合は --force-update を使用してください")
            print()
        
        if args.dry_run:
            print("*** DRY RUN モード：実際の処理は実行されません ***")
            return 0
        
        print("*** 実際のデータ取得を開始します ***")
        results = processor.process_all_files(target_files, args.force_update)
        
        # 結果確認
        successful_results = [r for r in results if r.success]
        if len(successful_results) == len(target_files):
            logger.info("全ファイル処理完了")
            return 0
        else:
            failed_count = len(target_files) - len(successful_results)
            logger.error(f"{failed_count}ファイルの処理に失敗")
            return 1
        
    except Exception as e:
        logger.error(f"処理中にエラーが発生しました: {e}")
        return 1

if __name__ == "__main__":
    exit(main())