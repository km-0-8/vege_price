#!/usr/bin/env python3
# 野菜価格予測 Slack通知システム - BigQueryの予測結果から安い野菜を通知

import pandas as pd
import os
import json
import requests
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Any
from dataclasses import dataclass, asdict
from pathlib import Path
from google.cloud import bigquery
from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions
from dotenv import load_dotenv
import psutil

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
class SlackConfig:
    # Slack通知設定
    project_id: str
    dataset_prefix: str = "vege"
    environment: str = "dev"
    slack_webhook_url: Optional[str] = None
    gcs_bucket: Optional[str] = None
    price_decrease_threshold: float = 0.1  # 10%以上の価格低下
    min_confidence: float = 0.7  # 最低信頼度70%
    ml_prediction_status: str = "unknown"
    timeout_seconds: int = 30
    enable_debug: bool = False
    
    @classmethod
    def from_env(cls) -> 'SlackConfig':
        # 環境変数から設定を作成
        load_dotenv()
        
        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
        
        return cls(
            project_id=project_id,
            dataset_prefix=os.getenv("DATASET_PREFIX", "vege"),
            environment=os.getenv("ENVIRONMENT", "dev"),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
            gcs_bucket=os.getenv("GCS_BUCKET"),
            price_decrease_threshold=float(os.getenv("PRICE_DECREASE_THRESHOLD", "0.1")),
            min_confidence=float(os.getenv("MIN_CONFIDENCE", "0.7")),
            ml_prediction_status=os.getenv("ML_PREDICTION_STATUS", "unknown"),
            timeout_seconds=int(os.getenv("SLACK_TIMEOUT_SECONDS", "30")),
            enable_debug=os.getenv("SLACK_DEBUG", "false").lower() == "true"
        )

@dataclass
class NotificationResult:
    # 通知結果データ
    success: bool
    target_month: int
    predictions_count: int
    model_available: bool
    duration_seconds: float
    timestamp: str
    message_preview: Optional[str] = None
    error: Optional[str] = None
    dry_run: bool = False


class EnhancedVegetablePriceNotifier:
    # 改善された野菜価格予測 Slack通知クラス
    
    def __init__(self, config: SlackConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # BigQueryクライアント初期化
        try:
            self.bq_client = bigquery.Client(project=config.project_id)
            self.storage_client = storage.Client(project=config.project_id) if config.gcs_bucket else None
        except Exception as e:
            self.logger.error(f"GCPクライアント初期化エラー: {e}")
            raise
        
        # Webhook URL検証
        if not config.slack_webhook_url:
            self.logger.warning("SLACK_WEBHOOK_URL環境変数が未設定です")
        
        # 統計情報
        self.notifications_sent = 0
        self.last_notification_time = None
        
    def get_cheapest_vegetables_predictions(self, target_month: int = None) -> pd.DataFrame:
        # 現在日付の翌月の安い野菜 TOP10を取得（最新データ月とは無関係）
        if target_month is None:
            # 現在日付の翌月を算出（固定）
            current_date = datetime.now()
            if current_date.month == 12:
                target_month = 1
                target_year = current_date.year + 1
            else:
                target_month = current_date.month + 1
                target_year = current_date.year
            
            self.logger.info(f"Slack通知対象: 現在日付({current_date.strftime('%Y年%m月%d日')})の翌月 = {target_year}年{target_month}月")
        else:
            target_year = datetime.now().year
            if target_month < datetime.now().month:
                target_year += 1
        
        self.logger.info(f"安い野菜予測を取得中: {target_year}年{target_month}月")
        self.logger.info(f"現在日時: {datetime.now().strftime('%Y年%m月%d日')}, 検索対象: {target_year}年{target_month}月の予測データ")
        
        # fact_price_predictionsテーブルの存在確認
        table_id = f"{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.fact_price_predictions"
        try:
            table = self.bq_client.get_table(table_id)
            self.logger.info(f"予測テーブル確認: {table.num_rows}行のデータが存在")
        except Exception as e:
            self.logger.warning(f"予測テーブルが見つかりません: {e}")
            self.logger.info("ML予測処理がまだ実行されていない可能性があります")
            return pd.DataFrame()
        
        query = f"""
        WITH latest_predictions AS (
          SELECT 
            item_name,
            model_type,
            predicted_price,
            last_actual_price,
            confidence_interval,
            prediction_reliability,
            model_r2,
            training_data_points,
            prediction_date,
            prediction_generated_at,
            ROW_NUMBER() OVER (
              PARTITION BY item_name 
              ORDER BY 
                -- MASEが低いほど良い（ナイーブ予測より優秀）
                CASE WHEN model_mae IS NOT NULL AND model_mae > 0 THEN model_mae ELSE 999 END ASC,
                -- sMAPEが低いほど良い
                CASE WHEN model_smape IS NOT NULL AND model_smape > 0 THEN model_smape ELSE 999 END ASC,
                -- MAEが低いほど良い
                CASE WHEN model_mae IS NOT NULL AND model_mae > 0 THEN model_mae ELSE 999 END ASC,
                -- 旧指標（後方互換性）
                model_r2 DESC,
                CASE 
                  WHEN model_type = 'LSTM' THEN 1     -- LSTMを優先
                  WHEN model_type = 'PROPHET' THEN 2  
                  WHEN model_type = 'ARIMA' THEN 3
                  ELSE 4
                END,
                prediction_generated_at DESC
            ) as rn
          FROM `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.fact_price_predictions`
          WHERE prediction_year = @target_year
            AND prediction_month = @target_month
            AND confidence_interval >= @min_confidence
            AND prediction_reliability IN ('HIGH', 'MEDIUM')
            AND training_data_points >= 24
            AND predicted_price > 0
            AND item_name NOT LIKE '%その他%'
            AND model_type IN ('PROPHET', 'LSTM', 'ARIMA')
        )
        
        SELECT 
          p.item_name,
          p.model_type,
          ROUND(p.predicted_price, 0) as predicted_price,
          ROUND(p.last_actual_price, 0) as current_price,
          NULL as price_change_pct,
          -- 時系列予測精度表示（MASE基準）
          ROUND(
            CASE 
              WHEN p.model_mae IS NOT NULL AND p.model_mae > 0 THEN
                CASE 
                  WHEN p.model_mae < 20 THEN 95  -- EXCELLENT
                  WHEN p.model_mae < 50 THEN 85  -- GOOD  
                  WHEN p.model_mae < 100 THEN 75  -- FAIR
                  WHEN p.model_mae < 200 THEN 60  -- POOR
                  ELSE 40                          -- VERY_POOR
                END
              WHEN p.model_r2 IS NOT NULL AND p.model_r2 >= 0 THEN p.model_r2 * 100
              ELSE 0
            END, 0
          ) as confidence_pct,
          p.prediction_reliability,
          ROUND(COALESCE(p.model_mae, 999), 1) as model_mase,  -- 名前はmodel_maseだが実際はMAE値を使用
          ROUND(COALESCE(p.model_mae, 999), 1) as model_mae,
          ROUND(COALESCE(p.model_smape, 999), 1) as model_smape,
          p.training_data_points,
          p.prediction_date,
          DATE(p.prediction_generated_at) as generated_date
        FROM latest_predictions p
        WHERE p.rn = 1
        ORDER BY 
          p.predicted_price ASC,  -- 最も安い順
          p.confidence_interval DESC,
          p.model_r2 DESC
        LIMIT 10
        """
        
        try:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("target_year", "INT64", target_year),
                    bigquery.ScalarQueryParameter("target_month", "INT64", target_month),
                    bigquery.ScalarQueryParameter("min_confidence", "FLOAT64", self.config.min_confidence)
                ]
            )
            
            df = self.bq_client.query(query, job_config=job_config).to_dataframe()
            self.logger.info(f"安い野菜予測取得完了: {len(df)}品目")
            
            # 取得した予測データの詳細をログ出力
            if not df.empty:
                # prediction_dateをdatetime型に明示的に変換
                df['prediction_date'] = pd.to_datetime(df['prediction_date'])
                unique_months = df['prediction_date'].dt.strftime('%Y年%m月').unique()
                self.logger.info(f"取得した予測データの月: {', '.join(unique_months)}")
                self.logger.info(f"予測データの品目: {', '.join(df['item_name'].unique()[:5])}...")
                
                # ターゲット月が含まれているか確認
                target_month_str = f"{target_year}年{target_month:02d}月"
                if target_month_str in unique_months:
                    self.logger.info(f"✅ ターゲット月({target_month_str})のデータが見つかりました")
                else:
                    self.logger.warning(f"⚠️ ターゲット月({target_month_str})のデータがありません。利用可能: {', '.join(unique_months)}")
            else:
                self.logger.warning(f"{target_year}年{target_month}月の予測データが見つかりません")
                
                # データがない場合の詳細調査
                self.logger.info("予測テーブルの全体データを確認中...")
                try:
                    # まずRAWテーブルを確認
                    raw_check_query = f"""
                    SELECT 
                      prediction_year,
                      prediction_month,
                      COUNT(*) as count
                    FROM `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_raw.ml_price_pred`
                    WHERE prediction_year >= {target_year - 1}
                    GROUP BY prediction_year, prediction_month
                    ORDER BY prediction_year, prediction_month
                    """
                    raw_df = self.bq_client.query(raw_check_query).to_dataframe()
                    if not raw_df.empty:
                        raw_months = raw_df.apply(lambda x: f"{int(x['prediction_year'])}年{int(x['prediction_month']):02d}月({int(x['count'])}件)", axis=1).tolist()
                        self.logger.info(f"RAWテーブル(ml_price_pred)のデータ: {', '.join(raw_months)}")
                    else:
                        self.logger.warning("RAWテーブル(ml_price_pred)にデータがありません")
                    
                    # 次にMARTテーブルを確認
                    mart_check_query = f"""
                    SELECT 
                      prediction_year,
                      prediction_month,
                      COUNT(*) as count
                    FROM `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.fact_price_predictions`
                    WHERE prediction_year >= {target_year - 1}
                    GROUP BY prediction_year, prediction_month
                    ORDER BY prediction_year, prediction_month
                    """
                    mart_df = self.bq_client.query(mart_check_query).to_dataframe()
                    if not mart_df.empty:
                        mart_months = mart_df.apply(lambda x: f"{int(x['prediction_year'])}年{int(x['prediction_month']):02d}月({int(x['count'])}件)", axis=1).tolist()
                        self.logger.info(f"MARTテーブル(fact_price_predictions)のデータ: {', '.join(mart_months)}")
                    else:
                        self.logger.warning("MARTテーブル(fact_price_predictions)にデータがありません")
                        
                        # RAWテーブルにデータがある場合、dbt変換が失敗している
                        if not raw_df.empty:
                            self.logger.error("⚠️ dbt変換エラー: RAWデータは存在するがMARTテーブルが空です")
                            self.logger.error("stg_price_predictions または fact_price_predictions のdbtモデルでエラーが発生しています")
                        
                except Exception as e:
                    self.logger.error(f"データ確認エラー: {e}")
            
            return df
        except Exception as e:
            self.logger.error(f"安い野菜予測取得エラー: {e}")
            return pd.DataFrame()
    
    def get_seasonal_insights(self, target_month: int) -> Dict:
        # 季節要因による価格変動洞察を取得
        query = f"""
        SELECT 
          item_name,
          ROUND(AVG(avg_monthly_price), 0) as historical_avg_price,
          COUNT(*) as data_points,
          ROUND(STDDEV(avg_monthly_price), 0) as price_volatility
        FROM `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.mart_seasonal_analysis`
        WHERE month = @target_month
        GROUP BY item_name
        HAVING data_points >= 3
        ORDER BY price_volatility DESC
        """
        
        try:
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("target_month", "INT64", target_month)
                ]
            )
            
            df = self.bq_client.query(query, job_config=job_config).to_dataframe()
            
            insights = {
                'volatile_items': df.head(5)['item_name'].tolist(),
                'stable_items': df.tail(5)['item_name'].tolist(),
                'month_name': datetime(2023, target_month, 1).strftime('%m月')
            }
            
            self.logger.info(f"季節洞察取得完了: {target_month}月の{len(df)}品目")
            return insights
        except Exception as e:
            self.logger.error(f"季節洞察取得エラー: {e}")
            return {}
    
    def check_model_availability(self) -> Dict:
        # GCSから最新モデルの利用可能性をチェック
        if not self.config.gcs_bucket or not self.storage_client:
            return {'available': False, 'reason': 'GCS bucket not configured'}
        
        try:
            bucket = self.storage_client.bucket(self.config.gcs_bucket)
            
            # 最新のメタデータを検索
            metadata_blobs = list(bucket.list_blobs(prefix='model-metadata/'))
            if not metadata_blobs:
                return {'available': False, 'reason': 'No model metadata found'}
            
            # 最新のメタデータファイルを取得
            latest_blob = max(metadata_blobs, key=lambda x: x.time_created)
            metadata_content = latest_blob.download_as_text()
            metadata = json.loads(metadata_content)
            
            return {
                'available': True,
                'version': metadata.get('version'),
                'created_at': metadata.get('created_at'),
                'model_types': metadata.get('model_types', []),
                'items_covered': len(metadata.get('items_covered', [])),
                'gcs_path': f"gs://{self.config.gcs_bucket}/{latest_blob.name}"
            }
            
        except Exception as e:
            self.logger.error(f"モデル利用可能性チェックエラー: {e}")
            return {'available': False, 'reason': f'Error: {str(e)}'}
    
    def create_slack_message(self, predictions_df: pd.DataFrame, target_month: int, 
                           seasonal_insights: Dict, model_info: Dict) -> Dict:
        # Slack投稿用メッセージを作成
        month_name = datetime(2023, target_month, 1).strftime('%m月')
        
        if predictions_df.empty:
            # ML予測データがない場合の判定
            if self.config.ml_prediction_status == 'failure':
                message_text = f"*{month_name}の価格予測処理でエラーが発生しました* ❌\n\n次回の処理実行をお待ちください。"
            elif self.config.ml_prediction_status == 'success':
                # ML予測は成功したがdbt変換でエラーが発生している可能性
                message_text = f"*{month_name}の価格予測データが空です* ⚠️\n\n"
                message_text += "ML予測は成功しましたが、dbt変換でエラーが発生している可能性があります。\n"
                message_text += "データエンジニアに確認を依頼してください。"
            elif not model_info.get('available', False):
                message_text = f"*{month_name}の価格予測データはまだ準備中です* 🔄\n\nML予測処理の完了をお待ちください。"
            else:
                message_text = f"*{month_name}の野菜価格予測データが準備できていません* 📊\n\n予測処理の完了をお待ちください。"
            
            return {
                "text": f"🥬 {month_name}の安い野菜TOP10",
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"{month_name}の安い野菜TOP10"
                        }
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": message_text
                        }
                    }
                ]
            }
        
        # メイン予測メッセージ（安い野菜TOP10）
        predictions_text = ""
        for i, (_, row) in enumerate(predictions_df.head(10).iterrows(), 1):
            # MASE値から詳細情報を表示
            mase_detail = ""
            if 'model_mase' in row and row['model_mase'] != 999:
                if row['model_mase'] < 50:  # MAE < 50円は良好
                    mase_detail = f" | 予測誤差 {row['model_mase']:.1f}円 (良好)"
                else:
                    mase_detail = f" | 予測誤差 {row['model_mase']:.1f}円"
            
            predictions_text += (
                f"**{i}位: {row['item_name']}** - {int(row['predicted_price'])}円/kg\n"
                f"　予測精度: {int(row['confidence_pct'])}% | {row['model_type']}{mase_detail}\n\n"
            )
        
        # 季節情報
        seasonal_text = ""
        if seasonal_insights:
            if seasonal_insights.get('volatile_items'):
                seasonal_text = f"*{month_name}によく変動する野菜*: " + "、".join(seasonal_insights['volatile_items'][:3])
        
        # モデル情報
        model_text = ""
        if model_info.get('available'):
            model_text = (
                f"\n*予測モデル情報*\n"
                f"バージョン: {model_info['version']} | "
                f"対象品目: {model_info['items_covered']}品目 | "
                f"更新: {model_info['created_at'][:10]}"
            )
        
        return {
            "text": f"{month_name}の安い野菜TOP10",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{month_name}の安価野菜TOP10"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"AI予測による{month_name}の野菜価格ランキング（安い順）\n\n{predictions_text}"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{seasonal_text}\n\n{model_text}"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"予測生成日: {datetime.now().strftime('%Y-%m-%d')} | 野菜市場分析プラットフォーム"
                        }
                    ]
                }
            ]
        }
    
    def send_slack_notification(self, message: Dict) -> bool:
        # Slackにメッセージを送信
        if not self.config.slack_webhook_url:
            self.logger.error("Slack Webhook URLが設定されていません")
            return False
        
        try:
            headers = {
                'Content-Type': 'application/json; charset=utf-8'
            }
            
            response = requests.post(
                self.config.slack_webhook_url,
                headers=headers,
                data=json.dumps(message, ensure_ascii=False).encode('utf-8'),
                timeout=self.config.timeout_seconds
            )
            
            if response.status_code == 200:
                self.logger.info("Slack通知送信成功")
                self.notifications_sent += 1
                self.last_notification_time = datetime.now()
                return True
            else:
                self.logger.error(f"Slack通知送信失敗: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Slack通知送信エラー: {e}")
            return False
    
    def run_notification(self, target_month: int = None, dry_run: bool = False) -> NotificationResult:
        # 通知処理を実行
        self.logger.info("=== 野菜価格Slack通知処理開始 ===")
        start_time = datetime.now()
        
        # メモリ監視
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            # ターゲット月の設定
            if target_month is None:
                next_month = (datetime.now() + timedelta(days=30))
                target_month = next_month.month
            
            # 安い野菜予測を取得
            predictions_df = self.get_cheapest_vegetables_predictions(target_month)
            
            # 季節洞察を取得
            seasonal_insights = self.get_seasonal_insights(target_month)
            
            # モデル情報を取得
            model_info = self.check_model_availability()
            
            # Slackメッセージを作成
            message = self.create_slack_message(
                predictions_df, target_month, seasonal_insights, model_info
            )
            
            # ドライラン確認
            if dry_run:
                self.logger.info("=== ドライラン: Slackメッセージ ===")
                if self.config.enable_debug:
                    print(json.dumps(message, ensure_ascii=False, indent=2))
                self.logger.info("ドライラン完了（実際の送信は行われませんでした）")
                return NotificationResult(
                    success=True,
                    target_month=target_month,
                    predictions_count=len(predictions_df),
                    model_available=model_info.get('available', False),
                    duration_seconds=(datetime.now() - start_time).total_seconds(),
                    timestamp=datetime.now().isoformat(),
                    message_preview=message['text'],
                    dry_run=True
                )
            
            # Slack通知送信
            success = self.send_slack_notification(message)
            
            # 結果サマリー
            end_time = datetime.now()
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            summary = NotificationResult(
                success=success,
                target_month=target_month,
                predictions_count=len(predictions_df),
                model_available=model_info.get('available', False),
                duration_seconds=(end_time - start_time).total_seconds(),
                timestamp=datetime.now().isoformat()
            )
            
            if success:
                self.logger.info(f"✅ 通知送信完了: {len(predictions_df)}品目の価格低下予測")
            else:
                self.logger.error("❌ 通知送信失敗")
            
            self.logger.info(f"メモリ使用量: {initial_memory:.1f}MB → {current_memory:.1f}MB")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"通知処理エラー: {e}")
            return NotificationResult(
                success=False,
                target_month=target_month or 0,
                predictions_count=0,
                model_available=False,
                duration_seconds=(datetime.now() - start_time).total_seconds(),
                timestamp=datetime.now().isoformat(),
                error=str(e)
            )


def main():
    # メイン実行関数
    import argparse
    
    parser = argparse.ArgumentParser(description='野菜価格予測Slack通知')
    parser.add_argument('--month', type=int, help='通知対象月 (1-12)')
    parser.add_argument('--dry-run', action='store_true', help='ドライラン（実際の送信なし）')
    parser.add_argument('--threshold', type=float, default=0.1, help='価格低下閾値 (デフォルト: 0.1 = 10%)')
    parser.add_argument('--confidence', type=float, default=0.7, help='最低信頼度 (デフォルト: 0.7 = 70%)')
    
    args = parser.parse_args()
    
    try:
        # 設定読み込み
        config = SlackConfig.from_env()
        
        # コマンドライン引数で上書き
        if args.threshold:
            config.price_decrease_threshold = args.threshold
        if args.confidence:
            config.min_confidence = args.confidence
        
        notifier = EnhancedVegetablePriceNotifier(config)
        result = notifier.run_notification(target_month=args.month, dry_run=args.dry_run)
        
        if result.success:
            print(f"✅ 通知処理完了")
            if not args.dry_run:
                print(f"📱 Slack通知送信: {result.predictions_count}品目")
            exit(0)
        else:
            print(f"❌ 通知処理失敗: {result.error or 'Unknown error'}")
            exit(1)
            
    except Exception as e:
        print(f"❌ エラー: {e}")
        exit(1)


if __name__ == "__main__":
    main()