import pandas as pd
import numpy as np
import os
import json
import logging
import tempfile
import shutil
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any, Tuple
from dataclasses import dataclass, asdict, field
from pathlib import Path
from google.cloud import bigquery, storage
from google.api_core import exceptions as gcp_exceptions
from dotenv import load_dotenv
import concurrent.futures
import psutil

# 内部モジュールのインポート
try:
    from ml_time_series import EnhancedTimeSeriesPricePrediction
    from ml_price import EnhancedVegetablePricePrediction
    ML_MODULES_AVAILABLE = True
except ImportError as e:
    logging.warning(f"ML modules not available: {e}")
    ML_MODULES_AVAILABLE = False

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



ENABLED_MODELS = ["lstm"]  # 高精度なLSTMのみを使用 (必要に応じて変更)

# 対象品目設定 (Noneの場合はデフォルト10品目を使用)
TARGET_ITEMS = None


@dataclass
class BatchConfig:
    # バッチ処理設定
    project_id: str
    dataset_prefix: str = "vege"
    environment: str = "dev"
    gcs_bucket: Optional[str] = None
    months_ahead: int = 6
    min_data_points: int = 24
    max_items: int = 20
    max_workers: int = 2
    model_storage_base: str = "ml-models"
    ts_model_storage_base: str = "time-series-models"
    chunk_size: int = 1000
    timeout_seconds: int = 3600
    enable_model_saving: bool = True
    enabled_models: List[str] = field(default_factory=lambda: ENABLED_MODELS)
    target_items: List[str] = field(default_factory=lambda: TARGET_ITEMS)
    
    def __post_init__(self):
        if self.target_items is None:
            self.target_items = [
                "キャベツ", "トマト", "じゃがいも", "玉ねぎ", "にんじん",
                "だいこん", "レタス", "ほうれんそう", "きゅうり", "なす"
            ]
    
    @classmethod
    def from_env(cls) -> 'BatchConfig':
        # 環境変数から設定を作成
        load_dotenv()
        
        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
        
        return cls(
            project_id=project_id,
            dataset_prefix=os.getenv("DATASET_PREFIX", "vege"),
            environment=os.getenv("ENVIRONMENT", "dev"),
            gcs_bucket=os.getenv("GCS_BUCKET"),
            months_ahead=int(os.getenv("PREDICTION_MONTHS_AHEAD", "6")),
            min_data_points=int(os.getenv("MIN_DATA_POINTS", "24")),
            max_items=int(os.getenv("MAX_PREDICTION_ITEMS", "20")),
            max_workers=int(os.getenv("BATCH_MAX_WORKERS", "2")),
            chunk_size=int(os.getenv("BATCH_CHUNK_SIZE", "1000")),
            timeout_seconds=int(os.getenv("BATCH_TIMEOUT_SECONDS", "3600")),
            enable_model_saving=os.getenv("ENABLE_MODEL_SAVING", "true").lower() == "true",
            # enabled_models は BatchConfig のデフォルト値を使用
        )

@dataclass
class PredictionResult:
    # 予測結果データ
    success: bool
    item_name: str
    model_type: str
    predictions_count: int = 0
    processing_time_seconds: float = 0.0
    model_version: Optional[str] = None
    message: str = ""
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []

@dataclass
class BatchSummary:
    # バッチ処理結果サマリー
    success: bool
    total_predictions: int
    target_items: int
    models_used: int
    duration_minutes: float
    start_time: str
    end_time: str
    error_message: Optional[str] = None
    results: List[PredictionResult] = None
    
    def __post_init__(self):
        if self.results is None:
            self.results = []

class EnhancedPredictionBatchProcessor:
    # 改善された価格予測バッチ処理クラス
    
    def __init__(self, config: BatchConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # GCPクライアント初期化
        try:
            self.bq_client = bigquery.Client(project=config.project_id)
            self.storage_client = storage.Client(project=config.project_id) if config.gcs_bucket else None
            self.table_id = f"{config.project_id}.{config.dataset_prefix}_{config.environment}_raw.ml_price_pred"
        except Exception as e:
            self.logger.error(f"GCPクライアント初期化エラー: {e}")
            raise
        
        # モデル初期化
        self.ts_predictor: Optional[EnhancedTimeSeriesPricePrediction] = None
        self.ml_predictor: Optional[EnhancedVegetablePricePrediction] = None
        
        # バージョン管理
        self.batch_date = datetime.now()
        
        # 統計情報
        self.processed_items = 0
        self.total_predictions = 0
        self.errors_count = 0
        
    async def initialize_models(self) -> None:
        # モデル初期化
        try:
            if not ML_MODULES_AVAILABLE:
                self.logger.warning("ML modules not available - batch processing will be limited")
                return
            
            self.logger.info("バッチ処理用モデル初期化開始")
            
            # 時系列予測モデル
            try:
                # EnhancedTimeSeriesPricePredictionは引数にConfigオブジェクトを期待する
                from ml_time_series import TimeSeriesConfig
                ts_config = TimeSeriesConfig(project_id=self.config.project_id)
                self.ts_predictor = EnhancedTimeSeriesPricePrediction(ts_config)
                self.logger.info("時系列予測モデル初期化完了")
            except Exception as e:
                self.logger.error(f"時系列予測モデル初期化エラー: {e}")
            
            # 機械学習予測モデル
            try:
                # EnhancedVegetablePricePredictionは引数にConfigオブジェクトを期待する
                from ml_price import MLConfig
                ml_config = MLConfig(project_id=self.config.project_id)
                self.ml_predictor = EnhancedVegetablePricePrediction(ml_config)
                self.logger.info("機械学習予測モデル初期化完了")
            except Exception as e:
                self.logger.error(f"機械学習予測モデル初期化エラー: {e}")
            
            self.logger.info("モデル初期化完了")
            
        except Exception as e:
            self.logger.error(f"モデル初期化で予期しないエラー: {e}")
            raise
    
    def generate_model_version(self, data_size: int, model_type: Optional[str] = None, suffix: str = "") -> str:
        # モデルバージョンを生成
        base_version = f"{self.batch_date.strftime('%Y.%m')}"
        
        # データサイズによるバージョン
        if data_size < 1000:
            size_version = f"v{data_size}r"  # records
        elif data_size < 100000:
            size_version = f"v{data_size//100}h"  # hundreds
        else:
            size_version = f"v{data_size//1000}k"  # thousands
        
        # モデル種別によるサフィックス
        model_suffix = ""
        if model_type:
            model_mapping = {
                'prophet': 'PR',
                'lstm': 'LS', 
                'arima': 'AR',
                'random_forest': 'RF',
                'gradient_boosting': 'GB',
                'linear_regression': 'LR'
            }
            model_suffix = f"_{model_mapping.get(model_type.lower(), model_type[:2].upper())}"
        
        # 追加サフィックス
        if suffix:
            model_suffix += f"_{suffix}"
            
        return f"{base_version}_{size_version}{model_suffix}"
    
    def save_trained_models(self, all_predictions: List[Dict[str, Any]]) -> None:
        # 訓練済みモデルをGCSにバージョン付きで保存
        if not self.config.enable_model_saving or not self.config.gcs_bucket:
            self.logger.warning("モデル保存が無効またはGCSバケットが未設定のため、モデル保存をスキップします")
            return
            
        self.logger.info("訓練済みモデルのGCS保存を開始...")
        
        try:
            version_str = self.batch_date.strftime('%Y%m')
            bucket = self.storage_client.bucket(self.config.gcs_bucket)
            
            # 一時ディレクトリでローカル保存してからGCSにアップロード
            with tempfile.TemporaryDirectory() as temp_dir:
                ts_local_dir = os.path.join(temp_dir, f"ts_models_{version_str}")
                ml_local_dir = os.path.join(temp_dir, f"ml_models_{version_str}")
                
                # 時系列モデル保存
                saved_ts_models = []
                if hasattr(self.ts_predictor, 'models') and self.ts_predictor.models:
                    self.ts_predictor.save_models(ts_local_dir)
                    saved_ts_models = self._upload_directory_to_gcs(
                        bucket, ts_local_dir, f"{self.config.ts_model_storage_base}/{version_str}"
                    )
                    self.logger.info(f"時系列モデルGCS保存完了: gs://{self.config.gcs_bucket}/{self.config.ts_model_storage_base}/{version_str}/")
                
                # 機械学習モデル保存
                saved_ml_models = []
                if hasattr(self.ml_predictor, 'models') and self.ml_predictor.models:
                    self.ml_predictor.save_models(ml_local_dir)
                    saved_ml_models = self._upload_directory_to_gcs(
                        bucket, ml_local_dir, f"{self.config.model_storage_base}/{version_str}"
                    )
                    self.logger.info(f"機械学習モデルGCS保存完了: gs://{self.config.gcs_bucket}/{self.config.model_storage_base}/{version_str}/")
                
                # モデルメタデータ保存
                model_metadata = {
                    'version': version_str,
                    'created_at': self.batch_date.isoformat(),
                    'gcs_bucket': self.config.gcs_bucket,
                    'ts_model_path': f"{self.config.ts_model_storage_base}/{version_str}",
                    'ml_model_path': f"{self.config.model_storage_base}/{version_str}",
                    'total_predictions': len(all_predictions),
                    'model_types': list(set(p['model_type'] for p in all_predictions)),
                    'items_covered': list(set(p['item_name'] for p in all_predictions)),
                    'saved_ts_models': saved_ts_models,
                    'saved_ml_models': saved_ml_models,
                    'environment': self.config.environment,
                    'project_id': self.config.project_id
                }
                
                # メタデータをGCSに保存
                metadata_path = f"model-metadata/metadata_{version_str}.json"
                blob = bucket.blob(metadata_path)
                blob.upload_from_string(
                    json.dumps(model_metadata, ensure_ascii=False, indent=2),
                    content_type='application/json'
                )
                
                self.logger.info(f"モデルメタデータGCS保存完了: gs://{self.config.gcs_bucket}/{metadata_path}")
                self.logger.info(f"保存されたモデル数: 時系列={len(saved_ts_models)}, ML={len(saved_ml_models)}")
                
        except Exception as e:
            self.logger.error(f"GCSモデル保存エラー: {e}")
            # エラーでも処理は継続（予測結果は保存済み）
    
    def _upload_directory_to_gcs(self, bucket, local_dir: str, gcs_prefix: str) -> List[str]:
        # ディレクトリをGCSにアップロード
        uploaded_files = []
        
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_dir)
                gcs_path = f"{gcs_prefix}/{relative_path}".replace('\\', '/')  # Windows対応
                
                blob = bucket.blob(gcs_path)
                blob.upload_from_filename(local_file_path)
                uploaded_files.append(gcs_path)
                self.logger.debug(f"アップロード: {local_file_path} -> gs://{bucket.name}/{gcs_path}")
        
        return uploaded_files
        
    def get_target_items(self) -> List[str]:
        # 予測対象品目を取得
        try:
            query = f# SELECT DISTINCT
# item_name,
# COUNT(*) as data_points,
# MIN(year) as start_year,
# MAX(year) as end_year
# FROM `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.mart_price_weather`
# WHERE avg_unit_price_yen IS NOT NULL
# AND year >= 2020
# GROUP BY item_name
# HAVING data_points >= {self.config.min_data_points}
# ORDER BY data_points DESC
# LIMIT {self.config.max_items}
            
            df = self.bq_client.query(query).to_dataframe()
            items = df['item_name'].tolist()
            self.logger.info(f"予測対象品目: {len(items)}品目 - {items}")
            return items
            
        except Exception as e:
            self.logger.warning(f"品目取得失敗: {e}. デフォルト品目を使用")
            return self.config.target_items
    
    def run_time_series_predictions(self, items: List[str]) -> List[Dict[str, Any]]:
        # 時系列予測を実行（最適化版）
        if not self.ts_predictor:
            self.logger.warning("時系列予測モデルが利用できません")
            return []
            
        self.logger.info("時系列予測を開始...")
        predictions = []
        
        # データキャッシュ用辞書
        data_cache = {}
        
        for item in items:
            # データを1回だけ抽出してキャッシュ
            if item not in data_cache:
                try:
                    df = self.ts_predictor.extract_time_series_data(item)
                    if len(df) >= self.config.min_data_points:
                        data_cache[item] = df
                        latest_data_date = df['ds'].max()
                        self.logger.info(f"[DATA] {item}: {len(df)}行 ({df['ds'].min()} - {latest_data_date.year}年{latest_data_date.month}月) → 予測期間1-6月先")
                    else:
                        self.logger.warning(f"{item}: データ不足（{len(df)}行）をスキップ")
                        continue
                except Exception as e:
                    self.logger.error(f"{item}: データ抽出エラー - {e}")
                    continue
            
            df = data_cache[item]
            
            # 品目処理開始の区切り
            enabled_models = [m.strip().lower() for m in self.config.enabled_models if m.strip().lower() in ['arima', 'prophet', 'lstm']]
            self.logger.info(f"[PROC] {item} ━━━ {'/'.join([m.upper() for m in enabled_models])} モデル実行 ━━━")
            
            for model_type in enabled_models:
                try:
                    start_time = time.time()
                    
                    # モデル訓練と評価
                    model_performance = None
                    if model_type == 'prophet':
                        model, forecast = self.ts_predictor.train_prophet_model(df)
                        # バックテストで性能評価
                        model_performance = self.ts_predictor.evaluate_model(df, 'prophet')
                    elif model_type == 'lstm':
                        model, history = self.ts_predictor.train_lstm_model(df)
                        # バックテストで性能評価
                        model_performance = self.ts_predictor.evaluate_model(df, 'lstm')
                    elif model_type == 'arima':
                        model, forecast = self.ts_predictor.train_arima_model(df)
                        if model is None:
                            continue
                        # バックテストで性能評価
                        model_performance = self.ts_predictor.evaluate_model(df, 'arima')
                    
                    # 予測実行
                    pred_results = self.ts_predictor.predict_future(
                        item, months_ahead=self.config.months_ahead, model_type=model_type
                    )
                    
                    # 予測結果の初期チェック
                    if pred_results.empty:
                        self.logger.warning(f"[ERROR] {item}-{model_type.upper()}: 予測結果が空です")
                        continue
                    
                    first_pred_date = pred_results['date'].min()
                    last_pred_date = pred_results['date'].max()
                    
                    # 結果をリストに追加
                    for _, pred_row in pred_results.iterrows():
                        # モデルバージョン生成
                        model_version = self.generate_model_version(len(df), model_type, item)
                        
                        # 日付オブジェクトの型を確認して適切に変換
                        pred_date = pred_row['date']
                        if hasattr(pred_date, 'date'):
                            # datetimeオブジェクトの場合
                            prediction_date = pred_date.date()
                            prediction_year = pred_date.year
                            prediction_month = pred_date.month
                            date_str = pred_date.strftime('%Y%m')
                        else:
                            # 既にdateオブジェクトの場合
                            prediction_date = pred_date
                            prediction_year = pred_date.year
                            prediction_month = pred_date.month
                            date_str = pred_date.strftime('%Y%m')
                        
                        # データフレームの日付も同様に処理
                        ds_min = df['ds'].min()
                        ds_max = df['ds'].max()
                        ds_last = df['ds'].iloc[-1]
                        
                        training_start = ds_min.date() if hasattr(ds_min, 'date') else ds_min
                        training_end = ds_max.date() if hasattr(ds_max, 'date') else ds_max
                        last_actual_date = ds_last.date() if hasattr(ds_last, 'date') else ds_last
                        
                        prediction = {
                            'prediction_id': f"{item}_{model_type}_{date_str}_ts",
                            'prediction_date': prediction_date,
                            'prediction_year': prediction_year,
                            'prediction_month': prediction_month,
                            'prediction_generated_at': datetime.now(),
                            'item_name': item,
                            'market': None,
                            'origin': None,
                            'model_type': model_type.upper(),
                            'model_version': model_version,
                            'predicted_price': float(pred_row['predicted_price']),
                            'lower_bound_price': float(pred_row.get('lower_bound', pred_row['predicted_price'] * 0.8)),
                            'upper_bound_price': float(pred_row.get('upper_bound', pred_row['predicted_price'] * 1.2)),
                            'confidence_interval': 0.8,
                            'training_data_points': len(df),
                            'training_period_start': training_start,
                            'training_period_end': training_end,
                            'last_actual_price': float(df['y'].iloc[-1]),
                            'last_actual_date': last_actual_date,
                            'model_mae': model_performance.mae if model_performance else None,
                            'model_mse': model_performance.mse if model_performance else None,
                            'model_rmse': model_performance.rmse if model_performance else None,
                            'model_r2': model_performance.r2 if model_performance else None,
                            'model_mape': model_performance.mape if model_performance else None,
                            'model_smape': model_performance.smape if model_performance else None,
                            'model_mase': model_performance.mase if model_performance else None,
                            'data_quality_score': 0.8,
                            'prediction_reliability': 'MEDIUM',
                            'created_at': datetime.now(),  # TIMESTAMPオブジェクト
                            'updated_at': datetime.now()   # TIMESTAMPオブジェクト
                        }
                        predictions.append(prediction)
                    
                    elapsed_time = time.time() - start_time
                    self.logger.info(f"[OK] {item}-{model_type.upper()}: {len(pred_results)}件 ({first_pred_date.strftime('%Y/%m')}-{last_pred_date.strftime('%Y/%m')}) {elapsed_time:.1f}s")
                    
                except Exception as e:
                    self.logger.error(f"{item} - {model_type}予測エラー: {e}")
                    self.errors_count += 1
                    continue
        
        self.logger.info(f"[DONE] 時系列予測完了: {len(predictions)}件")
        return predictions
    
    def run_ml_predictions(self, items: List[str]) -> List[Dict[str, Any]]:
        # 機械学習予測を実行
        if not self.ml_predictor:
            self.logger.warning("機械学習予測モデルが利用できません")
            return []
            
        self.logger.info("機械学習予測を開始...")
        predictions = []
        
        try:
            # 全品目のデータを一括取得して学習
            df = self.ml_predictor.extract_training_data()
            if len(df) < 100:
                self.logger.warning(f"ML予測: データ不足（{len(df)}行）")
                return predictions
            
            self.logger.info(f"ML予測: {len(df):,}行のデータで学習開始")
            
            # モデル学習
            results = self.ml_predictor.train_models(df)
            
            if results:
                best_model = min(results.keys(), key=lambda x: results[x].mae)
                self.logger.info(f"最良MLモデル: {best_model} (MAE: {results[best_model].mae:.2f})")
            
            # 各品目の予測を実行
            for item in items:
                try:
                    # PredictionRequestオブジェクトを作成して予測実行
                    from ml_price import PredictionRequest
                    request = PredictionRequest(
                        item_name=item,
                        months_ahead=self.config.months_ahead
                    )
                    predictions_data = self.ml_predictor.predict_future_prices(request)
                    
                    best_model = 'random_forest'  # デフォルト
                    if results:
                        best_model = min(results.keys(), key=lambda x: results[x].mae)
                    
                    # MLモデルバージョン生成
                    ml_model_version = self.generate_model_version(len(df), best_model, "ML")
                    
                    for pred in predictions_data:
                        prediction = {
                            'prediction_id': f"{item}_{best_model}_{pred['year']}{pred['month']:02d}_ml",
                            'prediction_date': pd.to_datetime(f"{pred['year']}-{pred['month']:02d}-01"),
                            'prediction_year': pred['year'],
                            'prediction_month': pred['month'],
                            'prediction_generated_at': datetime.now(),
                            'item_name': item,
                            'market': None,
                            'origin': None,
                            'model_type': best_model.upper(),
                            'model_version': ml_model_version,
                            'predicted_price': float(pred['predicted_price']),
                            'lower_bound_price': float(pred['predicted_price'] * 0.85),
                            'upper_bound_price': float(pred['predicted_price'] * 1.15),
                            'confidence_interval': 0.7,
                            'training_data_points': len(df),
                            'training_period_start': None,
                            'training_period_end': None,
                            'last_actual_price': None,
                            'last_actual_date': None,
                            'model_mae': results[best_model].mae if results else None,
                            'model_mse': results[best_model].mse if results else None,
                            'model_rmse': results[best_model].rmse if results else None,
                            'model_r2': results[best_model].r2 if results else None,
                            'data_quality_score': 0.7,
                            'prediction_reliability': 'MEDIUM',
                            'created_at': datetime.now(),  # TIMESTAMPオブジェクト
                            'updated_at': datetime.now()   # TIMESTAMPオブジェクト
                        }
                        predictions.append(prediction)
                    
                    self.logger.info(f"{item} - ML: {len(predictions_data)}件の予測完了 (v{ml_model_version})")
                    
                except Exception as e:
                    self.logger.error(f"{item} - ML予測エラー: {e}")
                    self.errors_count += 1
                    continue
        
        except Exception as e:
            self.logger.error(f"ML予測全体エラー: {e}")
            self.errors_count += 1
        
        self.logger.info(f"機械学習予測完了: {len(predictions)}件")
        return predictions
    
    def save_to_bigquery(self, predictions: List[Dict[str, Any]]) -> None:
        # 予測結果をBigQueryに保存
        if not predictions:
            self.logger.warning("保存する予測データがありません")
            return
        
        try:
            # DataFrame作成
            df = pd.DataFrame(predictions)
            
            # BigQueryジョブ設定
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE"  # 既存データを上書き
            )
            
            # データ型の統一（pyarrow変換エラー回避）
            if 'prediction_generated_at' in df.columns:
                df['prediction_generated_at'] = pd.to_datetime(df['prediction_generated_at'])
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'])
            if 'updated_at' in df.columns:
                df['updated_at'] = pd.to_datetime(df['updated_at'])
            if 'prediction_date' in df.columns:
                df['prediction_date'] = pd.to_datetime(df['prediction_date'])
            
            # データ保存
            job = self.bq_client.load_table_from_dataframe(df, self.table_id, job_config=job_config)
            job.result()  # 完了まで待機
            
            self.logger.info(f"BigQueryに保存完了: {len(predictions)}件 - {self.table_id}")
            
            # 統計情報出力
            stats = df.groupby(['model_type']).size()
            self.logger.info(f"モデル別予測件数: {stats.to_dict()}")
            
            # 予測期間の統計情報
            if 'prediction_date' in df.columns:
                
                min_pred_date = df['prediction_date'].min()
                max_pred_date = df['prediction_date'].max()
                unique_months = df['prediction_date'].dt.strftime('%Y年%m月').nunique()
                self.logger.info(f"予測期間: {min_pred_date.strftime('%Y年%m月')} - {max_pred_date.strftime('%Y年%m月')} (合計{unique_months}ヶ月間)")
                
                # 月別の予測件数
                month_stats = df['prediction_date'].dt.strftime('%Y年%m月').value_counts().sort_index()
                self.logger.info(f"月別予測件数: {month_stats.to_dict()}")
            else:
                self.logger.warning("予測データに日付情報がありません")
            
            # 統計更新
            self.total_predictions += len(predictions)
            
        except Exception as e:
            self.logger.error(f"BigQuery保存エラー: {e}")
            raise
    
    async def run_batch_prediction(self) -> BatchSummary:
        # バッチ予測処理を実行
        self.logger.info("=== 価格予測バッチ処理開始 ===")
        start_time = datetime.now()
        
        # メモリ監視
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            # モデル初期化
            await self.initialize_models()
            
            # 対象品目取得
            items = self.get_target_items()
            
            if not items:
                raise ValueError("予測対象品目が取得できません")
            
            all_predictions = []
            results = []
            
            # 時系列予測実行
            try:
                ts_start = time.time()
                ts_predictions = self.run_time_series_predictions(items)
                all_predictions.extend(ts_predictions)
                ts_time = time.time() - ts_start
                
                results.append(PredictionResult(
                    success=True,
                    item_name="ALL_ITEMS",
                    model_type="TIME_SERIES",
                    predictions_count=len(ts_predictions),
                    processing_time_seconds=ts_time,
                    message="時系列予測完了"
                ))
            except Exception as e:
                self.logger.error(f"時系列予測エラー: {e}")
                results.append(PredictionResult(
                    success=False,
                    item_name="ALL_ITEMS",
                    model_type="TIME_SERIES",
                    message=f"時系列予測エラー: {e}",
                    errors=[str(e)]
                ))
            
            # 機械学習予測実行
            try:
                ml_start = time.time()
                ml_predictions = self.run_ml_predictions(items)
                all_predictions.extend(ml_predictions)
                ml_time = time.time() - ml_start
                
                results.append(PredictionResult(
                    success=True,
                    item_name="ALL_ITEMS",
                    model_type="MACHINE_LEARNING",
                    predictions_count=len(ml_predictions),
                    processing_time_seconds=ml_time,
                    message="機械学習予測完了"
                ))
            except Exception as e:
                self.logger.error(f"機械学習予測エラー: {e}")
                results.append(PredictionResult(
                    success=False,
                    item_name="ALL_ITEMS",
                    model_type="MACHINE_LEARNING",
                    message=f"機械学習予測エラー: {e}",
                    errors=[str(e)]
                ))
            
            # BigQueryに保存
            if all_predictions:
                self.save_to_bigquery(all_predictions)
                
                # 訓練済みモデル保存
                if self.config.enable_model_saving:
                    self.save_trained_models(all_predictions)
            
            # 結果サマリー作成
            end_time = datetime.now()
            duration = end_time - start_time
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            summary = BatchSummary(
                success=True,
                total_predictions=len(all_predictions),
                target_items=len(items),
                models_used=len(set(p['model_type'] for p in all_predictions)) if all_predictions else 0,
                duration_minutes=duration.total_seconds() / 60,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                results=results
            )
            
            self.logger.info(f"=== バッチ処理完了 ===")
            self.logger.info(f"総予測件数: {summary.total_predictions}")
            self.logger.info(f"対象品目数: {summary.target_items}")
            self.logger.info(f"処理時間: {summary.duration_minutes:.1f}分")
            self.logger.info(f"メモリ使用量: {initial_memory:.1f}MB → {current_memory:.1f}MB")
            self.logger.info(f"エラー数: {self.errors_count}")
            
            # 結果をJSONファイルに保存
            with open('prediction_batch_summary.json', 'w', encoding='utf-8') as f:
                json.dump(asdict(summary), f, ensure_ascii=False, indent=2)
            
            return summary
            
        except Exception as e:
            self.logger.error(f"バッチ処理エラー: {e}")
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            error_summary = BatchSummary(
                success=False,
                total_predictions=0,
                target_items=0,
                models_used=0,
                duration_minutes=duration.total_seconds() / 60,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                error_message=str(e)
            )
            
            with open('prediction_batch_summary.json', 'w', encoding='utf-8') as f:
                json.dump(asdict(error_summary), f, ensure_ascii=False, indent=2)
            
            raise


async def main():
    # メイン実行関数
    parser = argparse.ArgumentParser(description="価格予測バッチ処理システム")
    
    parser.add_argument("--config-file", help="設定ファイルパス")
    parser.add_argument("--dry-run", action="store_true", help="実行せずに見積もりのみ表示")
    parser.add_argument("--max-items", type=int, help="最大品目数")
    parser.add_argument("--months-ahead", type=int, help="予測期間（月）")
    parser.add_argument("--disable-model-saving", action="store_true", help="モデル保存を無効化")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("価格予測バッチ処理システム")
    print("=" * 60)
    
    try:
        # 設定読み込み
        config = BatchConfig.from_env()
        
        # コマンドライン引数で上書き
        if args.max_items:
            config.max_items = args.max_items
        if args.months_ahead:
            config.months_ahead = args.months_ahead
        if args.disable_model_saving:
            config.enable_model_saving = False
        
        # プロセッサー初期化
        processor = EnhancedPredictionBatchProcessor(config)
        
        print(f"設定: {config.max_items}品目 x {'/'.join(config.enabled_models).upper()}モデル → {config.months_ahead}ヶ月先予測")
        
        if args.dry_run:
            print("*** DRY RUN モード：実際の処理は実行されません ***")
            return 0
        
        print("*** 実際のバッチ処理を開始します ***")
        summary = await processor.run_batch_prediction()
        
        if summary.success:
            print(f"予測バッチ処理完了: {summary.total_predictions}件")
            return 0
        else:
            print(f"予測バッチ処理失敗: {summary.error_message}")
            return 1
            
    except Exception as e:
        logger.error(f"バッチ処理中にエラーが発生しました: {e}")
        print(f"予測バッチ処理エラー: {e}")
        return 1


if __name__ == "__main__":
    import asyncio
    exit(asyncio.run(main()))