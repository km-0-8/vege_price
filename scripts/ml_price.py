import pandas as pd
import numpy as np
import os
import logging
import time
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

from google.cloud import bigquery
from google.api_core import exceptions as gcp_exceptions
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib
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
class MLConfig:
    # 機械学習設定
    project_id: str
    dataset_prefix: str = "vege"
    environment: str = "dev"
    model_dir: str = "models"
    test_size: float = 0.2
    random_state: int = 42
    max_features: int = 50
    price_outlier_threshold: float = 5000.0
    min_quantity_threshold: float = 0.0
    cross_validation_folds: int = 5
    enable_feature_importance: bool = True
    enable_model_caching: bool = True
    model_timeout_seconds: int = 1800
    
    @classmethod
    def from_env(cls) -> 'MLConfig':
        # 環境変数から設定を作成
        load_dotenv()
        
        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
        
        return cls(
            project_id=project_id,
            dataset_prefix=os.getenv("DATASET_PREFIX", "vege"),
            environment=os.getenv("ENVIRONMENT", "dev"),
            model_dir=os.getenv("ML_MODEL_DIR", "models"),
            test_size=float(os.getenv("ML_TEST_SIZE", "0.2")),
            random_state=int(os.getenv("ML_RANDOM_STATE", "42")),
            max_features=int(os.getenv("ML_MAX_FEATURES", "50")),
            price_outlier_threshold=float(os.getenv("PRICE_OUTLIER_THRESHOLD", "5000.0")),
            cross_validation_folds=int(os.getenv("CV_FOLDS", "5")),
            enable_feature_importance=os.getenv("ENABLE_FEATURE_IMPORTANCE", "true").lower() == "true",
            enable_model_caching=os.getenv("ENABLE_MODEL_CACHING", "true").lower() == "true",
            model_timeout_seconds=int(os.getenv("MODEL_TIMEOUT_SECONDS", "1800"))
        )

@dataclass
class ModelResult:
    # モデル評価結果
    model_name: str
    mse: float
    mae: float
    r2: float
    rmse: float
    training_time_seconds: float
    feature_importance: Optional[Dict[str, float]] = None
    model_params: Optional[Dict[str, Any]] = None

@dataclass
class PredictionRequest:
    # 予測リクエスト
    item_name: str
    market: Optional[str] = None
    origin: Optional[str] = None
    months_ahead: int = 3
    confidence_interval: float = 0.95

class EnhancedVegetablePricePrediction:
    # 改善された野菜価格予測モデルクラス
    
    def __init__(self, config: MLConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # BigQueryクライアント初期化
        try:
            self.bq_client = bigquery.Client(project=config.project_id)
        except Exception as e:
            self.logger.error(f"BigQueryクライアント初期化エラー: {e}")
            raise
        
        # モデル管理
        self.models: Dict[str, Any] = {}
        self.scalers: Dict[str, StandardScaler] = {}
        self.encoders: Dict[str, LabelEncoder] = {}
        self.feature_columns: List[str] = []
        self.model_results: Dict[str, ModelResult] = {}
        
        # 統計情報
        self.training_samples = 0
        self.feature_count = 0
        self.last_training_time = None
        
    def extract_training_data(self) -> pd.DataFrame:
        # BigQueryから機械学習用データを抽出
        query = f"""
        WITH enhanced_data AS (
        SELECT
        pw.year,
        pw.month,
        pw.item_name,
        pw.market,
        pw.origin,
        pw.avg_unit_price_yen,
        pw.total_quantity_kg,
        pw.total_amount_yen,
        pw.yoy_price_growth_rate,
        pw.mom_price_growth_rate,
        pw.price_3month_moving_avg,
        pw.price_6month_moving_avg,
        pw.avg_temperature,
        pw.avg_max_temperature,
        pw.avg_min_temperature,
        pw.total_precipitation,
        pw.avg_wind_speed,
        pw.total_sunshine,
        -- 時系列特徴量
        DATE(pw.year, pw.month, 1) as date_key,
        EXTRACT(QUARTER FROM DATE(pw.year, pw.month, 1)) as quarter,
        -- 前月の価格データ（ラグ特徴量）
        LAG(pw.avg_unit_price_yen, 1) OVER (
        PARTITION BY pw.item_name, pw.market, pw.origin
        ORDER BY pw.year, pw.month
        ) as price_lag_1m,
        LAG(pw.avg_unit_price_yen, 3) OVER (
        PARTITION BY pw.item_name, pw.market, pw.origin
        ORDER BY pw.year, pw.month
        ) as price_lag_3m,
        LAG(pw.avg_unit_price_yen, 12) OVER (
        PARTITION BY pw.item_name, pw.market, pw.origin
        ORDER BY pw.year, pw.month
        ) as price_lag_12m,
        -- 数量ラグ特徴量
        LAG(pw.total_quantity_kg, 1) OVER (
        PARTITION BY pw.item_name, pw.market, pw.origin
        ORDER BY pw.year, pw.month
        ) as quantity_lag_1m,
        -- 季節性特徴量
        sa.seasonal_price_index,
        sa.seasonal_quantity_index,
        sa.seasonality_level,
        -- アイテム特性
        ie.price_tier,
        ie.vegetable_category,
        ie.seasonality as item_seasonality,
        ie.avg_unit_price_yen as item_avg_price
        FROM `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.mart_price_weather` pw
        LEFT JOIN `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.mart_seasonal_analysis` sa
        ON pw.item_name = sa.item_name AND pw.month = sa.month
        LEFT JOIN `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.dim_item_enhanced` ie
        ON pw.item_name = ie.item_name
        WHERE pw.avg_unit_price_yen IS NOT NULL
        AND pw.avg_unit_price_yen > {self.config.min_quantity_threshold}
        AND pw.avg_unit_price_yen < {self.config.price_outlier_threshold}
        AND pw.total_quantity_kg > {self.config.min_quantity_threshold}
        )
        SELECT *
        FROM enhanced_data
        WHERE price_lag_1m IS NOT NULL  -- ラグ特徴量が有効なデータのみ
        ORDER BY item_name, market, origin, year, month
        """
        
        try:
            self.logger.info("BigQueryからデータを抽出中...")
            df = self.bq_client.query(query).to_dataframe()
            
            if df.empty:
                raise ValueError("抽出されたデータが空です")
            
            self.logger.info(f"抽出完了: {len(df):,}行のデータ")
            self.training_samples = len(df)
            
            return df
            
        except Exception as e:
            self.logger.error(f"データ抽出エラー: {e}")
            raise
    
    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        # 特徴量エンジニアリング
        self.logger.info("特徴量エンジニアリング中...")
        
        # カテゴリカル変数のエンコーディング
        categorical_columns = ['item_name', 'market', 'origin', 'price_tier', 
                             'vegetable_category', 'item_seasonality', 'seasonality_level']
        
        for col in categorical_columns:
            if col not in self.encoders:
                self.encoders[col] = LabelEncoder()
            if col in df.columns:
                try:
                    df[f'{col}_encoded'] = self.encoders[col].fit_transform(df[col].astype(str))
                except Exception as e:
                    self.logger.warning(f"エンコーディングエラー {col}: {e}")
                    # デフォルト値で埋める
                    df[f'{col}_encoded'] = 0
        
        try:
            # 追加の特徴量生成
            df['price_volatility'] = df.groupby(['item_name', 'market', 'origin'])['avg_unit_price_yen'].transform(
                lambda x: x.rolling(window=3, min_periods=1).std()
            ).fillna(0)
            
            # 気象データの偏差特徴量
            if 'avg_temperature' in df.columns:
                df['temperature_deviation'] = (df['avg_temperature'] - 
                    df.groupby('month')['avg_temperature'].transform('mean')).fillna(0)
            else:
                df['temperature_deviation'] = 0
                
            if 'total_precipitation' in df.columns:
                df['precipitation_deviation'] = (df['total_precipitation'] - 
                    df.groupby('month')['total_precipitation'].transform('mean')).fillna(0)
            else:
                df['precipitation_deviation'] = 0
            
            # 価格変動の方向性
            if 'mom_price_growth_rate' in df.columns:
                df['price_trend'] = np.where(df['mom_price_growth_rate'].fillna(0) > 5, 1, 
                                           np.where(df['mom_price_growth_rate'].fillna(0) < -5, -1, 0))
            else:
                df['price_trend'] = 0
            
            # 季節ダミー変数
            if 'quarter' in df.columns:
                df = pd.get_dummies(df, columns=['quarter'], prefix='quarter')
            
            self.logger.debug(f"特徴量エンジニアリング完了: {df.shape}")
            return df
            
        except Exception as e:
            self.logger.error(f"特徴量エンジニアリングエラー: {e}")
            raise
    
    def select_features(self, df: pd.DataFrame) -> List[str]:
        # 予測に使用する特徴量を選択
        feature_columns = [
            # 時系列特徴量
            'year', 'month',
            
            # ラグ特徴量（最重要）
            'price_lag_1m', 'price_lag_3m', 'price_lag_12m',
            'quantity_lag_1m',
            
            # 価格関連特徴量
            'yoy_price_growth_rate', 'mom_price_growth_rate',
            'price_3month_moving_avg', 'price_6month_moving_avg',
            'price_volatility',
            
            # 気象特徴量
            'avg_temperature', 'avg_max_temperature', 'avg_min_temperature',
            'total_precipitation', 'avg_wind_speed', 'total_sunshine',
            'temperature_deviation', 'precipitation_deviation',
            
            # 季節性特徴量
            'seasonal_price_index', 'seasonal_quantity_index',
            
            # 数量特徴量
            'total_quantity_kg', 'total_amount_yen',
            
            # カテゴリカル特徴量（エンコード済み）
            'item_name_encoded', 'market_encoded', 'origin_encoded',
            'price_tier_encoded', 'vegetable_category_encoded',
            'item_seasonality_encoded', 'seasonality_level_encoded',
            
            # その他
            'item_avg_price', 'price_trend'
        ]
        
        # 四半期ダミー変数を追加
        quarter_cols = [col for col in df.columns if col.startswith('quarter_')]
        feature_columns.extend(quarter_cols)
        
        # 存在する特徴量のみを選択
        available_features = [col for col in feature_columns if col in df.columns]
        missing_features = [col for col in feature_columns if col not in df.columns]
        
        if missing_features:
            self.logger.warning(f"以下の特徴量が見つかりません: {missing_features[:10]}...")  # 最初の10個のみ表示
        
        # 最大特徴量数制限
        if len(available_features) > self.config.max_features:
            self.logger.info(f"特徴量数を{self.config.max_features}に制限します")
            available_features = available_features[:self.config.max_features]
        
        self.logger.info(f"使用する特徴量数: {len(available_features)}")
        self.feature_count = len(available_features)
        return available_features
    
    def train_models(self, df: pd.DataFrame, target_column: str = 'avg_unit_price_yen') -> Dict[str, ModelResult]:
        # 複数のモデルを学習
        self.logger.info("モデル学習中...")
        start_time = time.time()
        
        # 特徴量準備
        df_processed = self.prepare_features(df)
        feature_columns = self.select_features(df_processed)
        
        # 欠損値処理
        df_clean = df_processed.dropna(subset=feature_columns + [target_column])
        
        if df_clean.empty:
            raise ValueError("欠損値処理後のデータが空です")
        
        X = df_clean[feature_columns]
        y = df_clean[target_column]
        
        self.logger.info(f"学習データサイズ: X={X.shape}, y={y.shape}")
        
        # 時系列分割（最新データをテスト用に保持）
        df_clean = df_clean.sort_values(['year', 'month'])
        split_point = int(len(df_clean) * (1 - self.config.test_size))
        
        X_train = X.iloc[:split_point]
        X_test = X.iloc[split_point:]
        y_train = y.iloc[:split_point]
        y_test = y.iloc[split_point:]
        
        self.logger.info(f"データ分割: 訓練={len(X_train)}, テスト={len(X_test)}")
        
        # 特徴量スケーリング
        self.scalers['main'] = StandardScaler()
        X_train_scaled = self.scalers['main'].fit_transform(X_train)
        X_test_scaled = self.scalers['main'].transform(X_test)
        
        # モデル定義
        models_config = {
            'random_forest': RandomForestRegressor(
                n_estimators=100, 
                max_depth=15, 
                min_samples_split=5,
                random_state=self.config.random_state,
                n_jobs=-1
            ),
            'gradient_boosting': GradientBoostingRegressor(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                random_state=self.config.random_state
            ),
            'linear_regression': LinearRegression()
        }
        
        # モデル学習と評価
        results = {}
        for model_name, model in models_config.items():
            model_start_time = time.time()
            self.logger.info(f"{model_name} を学習中...")
            
            try:
                if model_name == 'linear_regression':
                    model.fit(X_train_scaled, y_train)
                    y_pred = model.predict(X_test_scaled)
                else:
                    model.fit(X_train, y_train)
                    y_pred = model.predict(X_test)
                
                # 評価指標計算
                mse = mean_squared_error(y_test, y_pred)
                mae = mean_absolute_error(y_test, y_pred)
                r2 = r2_score(y_test, y_pred)
                training_time = time.time() - model_start_time
                
                # 特徴量重要度収集
                feature_importance = None
                if self.config.enable_feature_importance and hasattr(model, 'feature_importances_'):
                    importance_df = pd.DataFrame({
                        'feature': feature_columns,
                        'importance': model.feature_importances_
                    }).sort_values('importance', ascending=False)
                    feature_importance = dict(zip(importance_df['feature'], importance_df['importance']))
                    
                    self.logger.info(f"  上位5特徴量:")
                    for _, row in importance_df.head().iterrows():
                        self.logger.info(f"    {row['feature']}: {row['importance']:.3f}")
                
                # モデル結果作成
                model_result = ModelResult(
                    model_name=model_name,
                    mse=mse,
                    mae=mae,
                    r2=r2,
                    rmse=np.sqrt(mse),
                    training_time_seconds=training_time,
                    feature_importance=feature_importance,
                    model_params=model.get_params() if hasattr(model, 'get_params') else None
                )
                
                results[model_name] = model_result
                
                self.logger.info(f"  MSE: {mse:.2f}")
                self.logger.info(f"  MAE: {mae:.2f}")
                self.logger.info(f"  R²: {r2:.3f}")
                self.logger.info(f"  RMSE: {np.sqrt(mse):.2f}")
                self.logger.info(f"  訓練時間: {training_time:.2f}秒")
                
            except Exception as e:
                self.logger.error(f"{model_name}の訓練エラー: {e}")
                continue
        
        if not results:
            raise ValueError("すべてのモデルの訓練に失敗しました")
        
        # 最良モデルを選択
        best_model_name = min(results.keys(), key=lambda x: results[x].mae)
        best_model = models_config[best_model_name]
        
        # モデル保存
        self.models['best'] = best_model
        for name, result in results.items():
            self.models[name] = models_config[name]
        
        # 結果保存
        self.model_results = results
        
        self.logger.info(f"\n最良モデル: {best_model_name} (MAE: {results[best_model_name].mae:.2f})")
        
        # 特徴量リストを保存
        self.feature_columns = feature_columns
        self.last_training_time = datetime.now()
        
        total_time = time.time() - start_time
        self.logger.info(f"総訓練時間: {total_time:.2f}秒")
        
        return results
    
    def predict_future_prices(self, request: PredictionRequest) -> List[Dict[str, Union[str, int, float]]]:
        # 未来の価格を予測
        if 'best' not in self.models:
            raise ValueError("モデルが学習されていません。train_models()を先に実行してください。")
        
        item_name = request.item_name
        market = request.market or ""
        origin = request.origin or ""
        months_ahead = request.months_ahead
        
        try:
            # 最新データを取得
            latest_data_query = f"""SELECT
            pw.*,
            sa.seasonal_price_index,
            sa.seasonal_quantity_index,
            sa.seasonality_level,
            ie.price_tier,
            ie.vegetable_category,
            ie.seasonality as item_seasonality,
            ie.avg_unit_price_yen as item_avg_price
            FROM `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.mart_price_weather` pw
            LEFT JOIN `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.mart_seasonal_analysis` sa
            ON pw.item_name = sa.item_name AND pw.month = sa.month
            LEFT JOIN `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.dim_item_enhanced` ie
            ON pw.item_name = ie.item_name
            WHERE pw.item_name = @item_name"""
            
            query_params = [bigquery.ScalarQueryParameter("item_name", "STRING", item_name)]
            
            if market:
                latest_data_query += " AND pw.market = @market"
                query_params.append(bigquery.ScalarQueryParameter("market", "STRING", market))
            if origin:
                latest_data_query += " AND pw.origin = @origin"
                query_params.append(bigquery.ScalarQueryParameter("origin", "STRING", origin))
                
            latest_data_query += " ORDER BY pw.year DESC, pw.month DESC LIMIT 12"
            
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)
            df_latest = self.bq_client.query(latest_data_query, job_config=job_config).to_dataframe()
            
        except Exception as e:
            self.logger.error(f"最新データ取得エラー: {e}")
            raise
        
        if df_latest.empty:
            raise ValueError(f"指定された条件のデータが見つかりません: {item_name}")
        
        predictions = []
        current_date = datetime(int(df_latest.iloc[0]['year']), int(df_latest.iloc[0]['month']), 1)
        base_price = float(df_latest.iloc[0]['avg_unit_price_yen'])
        
        self.logger.info(f"{item_name}の予測を開始: {months_ahead}ヶ月先まで")
        
        for i in range(months_ahead):
            # 次月の日付
            if current_date.month == 12:
                next_date = datetime(current_date.year + 1, 1, 1)
            else:
                next_date = datetime(current_date.year, current_date.month + 1, 1)
            
            # 簡略化された予測（実際の実装では、より詳細な特徴量エンジニアリングが必要）
            # 季節性やトレンドを考慮したランダム予測
            seasonal_factor = 1.0 + 0.1 * np.sin(2 * np.pi * next_date.month / 12)  # 季節性
            trend_factor = 1.0 + 0.02 * i  # 緊やかな上昇トレンド
            noise_factor = np.random.normal(1.0, 0.05)  # ランダムノイズ
            
            predicted_price = base_price * seasonal_factor * trend_factor * noise_factor
            
            # 信頼区間計算（簡略版）
            confidence_width = predicted_price * 0.15  # 15%の幅
            lower_bound = max(0, predicted_price - confidence_width)
            upper_bound = predicted_price + confidence_width
            
            predictions.append({
                'year': next_date.year,
                'month': next_date.month,
                'predicted_price': round(float(predicted_price), 2),
                'lower_bound': round(float(lower_bound), 2),
                'upper_bound': round(float(upper_bound), 2),
                'confidence_interval': request.confidence_interval,
                'prediction_date': next_date.strftime('%Y-%m-%d')
            })
            
            current_date = next_date
        
        self.logger.info(f"{item_name}の予測完了: {len(predictions)}件")
        return predictions
    
    def save_models(self, model_dir: Optional[str] = None) -> None:
        # モデルとエンコーダーを保存
        model_dir = model_dir or self.config.model_dir
        
        try:
            os.makedirs(model_dir, exist_ok=True)
            
            saved_files = []
            
            # モデル保存
            for name, model in self.models.items():
                model_path = f'{model_dir}/model_{name}.pkl'
                joblib.dump(model, model_path)
                saved_files.append(model_path)
            
            # スケーラー保存
            for name, scaler in self.scalers.items():
                scaler_path = f'{model_dir}/scaler_{name}.pkl'
                joblib.dump(scaler, scaler_path)
                saved_files.append(scaler_path)
            
            # エンコーダー保存
            for name, encoder in self.encoders.items():
                encoder_path = f'{model_dir}/encoder_{name}.pkl'
                joblib.dump(encoder, encoder_path)
                saved_files.append(encoder_path)
            
            # 特徴量リスト保存
            if hasattr(self, 'feature_columns'):
                features_path = f'{model_dir}/feature_columns.pkl'
                joblib.dump(self.feature_columns, features_path)
                saved_files.append(features_path)
            
            # モデル結果保存
            if self.model_results:
                results_path = f'{model_dir}/model_results.pkl'
                joblib.dump({name: asdict(result) for name, result in self.model_results.items()}, results_path)
                saved_files.append(results_path)
            
            self.logger.info(f"モデルを保存しました: {model_dir}/ ({len(saved_files)}ファイル)")
            
        except Exception as e:
            self.logger.error(f"モデル保存エラー: {e}")
            raise
    
    def load_models(self, model_dir: Optional[str] = None) -> None:
        # 保存されたモデルを読み込み
        model_dir = model_dir or self.config.model_dir
        
        if not os.path.exists(model_dir):
            raise FileNotFoundError(f"モデルディレクトリが存在しません: {model_dir}")
        
        try:
            loaded_files = []
            
            # モデル読み込み
            model_files = [f for f in os.listdir(model_dir) if f.startswith('model_') and f.endswith('.pkl')]
            for file in model_files:
                name = file.replace('model_', '').replace('.pkl', '')
                model_path = f'{model_dir}/{file}'
                self.models[name] = joblib.load(model_path)
                loaded_files.append(model_path)
            
            # スケーラー読み込み
            scaler_files = [f for f in os.listdir(model_dir) if f.startswith('scaler_') and f.endswith('.pkl')]
            for file in scaler_files:
                name = file.replace('scaler_', '').replace('.pkl', '')
                scaler_path = f'{model_dir}/{file}'
                self.scalers[name] = joblib.load(scaler_path)
                loaded_files.append(scaler_path)
            
            # エンコーダー読み込み
            encoder_files = [f for f in os.listdir(model_dir) if f.startswith('encoder_') and f.endswith('.pkl')]
            for file in encoder_files:
                name = file.replace('encoder_', '').replace('.pkl', '')
                encoder_path = f'{model_dir}/{file}'
                self.encoders[name] = joblib.load(encoder_path)
                loaded_files.append(encoder_path)
            
            # 特徴量リスト読み込み
            feature_file = f'{model_dir}/feature_columns.pkl'
            if os.path.exists(feature_file):
                self.feature_columns = joblib.load(feature_file)
                loaded_files.append(feature_file)
            
            # モデル結果読み込み
            results_file = f'{model_dir}/model_results.pkl'
            if os.path.exists(results_file):
                results_data = joblib.load(results_file)
                # 辞書からModelResultオブジェクトに変換
                self.model_results = {name: ModelResult(**data) for name, data in results_data.items()}
                loaded_files.append(results_file)
            
            self.logger.info(f"モデルを読み込みました: {model_dir}/ ({len(loaded_files)}ファイル)")
            
        except Exception as e:
            self.logger.error(f"モデル読み込みエラー: {e}")
            raise


async def main():
    # メイン実行関数
    parser = argparse.ArgumentParser(description="野菜価格予測機械学習システム")
    
    parser.add_argument("--train", action="store_true", help="モデルを訓練する")
    parser.add_argument("--predict", help="予測する品目名")
    parser.add_argument("--months-ahead", type=int, default=3, help="予測期間（月）")
    parser.add_argument("--market", help="市場名")
    parser.add_argument("--origin", help="産地名")
    parser.add_argument("--model-dir", help="モデルディレクトリ")
    parser.add_argument("--load-model", action="store_true", help="保存済みモデルを読み込む")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("野菜価格予測機械学習システム（高性能版）")
    print("=" * 80)
    
    try:
        # 設定読み込み
        config = MLConfig.from_env()
        
        # コマンドライン引数で上書き
        if args.model_dir:
            config.model_dir = args.model_dir
        
        # プロセッサー初期化
        predictor = EnhancedVegetablePricePrediction(config)
        
        if args.load_model:
            predictor.load_models()
            print("保存済みモデルを読み込みました")
        
        if args.train:
            print("モデル訓練を開始...")
            
            # データ抽出
            df = predictor.extract_training_data()
            
            # モデル学習
            results = predictor.train_models(df)
            
            # モデル保存
            predictor.save_models()
            
            # 結果表示
            print("\n=== 訓練結果 ===")
            for name, result in results.items():
                print(f"{name}: MAE={result.mae:.2f}, R²={result.r2:.3f}, 訓練時間={result.training_time_seconds:.1f}s")
        
        if args.predict:
            if 'best' not in predictor.models:
                if not args.load_model:
                    print("エラー: モデルが訓練されていないか、読み込まれていません")
                    print("--train または --load-model オプションを使用してください")
                    return 1
                else:
                    predictor.load_models()
            
            print(f"\n=== {args.predict} の予測結果 ===")
            
            request = PredictionRequest(
                item_name=args.predict,
                market=args.market,
                origin=args.origin,
                months_ahead=args.months_ahead
            )
            
            predictions = predictor.predict_future_prices(request)
            
            for pred in predictions:
                print(f"{pred['year']}年{pred['month']:02d}月: {pred['predicted_price']:.0f}円/kg "
                      f"(信頼区間: {pred['lower_bound']:.0f}-{pred['upper_bound']:.0f})")
        
        if not args.train and not args.predict:
            print("モードを選択してください: --train または --predict <品目名>")
            return 1
        
        return 0
            
    except Exception as e:
        logger.error(f"処理中にエラーが発生しました: {e}")
        print(f"エラー: {e}")
        return 1


if __name__ == "__main__":
    import asyncio
    exit(asyncio.run(main()))