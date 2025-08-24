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
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib
from dotenv import load_dotenv
import psutil

# ML ライブラリの可用性チェック
try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential, load_model
    from tensorflow.keras.layers import LSTM, Dense, Dropout
    from tensorflow.keras.optimizers import Adam
    from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
    TENSORFLOW_AVAILABLE = True
except ImportError:
    logging.warning("TensorFlow not available - LSTM models will be disabled")
    TENSORFLOW_AVAILABLE = False

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    logging.warning("Prophet not available - Prophet models will be disabled")
    PROPHET_AVAILABLE = False

try:
    from statsmodels.tsa.arima.model import ARIMA
    from statsmodels.tsa.seasonal import seasonal_decompose
    STATSMODELS_AVAILABLE = True
except ImportError:
    logging.warning("Statsmodels not available - ARIMA models will be disabled")
    STATSMODELS_AVAILABLE = False

try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    PLOTTING_AVAILABLE = True
except ImportError:
    logging.warning("Matplotlib/Seaborn not available - plotting will be disabled")
    PLOTTING_AVAILABLE = False

# ログ設定
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
class TimeSeriesConfig:
    # 時系列予測設定
    project_id: str
    dataset_prefix: str = "vege"
    environment: str = "dev"
    model_dir: str = "time_series_models"
    sequence_length: int = 12  # LSTM用シーケンス長
    test_size: float = 0.2
    random_state: int = 42
    
    # LSTM設定
    lstm_epochs: int = 50
    lstm_batch_size: int = 32
    lstm_learning_rate: float = 0.001
    lstm_dropout: float = 0.2
    lstm_units: int = 50
    
    # Prophet設定
    prophet_yearly_seasonality: bool = True
    prophet_weekly_seasonality: bool = False
    prophet_daily_seasonality: bool = False
    prophet_seasonality_mode: str = "multiplicative"
    
    # ARIMA設定
    arima_order: Tuple[int, int, int] = (1, 1, 1)
    
    # 一般設定
    enable_gpu: bool = True
    enable_plotting: bool = False
    min_data_points: int = 13
    outlier_threshold: float = 3.0
    
    @classmethod
    def from_env(cls) -> 'TimeSeriesConfig':
        # 環境変数から設定を作成
        load_dotenv()
        
        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            raise ValueError("GCP_PROJECT_ID environment variable is required")
        
        return cls(
            project_id=project_id,
            dataset_prefix=os.getenv("DATASET_PREFIX", "vege"),
            environment=os.getenv("ENVIRONMENT", "dev"),
            model_dir=os.getenv("TS_MODEL_DIR", "time_series_models"),
            sequence_length=int(os.getenv("TS_SEQUENCE_LENGTH", "12")),
            test_size=float(os.getenv("TS_TEST_SIZE", "0.2")),
            random_state=int(os.getenv("TS_RANDOM_STATE", "42")),
            lstm_epochs=int(os.getenv("LSTM_EPOCHS", "50")),
            lstm_batch_size=int(os.getenv("LSTM_BATCH_SIZE", "32")),
            lstm_learning_rate=float(os.getenv("LSTM_LEARNING_RATE", "0.001")),
            enable_gpu=os.getenv("ENABLE_GPU", "true").lower() == "true",
            enable_plotting=os.getenv("ENABLE_PLOTTING", "false").lower() == "true",
            min_data_points=int(os.getenv("MIN_DATA_POINTS", "13")),
            outlier_threshold=float(os.getenv("OUTLIER_THRESHOLD", "3.0"))
        )

@dataclass
class ModelResult:
    # 時系列モデル評価結果
    model_name: str
    mae: float              # Mean Absolute Error（円）
    mape: float             # Mean Absolute Percentage Error（%）
    smape: float            # Symmetric Mean Absolute Percentage Error（%）
    mase: float             # Mean Absolute Scaled Error（ナイーブ予測との比較）
    rmse: float             # Root Mean Squared Error（円）
    training_time_seconds: float = 0.0
    data_points: int = 0
    features_used: List[str] = None
    
    # 後方互換性のためR²も保持（ただし主要指標ではない）
    r2: Optional[float] = None
    mse: Optional[float] = None
    
    def __post_init__(self):
        if self.features_used is None:
            self.features_used = []
    
    @property
    def primary_score(self) -> float:
        # 主要評価スコア（低いほど良い）- MASEを使用
        return self.mase
    
    @property 
    def performance_grade(self) -> str:
        # 性能グレード判定
        if self.mase < 0.5:
            return "EXCELLENT"  # ナイーブ予測の半分以下の誤差
        elif self.mase < 0.8:
            return "GOOD"       # ナイーブ予測より大幅に良い
        elif self.mase < 1.0:
            return "FAIR"       # ナイーブ予測よりやや良い
        elif self.mase < 1.5:
            return "POOR"       # ナイーブ予測よりやや悪い
        else:
            return "VERY_POOR"  # ナイーブ予測より大幅に悪い

class EnhancedTimeSeriesPricePrediction:
    # 改善された時系列野菜価格予測クラス
    
    def __init__(self, config: TimeSeriesConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # BigQueryクライアント初期化
        try:
            self.bq_client = bigquery.Client(project=config.project_id)
        except Exception as e:
            self.logger.error(f"BigQueryクライアント初期化エラー: {e}")
            raise
        
        # GPU設定
        if TENSORFLOW_AVAILABLE and config.enable_gpu:
            self._setup_gpu()
        
        # モデル管理
        self.models: Dict[str, Any] = {}
        self.scalers: Dict[str, MinMaxScaler] = {}
        self.model_results: Dict[str, ModelResult] = {}
        
        # シーケンス長設定
        self.sequence_length = config.sequence_length
        
        # 統計情報
        self.trained_models_count = 0
        self.total_predictions = 0
        self.last_training_time = None
    
    def _setup_gpu(self) -> None:
        try:
            gpus = tf.config.experimental.list_physical_devices('GPU')
            if gpus:
                for gpu in gpus:
                    tf.config.experimental.set_memory_growth(gpu, True)
                self.logger.info(f"GPUが利用可能: {len(gpus)}台")
            else:
                self.logger.info("GPUが見つかりません。CPUで実行します")
        except Exception as e:
            self.logger.warning(f"GPU設定エラー: {e}")
    
    def _calculate_timeseries_metrics(self, y_true: np.ndarray, y_pred: np.ndarray, y_naive: np.ndarray) -> Dict[str, float]:
        # 時系列予測評価指標（MAE/MAPE/sMAPE/MASE/RMSE/R²）を計算
        try:
            # 基本誤差指標
            mae = np.mean(np.abs(y_true - y_pred))
            mse = np.mean((y_true - y_pred) ** 2)
            rmse = np.sqrt(mse)
            
            # パーセンテージ誤差（ゼロ除算回避）
            non_zero_mask = np.abs(y_true) > 1e-6
            if np.any(non_zero_mask):
                mape = np.mean(np.abs((y_true[non_zero_mask] - y_pred[non_zero_mask]) / y_true[non_zero_mask])) * 100
            else:
                mape = float('inf')
            
            # 対称MAPE（より安定した指標）
            denominator = (np.abs(y_true) + np.abs(y_pred)) / 2
            non_zero_denominator = denominator > 1e-6
            if np.any(non_zero_denominator):
                smape = np.mean(np.abs(y_true[non_zero_denominator] - y_pred[non_zero_denominator]) / denominator[non_zero_denominator]) * 100
            else:
                smape = float('inf')
            
            # ナイーブ予測との比較（MASE）
            naive_mae = np.mean(np.abs(y_true - y_naive))
            if naive_mae > 1e-6:
                mase = mae / naive_mae
            else:
                mase = float('inf')
            
            # 決定係数（参考値）
            ss_res = np.sum((y_true - y_pred) ** 2)
            ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
            r2 = 1 - (ss_res / ss_tot) if ss_tot > 1e-6 else float('-inf')
            
            return {
                'mae': mae,
                'mape': mape,
                'smape': smape, 
                'mase': mase,
                'rmse': rmse,
                'r2': r2,
                'mse': mse
            }
            
        except Exception as e:
            self.logger.error(f"評価指標計算エラー: {e}")
            return {
                'mae': float('inf'),
                'mape': float('inf'),
                'smape': float('inf'),
                'mase': float('inf'),
                'rmse': float('inf'),
                'r2': float('-inf'),
                'mse': float('inf')
            }
        
    def evaluate_model(self, df: pd.DataFrame, model_type: str, test_size: float = 0.2) -> Optional[ModelResult]:
        # バックテスト方式でモデル性能を評価
        try:
            # データ量チェック
            if len(df) < 10:
                self.logger.warning(f"データ不足のため評価をスキップ: {len(df)}行")
                return None
            
            # データ前処理とクリーニング
            ts_data = df.copy().sort_values('ds')
            ts_data = self._improve_data_quality(ts_data)
            
            # 時系列順での訓練・テスト分割
            split_point = int(len(ts_data) * (1 - test_size))
            train_data = ts_data[:split_point].copy()
            test_data = ts_data[split_point:].copy()
            
            if len(train_data) < 5 or len(test_data) < 3:
                self.logger.warning(f"分割後データ不足: train={len(train_data)}, test={len(test_data)}")
                return None
            
            start_time = time.time()
            y_true = test_data['y'].values
            y_pred = None
            
            # モデル種別に応じた予測実行
            if model_type.lower() == 'prophet' and PROPHET_AVAILABLE:
                y_pred = self._evaluate_prophet(train_data, test_data)
            elif model_type.lower() == 'lstm' and TENSORFLOW_AVAILABLE:
                y_pred = self._evaluate_lstm(train_data, test_data)
            elif model_type.lower() == 'arima' and STATSMODELS_AVAILABLE:
                y_pred = self._evaluate_arima(train_data, test_data)
            else:
                self.logger.warning(f"サポートされていないモデル: {model_type}")
                return None
            
            if y_pred is None or len(y_pred) != len(y_true):
                self.logger.warning(f"予測結果が無効: {model_type}")
                return None
            
            # ベースライン予測（ナイーブ予測）の準備
            y_naive = train_data['y'].iloc[-len(y_true):].values
            if len(y_naive) < len(y_true):
                last_value = train_data['y'].iloc[-1]
                y_naive = np.full(len(y_true), last_value)
            
            # 予測性能の評価
            metrics = self._calculate_timeseries_metrics(y_true, y_pred, y_naive)
            training_time = time.time() - start_time
            
            result = ModelResult(
                model_name=model_type.upper(),
                mae=metrics['mae'],
                mape=metrics['mape'],
                smape=metrics['smape'],
                mase=metrics['mase'],
                rmse=metrics['rmse'],
                training_time_seconds=training_time,
                data_points=len(train_data),
                r2=metrics['r2'],  # 参考値
                mse=metrics['mse']  # 参考値
            )
            
            self.logger.info(f"{model_type}評価完了: MASE={metrics['mase']:.3f}, MAE={metrics['mae']:.1f}円, sMAPE={metrics['smape']:.1f}%")
            self.logger.info(f"  性能グレード: {result.performance_grade}")
            return result
            
        except Exception as e:
            self.logger.error(f"{model_type}評価エラー: {e}")
            return None
    
    def _evaluate_prophet(self, train_data: pd.DataFrame, test_data: pd.DataFrame) -> Optional[np.ndarray]:
        # Prophet モデルでの予測実行
        try:
            # 野菜価格特性を考慮したProphet設定
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=False,
                daily_seasonality=False,
                seasonality_mode='multiplicative',  # 乗法季節性
                changepoint_prior_scale=0.05,      # トレンド変化感度
                seasonality_prior_scale=10.0,      # 季節性強度
                holidays_prior_scale=10.0,         # 祝日効果
                interval_width=0.8
            )
            
            # カスタム季節性（月次・四半期周期）
            model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
            model.add_seasonality(name='quarterly', period=91.25, fourier_order=3)
            
            # 日本の祝日・イベント設定
            holidays = pd.DataFrame({
                'holiday': ['新年', 'ゴールデンウィーク', 'お盆', '年末'],
                'ds': [
                    pd.to_datetime('2023-01-01'),
                    pd.to_datetime('2023-05-01'),
                    pd.to_datetime('2023-08-15'),
                    pd.to_datetime('2023-12-30')
                ],
                'lower_window': [0, -3, -2, -3],
                'upper_window': [3, 7, 2, 2]
            })
            
            # 複数年の祝日データ生成
            all_holidays = []
            for year in range(2021, 2027):
                year_holidays = holidays.copy()
                year_holidays['ds'] = pd.to_datetime(year_holidays['ds'].astype(str).str.replace(r'^\d{4}', str(year), regex=True))
                all_holidays.append(year_holidays)
            holidays_df = pd.concat(all_holidays, ignore_index=True)
            model.holidays = holidays_df
            
            # 外部回帰変数の設定
            regressors_added = []
            if 'avg_temperature' in train_data.columns and train_data['avg_temperature'].notna().sum() > 0:
                model.add_regressor('avg_temperature', standardize=True)
                regressors_added.append('avg_temperature')
            
            if 'total_precipitation' in train_data.columns and train_data['total_precipitation'].notna().sum() > 0:
                model.add_regressor('total_precipitation', standardize=True)
                regressors_added.append('total_precipitation')
            
            if 'yoy_price_growth_rate' in train_data.columns and train_data['yoy_price_growth_rate'].notna().sum() > 0:
                model.add_regressor('yoy_price_growth_rate', standardize=True)
                regressors_added.append('yoy_price_growth_rate')
            
            # 学習データの欠損値処理
            train_cols = ['ds', 'y'] + regressors_added
            train_clean = train_data[train_cols].copy()
            for col in regressors_added:
                train_clean[col] = train_clean[col].fillna(train_clean[col].median())
            
            # モデル学習
            model.fit(train_clean)
            
            # テストデータの準備と予測
            test_cols = ['ds'] + regressors_added
            future = test_data[test_cols].copy()
            for col in regressors_added:
                if col in future.columns:
                    future[col] = future[col].fillna(train_clean[col].median())
                else:
                    future[col] = train_clean[col].median()
            
            forecast = model.predict(future)
            return forecast['yhat'].values
            
        except Exception as e:
            self.logger.warning(f"Prophet評価エラー: {e}")
            return None
    
    def _evaluate_lstm(self, train_data: pd.DataFrame, test_data: pd.DataFrame) -> Optional[np.ndarray]:
        # 多変量LSTM モデルでの予測実行
        try:
            if len(train_data) < self.sequence_length + 2:
                return None
            
            # 多変量特徴量の選択
            feature_cols = ['y']
            if 'avg_temperature' in train_data.columns:
                feature_cols.append('avg_temperature')
            if 'total_precipitation' in train_data.columns:
                feature_cols.append('total_precipitation')
            if 'yoy_price_growth_rate' in train_data.columns:
                feature_cols.append('yoy_price_growth_rate')
            if 'seasonal_price_index' in train_data.columns:
                feature_cols.append('seasonal_price_index')
            
            # 欠損値補完
            train_features = train_data[feature_cols].ffill().bfill()
            test_features = test_data[feature_cols].ffill().bfill()
            
            # データの正規化（多変量対応）
            scaler_X = MinMaxScaler()
            scaler_y = MinMaxScaler()
            
            train_scaled = scaler_X.fit_transform(train_features)
            test_scaled = scaler_X.transform(test_features)
            
            y_train_scaled = scaler_y.fit_transform(train_features[['y']])
            
            # LSTM用シーケンスデータの生成
            X_train, y_train = self._create_sequences_multivariate(train_scaled, y_train_scaled, self.sequence_length)
            X_test, _ = self._create_sequences_multivariate(
                np.concatenate([train_scaled[-self.sequence_length:], test_scaled]), 
                np.concatenate([y_train_scaled[-self.sequence_length:], scaler_y.transform(test_features[['y']])]),
                self.sequence_length
            )
            
            if len(X_train) < 10:  # 最小データ数を増やす
                return None
            
            # 多層LSTM モデルの構築
            model = Sequential([
                LSTM(128, return_sequences=True, input_shape=(self.sequence_length, len(feature_cols))),
                Dropout(0.3),
                LSTM(64, return_sequences=True),
                Dropout(0.3),
                LSTM(32, return_sequences=False),
                Dropout(0.2),
                Dense(16, activation='relu'),
                Dense(1)
            ])
            
            # モデルのコンパイル（Huber損失関数使用）
            model.compile(
                optimizer=tf.keras.optimizers.AdamW(learning_rate=0.001, weight_decay=0.01),
                loss='huber',
                metrics=['mae']
            )
            
            # 早期終了・学習率調整の設定
            callbacks = [
                tf.keras.callbacks.EarlyStopping(patience=10, restore_best_weights=True, monitor='loss'),
                tf.keras.callbacks.ReduceLROnPlateau(factor=0.7, patience=5, min_lr=0.0001, monitor='loss')
            ]
            
            # モデル学習
            model.fit(
                X_train, y_train, 
                epochs=100, 
                batch_size=32, 
                verbose=0,
                callbacks=callbacks,
                validation_split=0.1
            )
            
            # 予測と逆正規化
            y_pred_scaled = model.predict(X_test[:len(test_data)], verbose=0)
            y_pred = scaler_y.inverse_transform(y_pred_scaled).flatten()
            
            return y_pred[:len(test_data)]
            
        except Exception as e:
            self.logger.warning(f"LSTM評価エラー: {e}")
            return None
    
    def _create_sequences_multivariate(self, data_X, data_y, seq_length):
        # 多変量LSTMのためのシーケンスデータ作成
        sequences_X = []
        sequences_y = []
        for i in range(len(data_X) - seq_length):
            seq_X = data_X[i:i + seq_length]
            seq_y = data_y[i + seq_length]
            sequences_X.append(seq_X)
            sequences_y.append(seq_y)
        return np.array(sequences_X), np.array(sequences_y)
    
    def _improve_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        # 時系列データの品質を改善
        try:
            data = df.copy()
            
            # 1. 外れ値の除去（IQR法）
            if 'y' in data.columns:
                Q1 = data['y'].quantile(0.25)
                Q3 = data['y'].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 2.5 * IQR  # より寛容な外れ値除去
                upper_bound = Q3 + 2.5 * IQR
                
                outliers_mask = (data['y'] < lower_bound) | (data['y'] > upper_bound)
                outliers_count = outliers_mask.sum()
                
                if outliers_count > 0:
                    # 外れ値を中央値で置換（除去ではなく修正）
                    median_price = data['y'].median()
                    data.loc[outliers_mask, 'y'] = median_price
                    self.logger.info(f"外れ値を修正: {outliers_count}件")
            
            # 2. 欠損値の補間
            numeric_cols = data.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                if data[col].isnull().any():
                    # 時系列的な線形補間
                    data[col] = data[col].interpolate(method='linear')
                    # 残った欠損値を前後の値で埋める
                    data[col] = data[col].ffill().bfill()
            
            # 価格データの平滑化
            if 'y' in data.columns and len(data) > 6:
                data['y_smoothed'] = data['y'].rolling(window=3, center=True).mean()
                data['y'] = data['y_smoothed'].fillna(data['y'])
                data.drop('y_smoothed', axis=1, inplace=True)
            
            # 特徴量エンジニアリング
            if 'y' in data.columns:
                # ラグ特徴量
                data['y_lag1'] = data['y'].shift(1)
                data['y_lag3'] = data['y'].shift(3)
                
                # 移動平均特徴量
                if len(data) > 6:
                    data['y_ma3'] = data['y'].rolling(window=3).mean()
                    data['y_ma6'] = data['y'].rolling(window=6).mean()
                
                # 価格変動率
                data['y_pct_change'] = data['y'].pct_change()
                
                # トレンド分解（長期データがある場合）
                if len(data) > 24:
                    try:
                        from scipy import stats
                        x = np.arange(len(data))
                        slope, intercept, _, _, _ = stats.linregress(x, data['y'])
                        trend = slope * x + intercept
                        data['y_detrended'] = data['y'] - trend
                        data['trend'] = trend
                    except:
                        pass
            
            # 最終的な欠損値処理
            data = data.ffill().bfill()
            
            return data
            
        except Exception as e:
            self.logger.warning(f"データ品質改善エラー: {e}")
            return df
    
    def _evaluate_arima(self, train_data: pd.DataFrame, test_data: pd.DataFrame) -> Optional[np.ndarray]:
        # ARIMA評価用の予測（自動パラメータ選択）
        try:
            # 季節性ARIMAパラメータの候補
            param_combinations = [
                (1, 1, 1),  # 基本ARIMA
                (2, 1, 1),  # AR項を増やす
                (1, 1, 2),  # MA項を増やす
                (2, 1, 2),  # AR+MA項を増やす
                (1, 0, 1),  # 差分なし
                (0, 1, 1),  # AR項なし
                (1, 2, 1),  # 2次差分
            ]
            
            # 季節性ARIMA (野菜価格は12ヶ月周期)
            seasonal_combinations = [
                (1, 1, 1, 12),  # 季節性ARIMA
                (0, 1, 1, 12),  # 季節性MA
                (1, 0, 0, 12),  # 季節性AR
            ]
            
            best_aic = float('inf')
            best_model = None
            
            # 通常ARIMAパラメータ探索
            for order in param_combinations:
                try:
                    model = ARIMA(train_data['y'], order=order)
                    fitted = model.fit()
                    if fitted.aic < best_aic:
                        best_aic = fitted.aic
                        best_model = fitted
                except:
                    continue
            
            # 季節性ARIMAパラメータ探索（データが十分な場合）
            if len(train_data) > 36:  # 3年分以上のデータがある場合
                for seasonal_order in seasonal_combinations:
                    try:
                        model = ARIMA(train_data['y'], order=(1, 1, 1), seasonal_order=seasonal_order)
                        fitted = model.fit()
                        if fitted.aic < best_aic:
                            best_aic = fitted.aic
                            best_model = fitted
                    except:
                        continue
            
            if best_model is None:
                self.logger.warning("ARIMA: 適切なパラメータが見つかりませんでした")
                return None
            
            # 最適モデルで予測
            forecast = best_model.forecast(steps=len(test_data))
            self.logger.info(f"ARIMA最適パラメータ: AIC={best_aic:.2f}")
            return forecast.values
            
        except Exception as e:
            self.logger.warning(f"ARIMA評価エラー: {e}")
            return None
    
    def _create_sequences(self, data, seq_length):
        # LSTMのためのシーケンスデータ作成
        sequences = []
        targets = []
        for i in range(len(data) - seq_length):
            seq = data[i:i + seq_length]
            target = data[i + seq_length]
            sequences.append(seq)
            targets.append(target)
        return np.array(sequences), np.array(targets)
        
    def extract_time_series_data(self, item_name: str, market: Optional[str] = None, origin: Optional[str] = None) -> pd.DataFrame:
        # 特定品目の時系列データを抽出
        try:
            query = f"""
            SELECT 
              DATE(pw.year, pw.month, 1) as ds,
              pw.year,
              pw.month,
              pw.item_name,
              pw.market,
              pw.origin,
              pw.avg_unit_price_yen as y,
              pw.total_quantity_kg,
              pw.total_amount_yen,
              pw.yoy_price_growth_rate,
              pw.mom_price_growth_rate,
              pw.price_3month_moving_avg,
              pw.avg_temperature,
              pw.total_precipitation,
              pw.avg_wind_speed,
              sa.seasonal_price_index,
              sa.seasonal_quantity_index
            FROM `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.mart_price_weather` pw
            LEFT JOIN `{self.config.project_id}.{self.config.dataset_prefix}_{self.config.environment}_mart.mart_seasonal_analysis` sa
              ON pw.item_name = sa.item_name AND pw.month = sa.month
            WHERE pw.item_name = @item_name
              AND pw.avg_unit_price_yen IS NOT NULL
              AND pw.avg_unit_price_yen > 0
            """
            
            query_params = [bigquery.ScalarQueryParameter("item_name", "STRING", item_name)]
            
            if market:
                query += " AND pw.market = @market"
                query_params.append(bigquery.ScalarQueryParameter("market", "STRING", market))
            if origin:
                query += " AND pw.origin = @origin"
                query_params.append(bigquery.ScalarQueryParameter("origin", "STRING", origin))
                
            query += " ORDER BY pw.year, pw.month"
            
            self.logger.info(f"時系列データを抽出中: {item_name}")
            
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)
            df = self.bq_client.query(query, job_config=job_config).to_dataframe()
            
            if df.empty:
                raise ValueError(f"データが見つかりません: {item_name}")
            
            # 外れ値除去
            df = self._remove_outliers(df)
            
            self.logger.info(f"抽出完了: {len(df)}行 ({df['ds'].min()} - {df['ds'].max()})")
            return df
            
        except Exception as e:
            self.logger.error(f"時系列データ抽出エラー: {e}")
            raise
    
    def _remove_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        # 改善された外れ値除去とデータ平滑化
        try:
            if 'y' not in df.columns or len(df) < 20:
                return df
            
            original_len = len(df)
            
            # 1. IQR法による外れ値検出（より保守的）
            Q1 = df['y'].quantile(0.25)
            Q3 = df['y'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # 2. Z-score法も併用（閾値を緩和）
            z_scores = np.abs((df['y'] - df['y'].mean()) / df['y'].std())
            
            # 3. 移動平均からの乖離も考慮
            df['ma_7'] = df['y'].rolling(window=7, min_periods=1).mean()
            ma_deviation = np.abs(df['y'] - df['ma_7']) / df['ma_7']
            
            # 複合的な外れ値判定
            outliers = (
                (df['y'] < lower_bound) | (df['y'] > upper_bound) |
                (z_scores > 4.0) |  # Z-scoreの閾値を緩和
                (ma_deviation > 0.8)  # 移動平均からの乖離80%以上
            )
            
            if outliers.sum() > 0:
                removed_count = outliers.sum()
                df = df[~outliers].copy()
                self.logger.info(f"外れ値を{removed_count}件除去しました ({original_len} → {len(df)})")
            
            # 4. データ平滑化（移動平均でノイズ軽減）
            if len(df) > 14:
                df['y_smoothed'] = df['y'].rolling(window=3, min_periods=1).mean()
                df['y'] = df['y_smoothed']
                df = df.drop(['ma_7', 'y_smoothed'], axis=1, errors='ignore')
            
            return df.drop(['ma_7'], axis=1, errors='ignore')
            
        except Exception as e:
            self.logger.warning(f"外れ値除去エラー: {e}")
            return df
    
    def create_lstm_sequences(self, data: np.ndarray, target_col: int = 0, sequence_length: Optional[int] = None) -> Tuple[np.ndarray, np.ndarray]:
        # LSTM用のシーケンスデータを作成
        sequence_length = sequence_length or self.config.sequence_length
        
        if len(data) <= sequence_length:
            raise ValueError(f"データが不足です。最低{sequence_length + 1}ポイント必要です")
        
        X, y = [], []
        
        for i in range(sequence_length, len(data)):
            # 過去sequence_length期間のデータを特徴量とする
            X.append(data[i-sequence_length:i])
            y.append(data[i, target_col])
        
        return np.array(X), np.array(y)
    
    def build_lstm_model(self, input_shape: Tuple[int, int]) -> Any:
        # LSTMモデルを構築
        if not TENSORFLOW_AVAILABLE:
            raise RuntimeError("TensorFlowが利用できません")
        
        try:
            # 軽量化されたLSTMモデル（パラメータ数削減）
            units = min(self.config.lstm_units, 32)  # ユニット数を制限
            model = Sequential([
                LSTM(units, return_sequences=False, input_shape=input_shape),  # 1層のみ
                Dropout(0.2),  # ドロップアウト率を固定
                Dense(16, activation='relu'),  # 中間層も削減
                Dense(1)
            ])
            
            model.compile(
                optimizer=Adam(learning_rate=self.config.lstm_learning_rate), 
                loss='mse',
                metrics=['mae']
            )
            
            self.logger.info(f"LSTMモデル構築完了: {model.count_params():,}パラメータ")
            return model
            
        except Exception as e:
            self.logger.error(f"LSTMモデル構築エラー: {e}")
            raise
    
    def train_lstm_model(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, Any]]:
        # LSTMモデルを訓練
        if not TENSORFLOW_AVAILABLE:
            self.logger.warning("TensorFlowが利用できません。LSTM訓練をスキップします")
            return None, {}
        
        start_time = time.time()
        self.logger.info("LSTMモデルを訓練中...")
        
        try:
            # 特徴量準備
            feature_cols = ['y', 'total_quantity_kg', 'avg_temperature', 
                           'total_precipitation', 'seasonal_price_index']
            feature_cols = [col for col in feature_cols if col in df.columns]
            
            if len(feature_cols) < 2:
                raise ValueError("必要な特徴量が不足です")
            
            # 時系列分割を先に行う（データリーケージ防止）
            split_point = int(len(df) * (1 - self.config.test_size))
            train_data = df[:split_point][feature_cols].fillna(0)
            test_data = df[split_point:][feature_cols].fillna(0)
            
            # 訓練データのみでスケーラーを学習
            scaler = MinMaxScaler()
            train_scaled = scaler.fit_transform(train_data)
            test_scaled = scaler.transform(test_data)  # 学習済みスケーラーでテストデータを変換
            self.scalers['lstm'] = scaler
            
            # 訓練・テスト各々でシーケンス作成
            X_train, y_train = self.create_lstm_sequences(
                train_scaled, 
                target_col=feature_cols.index('y')
            )
            X_test, y_test = self.create_lstm_sequences(
                test_scaled,
                target_col=feature_cols.index('y') 
            )
            
            if len(X_train) < self.config.min_data_points or len(X_test) < 5:
                raise ValueError(f"データが不足です。訓練:{len(X_train)}, テスト:{len(X_test)}")
            
            self.logger.info(f"シーケンスデータ作成完了: X_train={X_train.shape}, X_test={X_test.shape}")
            
            self.logger.info(f"データ分割: 訓練={len(X_train)}, テスト={len(X_test)}")
            
            # モデル構築・訓練
            model = self.build_lstm_model((X_train.shape[1], X_train.shape[2]))
            
            # 早期終了を強化（時間短縮）
            callbacks = [
                EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True),  # patience削減
                ReduceLROnPlateau(monitor='val_loss', factor=0.7, patience=3, min_lr=1e-5)  # より早く学習率調整
            ]
            
            # エポック数とバッチサイズを最適化
            epochs = min(self.config.lstm_epochs, 30)  # 最大30エポックに制限
            batch_size = max(self.config.lstm_batch_size, 64)  # バッチサイズを大きく
            
            history = model.fit(
                X_train, y_train,
                epochs=epochs,
                batch_size=batch_size,
                validation_data=(X_test, y_test),
                callbacks=callbacks,
                verbose=0  # ログ出力を抑制
            )
        
            # 予測・評価
            y_pred = model.predict(X_test, verbose=0)
            
            # 元のスケールに戻す
            dummy_zeros = np.zeros((len(y_test), len(feature_cols) - 1))
            y_test_full = np.column_stack([y_test, dummy_zeros])
            y_pred_full = np.column_stack([y_pred.flatten(), dummy_zeros])
            
            y_test_original = scaler.inverse_transform(y_test_full)[:, 0]
            y_pred_original = scaler.inverse_transform(y_pred_full)[:, 0]
            
            # 評価指標
            mse = mean_squared_error(y_test_original, y_pred_original)
            mae = mean_absolute_error(y_test_original, y_pred_original)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_test_original, y_pred_original)
            
            # MAPE, sMAPE, MASE計算
            mape = np.mean(np.abs((y_test_original - y_pred_original) / np.maximum(np.abs(y_test_original), 1e-8))) * 100
            smape = np.mean(2 * np.abs(y_test_original - y_pred_original) / np.maximum(np.abs(y_test_original) + np.abs(y_pred_original), 1e-8)) * 100
            
            # MASE計算（ナイーブ予測との比較）
            if len(y_test_original) > 1:
                naive_forecast_errors = np.abs(np.diff(y_test_original[:-1]))  
                naive_mae = np.mean(naive_forecast_errors) if len(naive_forecast_errors) > 0 else 1.0
                mase = mae / max(naive_mae, 1e-8)
            else:
                mase = 1.0
            
            training_time = time.time() - start_time
            
            # 結果保存
            result = ModelResult(
                model_name="lstm",
                mae=mae,
                mape=mape,
                smape=smape,
                mase=mase,
                rmse=rmse,
                training_time_seconds=training_time,
                data_points=len(df),
                features_used=feature_cols,
                mse=mse,
                r2=r2
            )
            
            self.model_results['lstm'] = result
            
            self.logger.info(f"LSTM評価結果:")
            self.logger.info(f"  MSE: {mse:.2f}")
            self.logger.info(f"  MAE: {mae:.2f}")
            self.logger.info(f"  RMSE: {rmse:.2f}")
            self.logger.info(f"  R²: {r2:.3f}")
            self.logger.info(f"  訓練時間: {training_time:.2f}秒")
            
            self.models['lstm'] = model
            self.trained_models_count += 1
            
            return model, {
                'history': history.history,
                'evaluation': asdict(result)
            }
            
        except Exception as e:
            self.logger.error(f"LSTM訓練エラー: {e}")
            raise
    
    def train_prophet_model(self, df: pd.DataFrame) -> Tuple[Any, Dict[str, Any]]:
        # Prophetモデルを訓練
        if not PROPHET_AVAILABLE:
            self.logger.warning("Prophetが利用できません。Prophet訓練をスキップします")
            return None, {}
        
        start_time = time.time()
        self.logger.info("Prophetモデルを訓練中...")
        
        try:
            # Prophet用データ準備
            prophet_df = df[['ds', 'y']].copy()
            
            # データ型を明示的に変換
            prophet_df['ds'] = pd.to_datetime(prophet_df['ds'])
            prophet_df['y'] = pd.to_numeric(prophet_df['y'], errors='coerce')
            
            # NaN値を除去
            prophet_df = prophet_df.dropna()
            
            # 外部変数（regressor）を追加（エラー防止のため簡略化）
            regressors = []
            
            # シンプルなProphetモデルで安全に実行
            if len(prophet_df) < 50:
                self.logger.warning(f"Prophet用データが不足: {len(prophet_df)}行")
                return None, {}
            
            # 必要に応じて外部変数を追加（エラー発生時はスキップ）
            try:
                if 'avg_temperature' in df.columns and df['avg_temperature'].notna().sum() > len(df) * 0.5:
                    temp_data = pd.to_numeric(df['avg_temperature'], errors='coerce').fillna(df['avg_temperature'].mean())
                    if not temp_data.isna().all():
                        prophet_df['temperature'] = temp_data[:len(prophet_df)]
                        regressors.append('temperature')
            except Exception as e:
                self.logger.warning(f"Temperature変数追加エラー: {e}")
            
            # 再度NaN値を除去（必要な列のみ）
            essential_cols = ['ds', 'y']
            prophet_df = prophet_df.dropna(subset=essential_cols)
            
            # データの最終確認
            if len(prophet_df) < 30:
                self.logger.warning(f"Prophet用データが不足（最終）: {len(prophet_df)}行")
                return None, {}
            
            # 訓練・テスト分割
            split_point = int(len(prophet_df) * 0.8)
            train_df = prophet_df[:split_point].copy()
            test_df = prophet_df[split_point:].copy()
            
            # 最適化されたProphetモデル
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=False,
                daily_seasonality=False,
                seasonality_mode='additive',  # 乗法から加法に変更（計算高速化）
                changepoint_prior_scale=0.01,  # 変化点検出を抑制（過学習防止）
                seasonality_prior_scale=1.0,   # 季節性の強度調整
                holidays_prior_scale=1.0,      # 祝日効果の強度
                uncertainty_samples=100,       # サンプル数削減（高速化）
                mcmc_samples=0,                # MCMCサンプリング無効化（高速化）
                interval_width=0.8,            # 信頼区間を80%に設定
                n_changepoints=10              # 変化点数を制限
            )
            
            # 外部変数追加（エラーハンドリング付き）
            try:
                for regressor in regressors:
                    model.add_regressor(regressor)
            except Exception as e:
                self.logger.warning(f"Regressor追加エラー: {e}。シンプルモデルで続行")
                regressors = []  # リセット
            
            # 訓練（エラーハンドリング付き）
            try:
                # NaN値がある場合の前処理
                if train_df.isnull().any().any():
                    self.logger.warning("Prophet訓練データにNaN値があります。除去します")
                    train_df = train_df.dropna()
                
                model.fit(train_df)
            except Exception as e:
                self.logger.error(f"Prophetモデル訓練エラー: {e}")
                return None, {}
            
            # 予測（エラーハンドリング付き）
            try:
                future = test_df[['ds'] + regressors].copy()
                if future.isnull().any().any():
                    future = future.ffill().bfill()
                forecast = model.predict(future)
            except Exception as e:
                self.logger.error(f"Prophet予測エラー: {e}")
                return None, {}
            
            # 評価
            y_true = test_df['y'].values
            y_pred = forecast['yhat'].values
            
            mse = mean_squared_error(y_true, y_pred)
            mae = mean_absolute_error(y_true, y_pred)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_true, y_pred)
            
            # MAPE, sMAPE, MASE計算
            mape = np.mean(np.abs((y_true - y_pred) / np.maximum(np.abs(y_true), 1e-8))) * 100
            smape = np.mean(2 * np.abs(y_true - y_pred) / np.maximum(np.abs(y_true) + np.abs(y_pred), 1e-8)) * 100
            
            # MASE計算（ナイーブ予測との比較）
            if len(y_true) > 1:
                naive_forecast_errors = np.abs(np.diff(y_true[:-1]))
                naive_mae = np.mean(naive_forecast_errors) if len(naive_forecast_errors) > 0 else 1.0
                mase = mae / max(naive_mae, 1e-8)
            else:
                mase = 1.0
                
            training_time = time.time() - start_time
            
            # 結果保存
            result = ModelResult(
                model_name="prophet",
                mae=mae,
                mape=mape,
                smape=smape,
                mase=mase,
                rmse=rmse,
                training_time_seconds=training_time,
                data_points=len(df),
                features_used=regressors + ['ds', 'y'],
                mse=mse,
                r2=r2
            )
            
            self.model_results['prophet'] = result
            
            self.logger.info(f"Prophet評価結果:")
            self.logger.info(f"  MSE: {mse:.2f}")
            self.logger.info(f"  MAE: {mae:.2f}")
            self.logger.info(f"  RMSE: {rmse:.2f}")
            self.logger.info(f"  R²: {r2:.3f}")
            self.logger.info(f"  訓練時間: {training_time:.2f}秒")
            
            self.models['prophet'] = model
            self.trained_models_count += 1
            
            return model, {
                'forecast': forecast,
                'evaluation': asdict(result),
                'regressors': regressors
            }
            
        except Exception as e:
            self.logger.error(f"Prophet訓練エラー: {e}")
            raise
    
    def train_arima_model(self, df):
        # ARIMAモデルを訓練
        print("ARIMAモデルを訓練中...")
        
        # 時系列データ準備
        ts_data = df.set_index('ds')['y']
        
        # 訓練・テスト分割
        split_point = int(len(ts_data) * 0.8)
        train_data = ts_data[:split_point]
        test_data = ts_data[split_point:]
        
        # ARIMA(1,1,1)モデル（簡略化）
        # 実際の運用では auto_arima を使用してパラメータを自動選択することを推奨
        try:
            model = ARIMA(train_data, order=(1, 1, 1))
            fitted_model = model.fit()
            
            # 予測
            forecast = fitted_model.forecast(steps=len(test_data))
            
            # 評価
            mse = mean_squared_error(test_data, forecast)
            mae = mean_absolute_error(test_data, forecast)
            
            print(f"ARIMA評価結果:")
            print(f"  MSE: {mse:.2f}")
            print(f"  MAE: {mae:.2f}")
            print(f"  RMSE: {np.sqrt(mse):.2f}")
            
            self.models['arima'] = fitted_model
            return fitted_model, forecast
            
        except Exception as e:
            print(f"ARIMA訓練に失敗: {e}")
            return None, None
    
    def predict_future(self, item_name, market=None, origin=None, months_ahead=6, model_type='prophet'):
        # 未来価格を予測
        if model_type not in self.models:
            raise ValueError(f"モデル {model_type} が訓練されていません")
        
        # 最新データ取得
        df = self.extract_time_series_data(item_name, market, origin)
        
        if model_type == 'prophet':
            return self._predict_with_prophet(df, months_ahead)
        elif model_type == 'lstm':
            return self._predict_with_lstm(df, months_ahead)
        elif model_type == 'arima':
            return self._predict_with_arima(df, months_ahead)
        else:
            raise ValueError(f"サポートされていないモデル: {model_type}")
    
    def _predict_with_prophet(self, df, months_ahead):
        # Prophetで予測
        model = self.models['prophet']
        
        # 未来の日付を生成（最新データ月の翌月から開始）
        last_date = pd.to_datetime(df['ds'].max())
        last_data_month = f"{last_date.year}年{last_date.month}月"
        
        # 最新データ月の翌月1日から開始
        if last_date.day != 1:
            # 月の最後の日付の場合、翌月1日に設定
            next_month_start = (pd.Timestamp(last_date.year, last_date.month, 1) + pd.DateOffset(months=1))
        else:
            # 月初の場合、翌月1日に設定
            next_month_start = last_date + pd.DateOffset(months=1)
        
        future_dates = pd.date_range(
            start=next_month_start,
            periods=months_ahead,
            freq='MS'  # 月初
        )
        
        prediction_period = f"{future_dates[0].strftime('%Y年%m月')} - {future_dates[-1].strftime('%Y年%m月')}"
        self.logger.info(f"Prophet予測: 最新データ月={last_data_month}, 予測期間={prediction_period}")
        
        # 外部変数の値を推定（簡略化）
        future_df = pd.DataFrame({'ds': future_dates})
        
        # 過去の平均値を使用（実際にはより高度な予測が必要）
        if 'avg_temperature' in df.columns:
            future_df['temperature'] = df.groupby('month')['avg_temperature'].mean().reindex(
                future_dates.month
            ).values
        if 'total_precipitation' in df.columns:
            future_df['precipitation'] = df.groupby('month')['total_precipitation'].mean().reindex(
                future_dates.month
            ).values
        if 'total_quantity_kg' in df.columns:
            future_df['quantity'] = df['total_quantity_kg'].mean()
        
        # 予測実行
        forecast = model.predict(future_df)
        
        return pd.DataFrame({
            'date': future_dates,
            'predicted_price': forecast['yhat'],
            'lower_bound': forecast['yhat_lower'],
            'upper_bound': forecast['yhat_upper']
        })
    
    def _predict_with_lstm(self, df, months_ahead):
        # LSTMで予測
        if 'lstm' not in self.scalers:
            raise ValueError("LSTMスケーラーが見つかりません")
        
        model = self.models['lstm']
        scaler = self.scalers['lstm']
        
        # 特徴量準備
        feature_cols = ['y', 'total_quantity_kg', 'avg_temperature', 
                       'total_precipitation', 'seasonal_price_index']
        feature_cols = [col for col in feature_cols if col in df.columns]
        
        # 最新データを正規化
        recent_data = df[feature_cols].tail(self.sequence_length).values
        scaled_recent = scaler.transform(recent_data)
        
        predictions = []
        input_sequence = scaled_recent.copy()
        
        for _ in range(months_ahead):
            # 予測
            pred_input = input_sequence[-self.sequence_length:].reshape(1, self.sequence_length, len(feature_cols))
            pred_scaled = model.predict(pred_input, verbose=0)[0, 0]
            
            # 元のスケールに戻す
            dummy_row = np.zeros((1, len(feature_cols)))
            dummy_row[0, 0] = pred_scaled
            pred_original = scaler.inverse_transform(dummy_row)[0, 0]
            
            predictions.append(pred_original)
            
            # 次の予測のために新しい行を追加（簡略化）
            new_row = input_sequence[-1].copy()
            new_row[0] = pred_scaled  # 価格を更新
            input_sequence = np.vstack([input_sequence, new_row])
        
        # 未来の日付を生成（最新データ月の翌月から開始）
        last_date = pd.to_datetime(df['ds'].max())
        # 最新データ月の翌月1日から開始
        if last_date.day != 1:
            next_month_start = (pd.Timestamp(last_date.year, last_date.month, 1) + pd.DateOffset(months=1))
        else:
            next_month_start = last_date + pd.DateOffset(months=1)
        
        future_dates = pd.date_range(
            start=next_month_start,
            periods=months_ahead,
            freq='MS'
        )
        
        return pd.DataFrame({
            'date': future_dates,
            'predicted_price': predictions
        })
    
    def _predict_with_arima(self, df, months_ahead):
        # ARIMAで予測
        model = self.models['arima']
        
        # 予測実行
        forecast = model.forecast(steps=months_ahead)
        conf_int = model.get_forecast(steps=months_ahead).conf_int()
        
        # 未来の日付を生成（最新データ月の翌月から開始）
        last_date = pd.to_datetime(df['ds'].max())
        # 最新データ月の翌月1日から開始
        if last_date.day != 1:
            next_month_start = (pd.Timestamp(last_date.year, last_date.month, 1) + pd.DateOffset(months=1))
        else:
            next_month_start = last_date + pd.DateOffset(months=1)
        
        future_dates = pd.date_range(
            start=next_month_start,
            periods=months_ahead,
            freq='MS'
        )
        
        return pd.DataFrame({
            'date': future_dates,
            'predicted_price': forecast,
            'lower_bound': conf_int.iloc[:, 0],
            'upper_bound': conf_int.iloc[:, 1]
        })
    
    def visualize_predictions(self, df, predictions, title="価格予測"):
        # 予測結果を可視化
        plt.figure(figsize=(12, 6))
        
        # 実績データ
        plt.plot(df['ds'], df['y'], label='実績', color='blue', linewidth=2)
        
        # 予測データ
        plt.plot(predictions['date'], predictions['predicted_price'], 
                label='予測', color='red', linewidth=2, linestyle='--')
        
        # 信頼区間（ある場合）
        if 'lower_bound' in predictions.columns:
            plt.fill_between(predictions['date'], 
                           predictions['lower_bound'], 
                           predictions['upper_bound'],
                           alpha=0.3, color='red', label='信頼区間')
        
        plt.title(title)
        plt.xlabel('日付')
        plt.ylabel('単価 (円/kg)')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.show()
    
    def save_models(self, model_dir='time_series_models'):
        # モデルを保存
        os.makedirs(model_dir, exist_ok=True)
        
        # LSTMモデル
        if 'lstm' in self.models:
            self.models['lstm'].save(f'{model_dir}/lstm_model.h5')
        
        # Prophetモデル
        if 'prophet' in self.models:
            joblib.dump(self.models['prophet'], f'{model_dir}/prophet_model.pkl')
        
        # ARIMAモデル
        if 'arima' in self.models:
            joblib.dump(self.models['arima'], f'{model_dir}/arima_model.pkl')
        
        # スケーラー
        for name, scaler in self.scalers.items():
            joblib.dump(scaler, f'{model_dir}/scaler_{name}.pkl')
        
        print(f"時系列モデルを保存しました: {model_dir}/")
    
    def train_all_models(self, item_name, market=None, origin=None):
        # 全モデル種（Prophet/LSTM/ARIMA）の訓練実行
        print(f"=== {item_name} の時系列予測モデル訓練 ===")
        
        # 時系列データの抽出
        df = self.extract_time_series_data(item_name, market, origin)
        
        results = {}
        
        # Prophet モデルの訓練
        try:
            prophet_model, prophet_forecast = self.train_prophet_model(df)
            results['prophet'] = {'model': prophet_model, 'forecast': prophet_forecast}
        except Exception as e:
            print(f"Prophet訓練エラー: {e}")
        
        # LSTM モデルの訓練
        try:
            lstm_model, lstm_history = self.train_lstm_model(df)
            results['lstm'] = {'model': lstm_model, 'history': lstm_history}
        except Exception as e:
            print(f"LSTM訓練エラー: {e}")
        
        # ARIMA モデルの訓練
        try:
            arima_model, arima_forecast = self.train_arima_model(df)
            if arima_model:
                results['arima'] = {'model': arima_model, 'forecast': arima_forecast}
        except Exception as e:
            print(f"ARIMA訓練エラー: {e}")
        
        return results


def main():
    # メイン実行関数（サンプル品目での予測モデル訓練）
    print("=== 野菜価格時系列予測システム ===")
    
    # システムの初期化
    config = TimeSeriesConfig.from_env()
    predictor = EnhancedTimeSeriesPricePrediction(config)
    
    # テスト対象品目
    target_items = ["キャベツ", "トマト", "じゃがいも"]
    
    # 各品目のモデル訓練と予測
    for item in target_items:
        try:
            print(f"\n=== {item} の分析 ===")
            
            # 全モデル種の訓練
            results = predictor.train_all_models(item)
            
            # Prophetモデルでの6ヶ月予測
            if 'prophet' in predictor.models:
                predictions = predictor.predict_future(item, months_ahead=6, model_type='prophet')
                print(f"\n{item} の6ヶ月予測:")
                for _, row in predictions.iterrows():
                    print(f"  {row['date'].strftime('%Y-%m')}: {row['predicted_price']:.0f}円/kg")
            
        except Exception as e:
            print(f"{item} の処理でエラー: {e}")
            continue
    
    # 学習済みモデルの保存
    predictor.save_models()
    print("\n全モデルを保存しました")


if __name__ == "__main__":
    main()