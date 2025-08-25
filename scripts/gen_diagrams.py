import os
import logging
import time
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Union, Any, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
from dotenv import load_dotenv
import psutil

try:
    from diagrams import Diagram, Cluster, Node, Edge
    
    # GCP サービス（公式アイコン）
    from diagrams.gcp.analytics import Bigquery, Dataflow, Pubsub, Dataproc
    from diagrams.gcp.compute import AppEngine, Functions, ComputeEngine, KubernetesEngine
    from diagrams.gcp.storage import Storage as GCS
    from diagrams.gcp.ml import AIPlatform, Automl
    
    # オンプレミス・汎用サービス  
    from diagrams.onprem.client import Users
    from diagrams.onprem.compute import Server
    from diagrams.onprem.inmemory import Redis
    from diagrams.onprem.network import Internet
    from diagrams.onprem.workflow import Airflow
    from diagrams.onprem.analytics import Dbt
    from diagrams.onprem.monitoring import Grafana
    
    # プログラミング言語・フレームワーク
    from diagrams.programming.language import Python
    
    # 汎用アイコン
    from diagrams.generic.storage import Storage
    from diagrams.generic.database import SQL
    from diagrams.generic.compute import Rack
    from diagrams.generic.blank import Blank
    
    DIAGRAMS_AVAILABLE = True
    
except ImportError as e:
    logging.warning(f"Diagrams library not available: {e}")
    DIAGRAMS_AVAILABLE = False

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
class DiagramConfig:
    # 図表生成設定
    output_dir: str = "../docs/images"
    image_format: str = "png"
    enable_show: bool = False
    default_direction: str = "TB"
    font_size: str = "40"
    node_sep: str = "1.5"
    rank_sep: str = "2.0"
    pad: str = "1.0"
    splines: str = "ortho"
    overlap: str = "false"
    compound: str = "true"
    enable_performance_monitoring: bool = True
    timeout_seconds: int = 300
    
    @classmethod
    def from_env(cls) -> 'DiagramConfig':
        # 環境変数から設定を作成
        load_dotenv()
        
        return cls(
            output_dir=os.getenv("DIAGRAM_OUTPUT_DIR", "../docs/images"),
            image_format=os.getenv("DIAGRAM_FORMAT", "png"),
            enable_show=os.getenv("DIAGRAM_SHOW", "false").lower() == "true",
            default_direction=os.getenv("DIAGRAM_DIRECTION", "TB"),
            font_size=os.getenv("DIAGRAM_FONT_SIZE", "40"),
            enable_performance_monitoring=os.getenv("ENABLE_PERFORMANCE_MONITORING", "true").lower() == "true",
            timeout_seconds=int(os.getenv("DIAGRAM_TIMEOUT_SECONDS", "300"))
        )

@dataclass
class DiagramResult:
    # 図表生成結果データ
    success: bool
    diagram_name: str
    file_path: Optional[str] = None
    generation_time_seconds: float = 0.0
    error: Optional[str] = None


class EnhancedDiagramGenerator:
    # 改善された図表生成システムクラス
    
    def __init__(self, config: DiagramConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 依存関係チェック
        if not DIAGRAMS_AVAILABLE:
            raise RuntimeError("Diagrams library is not available. Please install: pip install diagrams")
        
        # 出力ディレクトリ作成
        try:
            os.makedirs(config.output_dir, exist_ok=True)
            self.logger.info(f"出力ディレクトリ準備完了: {config.output_dir}")
        except Exception as e:
            self.logger.error(f"出力ディレクトリ作成エラー: {e}")
            raise
        
        # 統計情報
        self.diagrams_generated = 0
        self.total_generation_time = 0.0
        self.generation_start_time = None
        
    def get_default_graph_attr(self, **overrides) -> Dict[str, str]:
        # デフォルトのグラフ属性を取得
        default_attr = {
            "fontsize": self.config.font_size,
            "nodesep": self.config.node_sep,
            "ranksep": self.config.rank_sep,
            "pad": self.config.pad,
            "splines": self.config.splines,
            "overlap": self.config.overlap,
            "compound": self.config.compound
        }
        default_attr.update(overrides)
        return default_attr
    
    def _generate_diagram_with_monitoring(self, diagram_func, diagram_name: str, **kwargs) -> DiagramResult:
        # パフォーマンス監視付きで図表を生成
        start_time = time.time()
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            self.logger.info(f"{diagram_name}の生成を開始...")
            result = diagram_func(**kwargs)
            generation_time = time.time() - start_time
            
            current_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            self.logger.info(f"{diagram_name}生成完了: {generation_time:.2f}秒")
            
            if self.config.enable_performance_monitoring:
                self.logger.info(f"メモリ使用量: {initial_memory:.1f}MB → {current_memory:.1f}MB")
            
            self.diagrams_generated += 1
            self.total_generation_time += generation_time
            
            return DiagramResult(
                success=True,
                diagram_name=diagram_name,
                file_path=result,
                generation_time_seconds=generation_time
            )
            
        except Exception as e:
            generation_time = time.time() - start_time
            self.logger.error(f"{diagram_name}生成エラー: {e}")
            
            return DiagramResult(
                success=False,
                diagram_name=diagram_name,
                generation_time_seconds=generation_time,
                error=str(e)
            )
    
    def generate_overall_architecture(self) -> str:
        # 全体アーキテクチャ図を生成
        filename = f"{self.config.output_dir}/overall_architecture_v2"
        
        with Diagram("野菜市場分析プラットフォーム - 全体アーキテクチャ", 
                     filename=filename, 
                     show=self.config.enable_show, 
                     direction=self.config.default_direction, 
                     graph_attr=self.get_default_graph_attr(
                         fontsize="45",
                         nodesep="1.5",
                         ranksep="2.0"
                     )):
            
            # データソース層
            with Cluster("Data Sources", graph_attr={"style": "rounded", "bgcolor": "lightblue"}):
                market_web = Internet("Market Data\nWebsite (Excel)")
                jma_api = Internet("JMA Weather\nAPI (JSON)")
            
            # データ収集・処理層
            with Cluster("Data Collection & Processing", graph_attr={"style": "rounded", "bgcolor": "lightgreen"}):
                vege_collector = Python("get_vege_data.py")
                weather_collector = Python("get_weather_data.py")
                
            # Google Cloud Platform
            with Cluster("Google Cloud Platform", graph_attr={"style": "rounded", "bgcolor": "lightyellow"}):
                # ストレージレイヤー
                with Cluster("Storage Layer"):
                    gcs_raw = GCS("Raw Data\n(CSV Files)")
                
                # BigQuery データウェアハウス
                with Cluster("BigQuery Data Warehouse"):
                    with Cluster("RAW Layer"):
                        bq_raw_market = Bigquery("tokyo_market")
                        bq_raw_weather = Bigquery("weather_hourly")
                        bq_raw_predictions = Bigquery("ml_price_pred")
                    
                    with Cluster("STG Layer (dbt)"):
                        bq_stg_market = Bigquery("stg_market_raw")
                        bq_stg_weather = Bigquery("stg_weather_observation")
                        bq_stg_predictions = Bigquery("stg_price_pred")
                        
                    with Cluster("MART Layer (dbt)"):
                        bq_dims = Bigquery("Dimensions\n(5 tables)")
                        bq_facts = Bigquery("Facts\n(3 tables)")
                        bq_marts = Bigquery("Analysis Marts\n(3 tables)")
                
                # dbt変換エンジン
                dbt_runner = Dataflow("dbt Transformations")
            
            # 分析・機械学習層
            with Cluster("Analytics & ML", graph_attr={"style": "rounded", "bgcolor": "lightcoral"}):
                ml_models = AIPlatform("ML Models\n(Prophet/LSTM/ARIMA\n/Random Forest/Gradient Boosting)")
                batch_ml = ComputeEngine("ML Batch Processing\n(ml_batch.py)")
                model_cache = GCS("Model Storage\n(Trained Models)")
                
            # 可視化・レポート層
            with Cluster("Visualization & Reporting", graph_attr={"style": "rounded", "bgcolor": "lightpink"}):
                slack_notifications = Server("Slack通知\n(安価野菜予測)")
                cost_monitoring = Server("コスト監視\n(cost_monitor.py)")
                users = Users("Business Users")
                
            # データフロー（エッジラベル付き）
            market_web >> Edge(label="Monthly Excel\nDownload", style="bold", color="blue") >> vege_collector
            jma_api >> Edge(label="Hourly Weather\nData API", style="bold", color="blue") >> weather_collector
            
            vege_collector >> Edge(label="CSV Upload", style="dashed", color="green") >> gcs_raw
            weather_collector >> Edge(label="CSV Upload", style="dashed", color="green") >> gcs_raw
            
            gcs_raw >> Edge(label="Batch Load", style="solid", color="orange") >> bq_raw_market
            gcs_raw >> Edge(label="Batch Load", style="solid", color="orange") >> bq_raw_weather
            
            bq_raw_market >> Edge(label="dbt run", style="bold", color="purple") >> dbt_runner
            bq_raw_weather >> Edge(label="dbt run", style="bold", color="purple") >> dbt_runner
            
            dbt_runner >> Edge(label="Transform", style="solid", color="red") >> bq_stg_market
            dbt_runner >> Edge(label="Transform", style="solid", color="red") >> bq_stg_weather
            dbt_runner >> Edge(label="Transform", style="solid", color="red") >> bq_stg_predictions
            bq_stg_market >> Edge(label="Aggregate", style="dotted", color="darkgreen") >> bq_dims
            bq_stg_market >> Edge(label="Aggregate", style="dotted", color="darkgreen") >> bq_facts
            bq_stg_weather >> Edge(label="Aggregate", style="dotted", color="darkgreen") >> bq_marts
            
            bq_marts >> Edge(label="Training Data", style="bold", color="darkred") >> batch_ml
            batch_ml >> Edge(label="ML Processing", style="bold", color="darkred") >> ml_models
            ml_models >> Edge(label="Predictions", style="dashed", color="darkblue") >> bq_raw_predictions
            ml_models >> Edge(label="Store Models", style="dotted", color="gray") >> model_cache
            
            bq_marts >> Edge(label="Prediction Data", style="solid", color="darkviolet") >> slack_notifications
            cost_monitoring >> Edge(label="Monitor", style="dashed", color="red") >> bq_raw_market
            slack_notifications >> Edge(label="Alerts", style="bold", color="black") >> users
        
        return f"{filename}.{self.config.image_format}"
    
    def generate_data_pipeline_architecture(self) -> str:
        # データパイプライン詳細図を生成
        filename = f"{self.config.output_dir}/data_pipeline_architecture_v2"
        
        with Diagram("データパイプライン詳細アーキテクチャ", 
                     filename=filename, 
                     show=self.config.enable_show, 
                     direction=self.config.default_direction, 
                     graph_attr=self.get_default_graph_attr(
                         nodesep="2.5",
                         ranksep="3.5",
                         pad="2.0",
                         concentrate="true"
                     )):
            
            # 第1層: データソース
            with Cluster("🌐 External Data Sources", graph_attr={"style": "rounded", "bgcolor": "lightblue", "margin": "20"}):
                market_site = Internet("Market Data\nWebsite")
                jma_api = Internet("JMA Weather\nAPI")
                
            # 第2層: データ収集
            with Cluster("📥 Data Collection Layer", graph_attr={"style": "rounded", "bgcolor": "lightgreen", "margin": "20"}):
                market_collector = Python("Market Data\nCollector")
                weather_collector = Python("Weather Data\nCollector")
            
            # 第3層: データ処理・変換
            with Cluster("Data Processing Layer", graph_attr={"style": "rounded", "bgcolor": "lightyellow", "margin": "20"}):
                market_processor = Python("Market ETL\nProcessor")
                weather_processor = Python("Weather ETL\nProcessor")
                
            # 第4層: クラウドストレージ
            with Cluster("Cloud Storage Layer", graph_attr={"style": "rounded", "bgcolor": "lightcyan", "margin": "20"}):
                gcs_storage = GCS("Raw Data Storage\n(CSV Files)")
                
            # 第5層: BigQuery RAW層
            with Cluster("BigQuery RAW Layer", graph_attr={"style": "rounded", "bgcolor": "wheat", "margin": "20"}):
                raw_market_table = Bigquery("tokyo_market\n(Raw Data)")
                raw_weather_table = Bigquery("weather_hourly\n(Raw Data)")
                raw_predictions_table = Bigquery("ml_price_pred\n(Predictions)")
            
            # 第6層: dbt変換エンジン
            dbt_engine = Dataflow("dbt Transformation\nEngine")
                
            # 第7層: BigQuery変換済み層
            with Cluster("BigQuery Transformed Layers", graph_attr={"style": "rounded", "bgcolor": "lightpink", "margin": "20"}):
                with Cluster("STG Layer", graph_attr={"style": "dotted", "bgcolor": "mistyrose"}):
                    stg_market_table = Bigquery("stg_market_raw")
                    stg_weather_table = Bigquery("stg_weather_observation")
                    stg_predictions_table = Bigquery("stg_price_pred")
                    
                with Cluster("MART Layer", graph_attr={"style": "dotted", "bgcolor": "lavenderblush"}):
                    mart_dims = Bigquery("Dimensions\n(5 tables)")
                    mart_facts = Bigquery("Facts\n(3 tables)")
                    mart_analysis = Bigquery("Analysis Marts\n(3 tables)")
            
            # データフロー（階層別・色分け）
            market_site >> Edge(label="Excel Download", style="bold", color="blue") >> market_collector
            jma_api >> Edge(label="API Call", style="bold", color="blue") >> weather_collector
            
            market_collector >> Edge(label="Parse & Validate", style="solid", color="green") >> market_processor
            weather_collector >> Edge(label="Normalize & Clean", style="solid", color="green") >> weather_processor
            
            market_processor >> Edge(label="CSV Upload", style="dashed", color="orange") >> gcs_storage
            weather_processor >> Edge(label="CSV Upload", style="dashed", color="orange") >> gcs_storage
            
            gcs_storage >> Edge(label="Batch Load", style="solid", color="purple") >> raw_market_table
            gcs_storage >> Edge(label="Batch Load", style="solid", color="purple") >> raw_weather_table
            
            raw_market_table >> Edge(label="dbt Source", style="bold", color="red") >> dbt_engine
            raw_weather_table >> Edge(label="dbt Source", style="bold", color="red") >> dbt_engine
            
            dbt_engine >> Edge(label="STG Transform", style="dotted", color="darkgreen") >> stg_market_table
            dbt_engine >> Edge(label="STG Transform", style="dotted", color="darkgreen") >> stg_weather_table
            dbt_engine >> Edge(label="MART Build", style="solid", color="darkred") >> mart_dims
            dbt_engine >> Edge(label="MART Build", style="solid", color="darkred") >> mart_facts
            dbt_engine >> Edge(label="MART Build", style="solid", color="darkred") >> mart_analysis
        
        return f"{filename}.{self.config.image_format}"
    
    def generate_ml_architecture(self) -> str:
        # 機械学習・予測システム詳細図を生成
        filename = f"{self.config.output_dir}/ml_architecture_v2"
        
        with Diagram("機械学習バッチ予測システム", 
                     filename=filename, 
                     show=self.config.enable_show, 
                     direction=self.config.default_direction, 
                     graph_attr=self.get_default_graph_attr(
                         fontsize="45",
                         nodesep="2.0",
                         ranksep="2.8",
                         pad="1.5"
                     )):
            
            # データソース層
            with Cluster("BigQuery データマート"):
                price_weather_mart = Bigquery("mart_price_weather\n価格・気象統合データ")
                seasonal_mart = Bigquery("mart_seasonal_analysis\n季節性分析データ")
            
            # 機械学習パイプライン
            with Cluster("機械学習パイプライン"):
                # データ前処理
                data_processor = ComputeEngine("特徴量エンジニアリング\n& データ前処理")
                
                # モデル群
                with Cluster("時系列予測 & 機械学習モデル"):
                    prophet_model = AIPlatform("Prophet\n(時系列)")
                    lstm_model = AIPlatform("LSTM\n(深層学習)")
                    arima_model = AIPlatform("ARIMA\n(統計的)")
                    rf_model = AIPlatform("Random Forest\n(アンサンブル)")
                    gb_model = AIPlatform("Gradient Boosting\n(勾配ブースティング)")
                    lr_model = AIPlatform("Linear Regression\n(ベースライン)")
                    
                # モデル評価・選択
                model_evaluator = AIPlatform("モデル評価\n& 選択")
                hyperparameter_tuner = Automl("ハイパーパラメータ\nチューニング")
            
            # 予測・通知サービス層
            with Cluster("予測・通知サービス"):
                batch_processor = ComputeEngine("MLバッチ処理\n(ml_batch.py)")
                model_store = GCS("モデル保存\n(学習済みモデル)")
                slack_service = Server("Slack通知\n(slack_notif.py)")
                
            # 利用者・システム層
            with Cluster("利用者・システム"):
                business_users = Users("ビジネスユーザー")
                data_analysts = Users("データアナリスト")
                
            # 運用・監視層
            with Cluster("運用・監視"):
                cost_monitor = Server("コスト監視\n(cost_monitor.py)")
                github_actions = Server("GitHub Actions\n(月次パイプライン)")
                
            # データフロー（エッジラベル付き）
            price_weather_mart >> Edge(label="学習データ") >> data_processor
            seasonal_mart >> Edge(label="特徴量データ") >> data_processor
            
            data_processor >> Edge(label="前処理済み特徴量") >> prophet_model
            data_processor >> Edge(label="前処理済み特徴量") >> lstm_model
            data_processor >> Edge(label="前処理済み特徴量") >> arima_model
            data_processor >> Edge(label="前処理済み特徴量") >> rf_model
            data_processor >> Edge(label="前処理済み特徴量") >> gb_model
            data_processor >> Edge(label="前処理済み特徴量") >> lr_model
            
            prophet_model >> Edge(label="予測結果") >> model_evaluator
            lstm_model >> Edge(label="予測結果") >> model_evaluator
            arima_model >> Edge(label="予測結果") >> model_evaluator
            rf_model >> Edge(label="予測結果") >> model_evaluator
            gb_model >> Edge(label="予測結果") >> model_evaluator
            lr_model >> Edge(label="予測結果") >> model_evaluator
            
            model_evaluator >> Edge(label="最適モデル") >> hyperparameter_tuner
            hyperparameter_tuner >> Edge(label="調整済みモデル") >> model_store
            
            github_actions >> Edge(label="月次実行") >> batch_processor
            batch_processor >> Edge(label="モデル読み込み") >> model_store
            batch_processor >> Edge(label="予測実行") >> slack_service
            
            slack_service >> Edge(label="通知") >> business_users
            data_analysts >> Edge(label="分析クエリ") >> price_weather_mart
            
            cost_monitor >> Edge(label="使用量監視") >> price_weather_mart
        
        return f"{filename}.{self.config.image_format}"
    
    def generate_all_diagrams(self) -> List[DiagramResult]:
        # 全ての図表を生成
        self.logger.info("=== 包括的図表生成を開始 ===")
        self.generation_start_time = datetime.now()
        
        # メモリ監視
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        results = []
        
        # 図表生成関数のマッピング
        diagram_generators = {
            "Overall Architecture": self.generate_overall_architecture,
            "Data Pipeline Architecture": self.generate_data_pipeline_architecture,
            "ML Architecture": self.generate_ml_architecture,
        }
        
        for diagram_name, generator_func in diagram_generators.items():
            result = self._generate_diagram_with_monitoring(
                generator_func, diagram_name
            )
            results.append(result)
            
            if not result.success:
                self.logger.warning(f"{diagram_name}の生成に失敗しましたが、処理を継続します")
        
        # 結果サマリー
        end_time = datetime.now()
        duration = end_time - self.generation_start_time
        current_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        successful_diagrams = [r for r in results if r.success]
        failed_diagrams = [r for r in results if not r.success]
        
        self.logger.info(f"=== 図表生成完了 ===")
        self.logger.info(f"成功: {len(successful_diagrams)}/{len(results)}図表")
        self.logger.info(f"総処理時間: {duration.total_seconds():.2f}秒")
        self.logger.info(f"平均生成時間: {self.total_generation_time/len(results):.2f}秒/図表")
        
        if self.config.enable_performance_monitoring:
            self.logger.info(f"メモリ使用量: {initial_memory:.1f}MB → {current_memory:.1f}MB")
        
        if failed_diagrams:
            self.logger.warning(f"失敗した図表: {[d.diagram_name for d in failed_diagrams]}")
        
        return results


def main():
    # メイン実行関数
    parser = argparse.ArgumentParser(description="野菜市場分析プラットフォーム図表生成システム")
    
    parser.add_argument("--output-dir", help="出力ディレクトリ")
    parser.add_argument("--show", action="store_true", help="生成後に図表を表示")
    parser.add_argument("--format", choices=["png", "jpg", "svg", "pdf"], default="png", help="出力形式")
    parser.add_argument("--disable-monitoring", action="store_true", help="パフォーマンス監視を無効化")
    parser.add_argument("--timeout", type=int, help="タイムアウト秒数")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("野菜市場分析プラットフォーム図表生成システム（高性能版）")
    print("=" * 80)
    
    try:
        # 設定読み込み
        config = DiagramConfig.from_env()
        
        # コマンドライン引数で上書き
        if args.output_dir:
            config.output_dir = args.output_dir
        if args.show:
            config.enable_show = True
        if args.format:
            config.image_format = args.format
        if args.disable_monitoring:
            config.enable_performance_monitoring = False
        if args.timeout:
            config.timeout_seconds = args.timeout
        
        # ジェネレーター初期化
        generator = EnhancedDiagramGenerator(config)
        
        print(f"実行設定:")
        print(f"  出力ディレクトリ: {config.output_dir}")
        print(f"  出力形式: {config.image_format}")
        print(f"  パフォーマンス監視: {'ON' if config.enable_performance_monitoring else 'OFF'}")
        print(f"  タイムアウト: {config.timeout_seconds}秒")
        print()
        
        # 図表生成実行
        results = generator.generate_all_diagrams()
        
        # 結果表示
        successful_diagrams = [r for r in results if r.success]
        failed_diagrams = [r for r in results if not r.success]
        
        if successful_diagrams:
            print("[OK] 生成成功:")
            for result in successful_diagrams:
                print(f"  - {result.diagram_name}: {result.file_path}")
        
        if failed_diagrams:
            print("[ERROR] 生成失敗:")
            for result in failed_diagrams:
                print(f"  - {result.diagram_name}: {result.error}")
        
        if len(successful_diagrams) == len(results):
            print(f"\n[SUCCESS] 全ての図表が正常に生成されました！")
            return 0
        elif successful_diagrams:
            print(f"\n[WARNING] 部分的成功: {len(successful_diagrams)}/{len(results)}図表")
            return 1
        else:
            print(f"\n[ERROR] 全ての図表生成に失敗しました")
            return 1
            
    except Exception as e:
        logger.error(f"図表生成中にエラーが発生しました: {e}")
        print(f"[ERROR] エラー: {e}")
        print("\n[NOTE] 注意: Graphvizがシステムにインストールされていない可能性があります。")
        print("[INFO] Graphvizインストール: https://graphviz.org/download/")
        print("[INFO] pip install diagrams graphviz")
        return 1


if __name__ == "__main__":
    exit(main())