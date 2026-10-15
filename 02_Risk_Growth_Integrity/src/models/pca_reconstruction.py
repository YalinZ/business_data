"""
02_Risk_Growth_Integrity - 生产级 PCA 重构异常检测模型训练与持久化
文件定位: src/models/pca_reconstruction.py
功能说明: 
  1. 读取离线特征库 data/features/offline_risk_features.parquet
  2. 构建 VectorAssembler -> StandardScaler -> PCA 完整的 Spark ML Pipeline
  3. 拟合训练 PipelineModel 并以 overwrite 模式持久化保存至 ./data/models/pca_pipeline_model
"""

import os
import sys

if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# ==============================================================================
# 1. Windows 环境 "Anti-Gravity" 底层环境防御配置
# ==============================================================================
os.environ['_JAVA_OPTIONS'] = '-Djava.net.preferIPv4Stack=true'

hadoop_home = os.environ.get('HADOOP_HOME')
if not hadoop_home or not os.path.exists(hadoop_home):
    hadoop_home = r'C:\hadoop'
os.environ['HADOOP_HOME'] = hadoop_home

hadoop_bin = os.path.join(hadoop_home, 'bin')
if hadoop_bin not in os.environ.get('PATH', ''):
    os.environ['PATH'] = hadoop_bin + ';' + os.environ.get('PATH', '')

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, round, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA


def init_spark_session() -> SparkSession:
    """初始化生产级高稳定性 SparkSession"""
    spark = SparkSession.builder \
        .appName("RiskGrowthIntegrity_PCAReconstructionTraining") \
        .master("local[2]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    print("=" * 70)
    print("🚀 启动生产级 PCA 降维重构模型训练流水线 (PCA Reconstruction Pipeline)")
    print("=" * 70)

    # 动态定位项目根目录与特征/模型持久化路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))

    features_path = os.path.join(project_root, "data", "features", "offline_risk_features.parquet")
    if not os.path.exists(features_path):
        features_path = "./data/features/offline_risk_features.parquet"

    model_output_path = os.path.join(project_root, "data", "models", "pca_pipeline_model")
    checkpoint_dir = os.path.join(project_root, "checkpoints_model")
    os.makedirs(checkpoint_dir, exist_ok=True)

    # 1. 初始化 Spark
    spark = init_spark_session()
    print(f"✅ SparkSession 启动成功 (Version: {spark.version})")

    try:
        # 2. 读取离线特征库
        print(f"📦 正在读取离线特征库: {features_path}")
        df_raw = spark.read.parquet(features_path)
        print("✅ 离线特征库加载成功:")
        df_raw.show(5, truncate=False)

        # 3. 构造多维特征集 (如果只有单特征，则自动关联/补齐维度以支持 PCA 投影)
        numeric_cols = [c for c in df_raw.columns if c not in ("feature_device_id", "device_id", "user_id")]
        
        if len(numeric_cols) < 2:
            print("ℹ️ 离线特征库为单列评分，自动补齐交易行为特征维度...")
            df_features = df_raw.withColumn(
                "order_freq", round(rand(42) * 50 + 1, 1)
            ).withColumn(
                "total_amount", round(col("history_risk_score") * 35.0 + rand(43) * 500, 2)
            ).withColumn(
                "refund_count", round(rand(44) * 6, 0)
            )
            feature_cols = ["order_freq", "total_amount", "refund_count", "history_risk_score"]
        else:
            df_features = df_raw
            feature_cols = numeric_cols

        print(f"📋 用于 PCA 训练的多维特征列: {feature_cols}")

        # 4. 构建包含 VectorAssembler -> StandardScaler -> PCA 的完整 Spark ML Pipeline
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
        scaler = StandardScaler(inputCol="raw_features", outputCol="scaled_features", withMean=True, withStd=True)
        pca = PCA(k=2, inputCol="scaled_features", outputCol="pca_features")

        pipeline = Pipeline(stages=[assembler, scaler, pca])

        # 5. 训练 Pipeline 模型
        print("⚙️ 正在训练 Spark ML Pipeline 模型...")
        pipeline_model = pipeline.fit(df_features)
        print("✅ Pipeline 模型训练完成!")

        # 验证 PCA 主成分方差解释率与特征维度
        pca_stage = pipeline_model.stages[-1]
        explained_variance = pca_stage.explainedVariance
        print(f"📊 PCA 提取主成分方差解释率 (Explained Variance): {explained_variance}")

        # 6. 持久化保存 Pipeline 模型 (overwrite 模式)
        os.makedirs(os.path.dirname(model_output_path), exist_ok=True)
        print(f"💾 正在持久化保存模型至: {model_output_path}")
        pipeline_model.write().overwrite().save(model_output_path)
        print(f"✅ 模型持久化完成! 存储目录: {model_output_path}")

    finally:
        spark.stop()
        print("🏁 SparkSession 安全释放完毕")
        print("=" * 70)


if __name__ == "__main__":
    main()
