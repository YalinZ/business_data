"""
02_Risk_Growth_Integrity - 企业级离线特征库每日定时更新流水线
文件定位: src/batch/feature_store_update.py
功能说明: 
  1. 模拟接入上游图拓扑与行为基础特征 (收货地址相似度、IP共用数、裂变网络深度)
  2. 基于风控业务阈值规则融合计算综合历史风险分 (history_risk_score)
  3. 覆写输出至 data/features/offline_risk_features.parquet，模拟特征库 T+0 / T-1 回填
"""

import os
import sys

# ==============================================================================
# 1. Windows 环境 "Anti-Gravity" 底层环境防御配置
# ==============================================================================
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
from pyspark.sql.functions import col, round


def init_spark_session() -> SparkSession:
    """初始化具备 Windows 强兼容性的 SparkSession"""
    spark = SparkSession.builder \
        .appName("RiskGrowthIntegrity_FeatureStoreUpdate") \
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


def load_upstream_features(spark: SparkSession):
    """
    模拟读取上游特征流水线交付的多维基础特征表
    采用纯 JVM 声明式 SQL 构造，100% 避开 Windows 下 Python Worker 序列化通信缺陷
    """
    df_upstream = spark.sql("""
        SELECT 'd123' AS device_id, 0.95 AS address_similarity, 4.0 AS ip_shared_count, 6.0 AS network_depth UNION ALL
        SELECT 'd456', 0.10, 1.0, 1.0 UNION ALL
        SELECT 'd789', 0.88, 5.0, 8.0 UNION ALL
        SELECT 'dev_gang_01', 0.92, 6.0, 9.0 UNION ALL
        SELECT 'dev_gang_02', 0.85, 4.0, 7.0 UNION ALL
        SELECT 'dev_farm_01', 0.98, 8.0, 10.0 UNION ALL
        SELECT 'dev_farm_02', 0.91, 7.0, 8.0 UNION ALL
        SELECT 'dev_norm_01', 0.05, 1.0, 1.0 UNION ALL
        SELECT 'dev_norm_02', 0.12, 1.0, 2.0 UNION ALL
        SELECT 'dev_norm_03', 0.00, 1.0, 1.0
    """)
    return df_upstream


def main():
    print("=" * 70)
    print("🚀 启动离线特征库定时更新流水线 (Feature Store Update Batch)")
    print("=" * 70)

    # 动态定位项目根目录与特征持久化路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    output_parquet_path = os.path.join(project_root, "data", "features", "offline_risk_features.parquet")

    # 1. 初始化 Spark
    spark = init_spark_session()
    print(f"✅ SparkSession 启动成功 (Version: {spark.version})")

    try:
        # 2. 模拟读取上游特征数据
        df_upstream = load_upstream_features(spark)
        print("✅ 上游特征数据接入完成:")
        df_upstream.show(5, truncate=False)

        # 3. 离线规则融合与打分计算
        # 打分公式: (address_similarity * 10) + (ip_shared_count * 15) + (network_depth * 5)
        fused_df = df_upstream.withColumn(
            "history_risk_score",
            round(
                (col("address_similarity") * 10.0) +
                (col("ip_shared_count") * 15.0) +
                (col("network_depth") * 5.0),
                2
            )
        )

        # 4. 提取下游流处理对齐字段: feature_device_id 与 history_risk_score
        feature_store_df = fused_df.select(
            col("device_id").alias("feature_device_id"),
            col("history_risk_score")
        )

        print("\n📊 最终离线特征大表预览 (Top 10):")
        feature_store_df.show(10, truncate=False)

        # 5. 持久化覆写落盘至本地特征目录 (Parquet)
        os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)
        feature_store_df.write \
            .mode("overwrite") \
            .parquet(output_parquet_path)
            
        print(f"💾 特征库已成功覆写持久化至: {output_parquet_path}")

    finally:
        spark.stop()
        print("🏁 SparkSession 安全释放完毕")
        print("=" * 70)


if __name__ == "__main__":
    main()
