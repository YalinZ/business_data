import os
import sys

# Windows 终端 UTF-8 编码防乱码保护
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

# 确保本模块与根路径均在 sys.path 中
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# ==============================================================================
# 1. Windows 环境 "Anti-Gravity" 底层环境防御配置 (生死线配置)
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
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, when, sum
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# 导入风控规则与模型联动引擎模块
from rule_engine import load_pca_pipeline_model, apply_pca_anomaly_scoring, apply_rule_and_decision

# 动态定位特征库与 Checkpoint 存储路径
features_parquet_path = os.path.join(project_root, "data", "features", "offline_risk_features.parquet")
if not os.path.exists(features_parquet_path):
    features_parquet_path = "./data/features/offline_risk_features.parquet"

checkpoint_dir = os.path.join(project_root, "checkpoint_dir", "risk_engine")
os.makedirs(checkpoint_dir, exist_ok=True)

# ==============================================================================
# 2. 初始化具备强隔离与低延时容错的 SparkSession
# ==============================================================================
spark = SparkSession.builder \
    .appName("RealTimeRiskAndGrowthEngine") \
    .config("spark.sql.shuffle.partitions", "5") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .config("spark.python.worker.setupTimeout", "300") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================================================================
# 3. 挂载真实离线图特征库 (Parquet 批量表，严禁 broadcast 内存隔离)
# ==============================================================================
print(f"📦 正在挂载离线特征库: {features_parquet_path}")
offline_df = spark.read.parquet(features_parquet_path)
print("✅ 离线特征库挂载成功，元数据结构:")
offline_df.printSchema()

# ==============================================================================
# 4. 加载生产级 PCA 降维重构流水线模型
# ==============================================================================
pca_pipeline_model = load_pca_pipeline_model()

# ==============================================================================
# 5. 定义 Kafka 输入事件流 Schema
# ==============================================================================
schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("action", StringType(), True),  # coupon(领券), order(下单), refund(退单)
    StructField("amount", DoubleType(), True),
    StructField("event_time", TimestampType(), True)
])

# ==============================================================================
# 6. 从 Kafka 读取实时数据流 (Exactly-Once 语义接入)
# ==============================================================================
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "realtime_risk_events") \
    .option("startingOffsets", "earliest") \
    .load()

# 解析 JSON 并设置 2 分钟 Watermark 容忍乱序迟到数据
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withWatermark("event_time", "2 minutes")

# ==============================================================================
# 7. 5分钟滑动窗口聚合计算 (1分钟滑动一次)
# ==============================================================================
windowed_aggs = parsed_stream \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("device_id")
    ) \
    .agg(
        sum(when(col("action") == "coupon", 1).otherwise(0)).alias("coupon_freq"),
        sum(when(col("action") == "order", 1).otherwise(0)).alias("order_count"),
        sum(when(col("action") == "order", col("amount")).otherwise(0.0)).alias("total_amount"),
        sum(when(col("action") == "refund", 1).otherwise(0)).alias("refund_count")
    ) \
    .withColumn(
        "refund_rate", 
        when(col("order_count") > 0, col("refund_count") / col("order_count")).otherwise(0.0)
    )

# ==============================================================================
# 8. 流批普通 Left Outer Join (严禁 broadcast，依托 AQE 动态优化)
# ==============================================================================
enriched_stream = windowed_aggs.join(
    offline_df, 
    windowed_aggs.device_id == offline_df.feature_device_id, 
    "left_outer"
).fillna({"history_risk_score": 0.0})

# ==============================================================================
# 9. 调用 rule_engine 模块进行 PCA 模型打分、规则融合与阶梯决策
# ==============================================================================
# 1. 提取 PCA 重构误差异常分 (pca_anomaly_score)
pca_scored_stream = apply_pca_anomaly_scoring(enriched_stream, pca_pipeline_model)

# 2. 融合打分与阶梯拦截判定
# 融合公式: Final_Score = 离线图特征分*0.3 + 实时聚合异常规则分*0.3 + PCA模型异常分*0.4
# 决策阶梯: >=85 BLOCK, >=60 CAPTCHA, <60 PASS
final_decision_stream = apply_rule_and_decision(pca_scored_stream)

output_stream = final_decision_stream.select(
    "window",
    "device_id",
    "history_risk_score",
    "realtime_rule_score",
    "pca_anomaly_score",
    "final_score",
    "decision"
)

# ==============================================================================
# 10. 输出到 Console 进行流式阶梯拦截呈现 (配置 Checkpoint 容错保障)
# ==============================================================================
print("🚀 实时风控规则与异常模型联动引擎已就绪，正在监听事件流进行分级管控...")
query = output_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()