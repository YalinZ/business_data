"""
02_Risk_Growth_Integrity - 离线图拓扑特征批处理计算流水线
文件定位: src/batch/graph_features.py
功能说明: 
  1. 基于 Spark SQL 提取设备共用与聚集基础统计特征
  2. 基于 GraphFrames 提取网络连通子图 (Connected Components) 与节点核心度 (PageRank)
  3. 计算各设备的历史综合风险分 (history_risk_score)，并以 Parquet 格式持久化落地
"""

import os
import sys
import subprocess

# ==============================================================================
# 1. Windows 环境 "Anti-Gravity" 底层防御配置
# ==============================================================================
# 自动检测与适配 HADOOP_HOME，规避 winutils / native-hadoop 丢失异常
hadoop_home = os.environ.get('HADOOP_HOME')
if not hadoop_home or not os.path.exists(hadoop_home):
    hadoop_home = r'C:\hadoop'
os.environ['HADOOP_HOME'] = hadoop_home

hadoop_bin = os.path.join(hadoop_home, 'bin')
if hadoop_bin not in os.environ.get('PATH', ''):
    os.environ['PATH'] = hadoop_bin + ';' + os.environ.get('PATH', '')

# 锁死 Python 解释器路径，杜绝 Windows 下多版本 Python 导致的 Worker 崩溃
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 动态挂载 GraphFrames 0.8.3 依赖包 (针对 Spark 3.5 / Scala 2.12)
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 pyspark-shell'

# 检查并自动补全 graphframes Python 客户端绑定
try:
    from graphframes import GraphFrame
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "graphframes", "-q"])
    from graphframes import GraphFrame

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, round, lit


def init_spark_session(checkpoint_dir: str) -> SparkSession:
    """初始化具备极端稳定性、强制禁用 IPv6、隔离 Broadcast 的生产级 SparkSession"""
    spark = SparkSession.builder \
        .appName("RiskGrowthIntegrity_OfflineGraphFeaturesBatch") \
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
    
    # 设置 GraphFrames 连通图算法强制要求的 Checkpoint 目录
    os.makedirs(checkpoint_dir, exist_ok=True)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    
    return spark


def load_mock_raw_tables(spark: SparkSession):
    """
    数据接入: 模拟从 data/raw/ 读取的 T-1 业务底表
    采用声明式 Spark SQL 构造，100% 避免 Windows 下 Python Worker 序列化通信缺陷
    """
    # 用户-设备-IP 行为日志表 (包含黑产农场群控聚集模式与正常用户对照组)
    df_user_device = spark.sql("""
        SELECT 'u101' AS user_id, 'dev_gang_01' AS device_id, '10.0.0.1' AS ip UNION ALL
        SELECT 'u102', 'dev_gang_01', '10.0.0.1' UNION ALL
        SELECT 'u103', 'dev_gang_01', '10.0.0.2' UNION ALL
        SELECT 'u104', 'dev_gang_02', '10.0.0.2' UNION ALL
        SELECT 'u105', 'dev_gang_02', '10.0.0.2' UNION ALL
        SELECT 'u106', 'dev_gang_02', '10.0.0.3' UNION ALL
        SELECT 'u201', 'dev_farm_01', '192.168.1.10' UNION ALL
        SELECT 'u202', 'dev_farm_01', '192.168.1.10' UNION ALL
        SELECT 'u203', 'dev_farm_02', '192.168.1.10' UNION ALL
        SELECT 'u204', 'dev_farm_02', '192.168.1.11' UNION ALL
        SELECT 'u301', 'dev_norm_01', '172.16.0.1' UNION ALL
        SELECT 'u302', 'dev_norm_02', '172.16.0.2' UNION ALL
        SELECT 'u303', 'dev_norm_03', '172.16.0.3'
    """)

    # 裂变拉新邀请关系表 (邀请者 inviter_id -> 被邀请者 invitee_id)
    df_invites = spark.sql("""
        SELECT 'u101' AS inviter_id, 'u102' AS invitee_id UNION ALL
        SELECT 'u101', 'u103' UNION ALL
        SELECT 'u102', 'u104' UNION ALL
        SELECT 'u103', 'u105' UNION ALL
        SELECT 'u104', 'u106' UNION ALL
        SELECT 'u201', 'u202' UNION ALL
        SELECT 'u202', 'u203' UNION ALL
        SELECT 'u203', 'u204' UNION ALL
        SELECT 'u204', 'u201' UNION ALL
        SELECT 'u301', 'u302'
    """)

    return df_user_device, df_invites


def compute_device_sql_features(spark: SparkSession, df_user_device):
    """基础特征计算 (Spark SQL): 统计每个设备的共用用户数 (device_shared_users) 及关联 IP 数"""
    df_user_device.createOrReplaceTempView("user_device_raw")
    sql_features = spark.sql("""
        WITH dev_stats AS (
            SELECT 
                device_id,
                COUNT(DISTINCT user_id) AS device_shared_users,
                COUNT(DISTINCT ip) AS device_ip_count
            FROM user_device_raw
            GROUP BY device_id
        )
        SELECT 
            u.user_id,
            u.device_id,
            d.device_shared_users,
            d.device_ip_count
        FROM user_device_raw u
        LEFT JOIN dev_stats d ON u.device_id = d.device_id
    """)
    return sql_features


def compute_graph_topology_features(spark: SparkSession, df_user_device, df_invites):
    """
    图拓扑特征计算 (GraphFrames):
      1. 连通子图 (Connected Components) -> 提取网络团伙规模 network_size
      2. PageRank (5次迭代) -> 提取核心头目/关键跳转权重 pagerank
    """
    df_user_device.createOrReplaceTempView("v_user_device")
    df_invites.createOrReplaceTempView("v_invites")

    # 构造唯一顶点集合
    vertices = spark.sql("""
        SELECT DISTINCT user_id AS id FROM (
            SELECT user_id FROM v_user_device
            UNION ALL
            SELECT inviter_id AS user_id FROM v_invites
            UNION ALL
            SELECT invitee_id AS user_id FROM v_invites
        )
    """)

    # 构造边集合
    edges = df_invites.selectExpr("inviter_id AS src", "invitee_id AS dst")

    # 构建 GraphFrame
    g = GraphFrame(vertices, edges)

    # 1. 连通图团伙挖掘
    cc = g.connectedComponents()
    component_sizes = cc.groupBy("component").agg(count("id").alias("network_size"))
    cc_features = cc.join(component_sizes, on="component", how="left") \
        .select(col("id").alias("user_id"), col("network_size"))

    # 2. PageRank 核心度挖掘 (5 次迭代)
    pr = g.pageRank(resetProbability=0.15, maxIter=5)
    pr_features = pr.vertices.select(col("id").alias("user_id"), col("pagerank"))

    # 图特征组合
    graph_features = cc_features.join(pr_features, on="user_id", how="inner")
    return graph_features


def main():
    print("=" * 70)
    print("🚀 启动离线图特征批处理流水线 (Risk Graph Features Batch Pipeline)")
    print("=" * 70)

    # 动态定位项目根目录 (无论在哪个工作目录下启动脚本均能准确定位)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    checkpoint_dir = os.path.join(project_root, "checkpoints_graph")
    output_parquet_path = os.path.join(project_root, "data", "features", "offline_risk_features.parquet")

    # 1. 初始化 Spark 会话
    spark = init_spark_session(checkpoint_dir)
    print(f"✅ SparkSession 启动成功 (Version: {spark.version})")
    print(f"📁 Checkpoint 目录就绪: {checkpoint_dir}")

    try:
        # 2. 模拟接入 T-1 业务底表
        df_user_device, df_invites = load_mock_raw_tables(spark)
        print("✅ 业务底表接入完成")

        # 3. 计算基础 SQL 统计特征
        sql_features_df = compute_device_sql_features(spark, df_user_device)
        print("✅ SQL 基础统计特征计算完成")

        # 4. 计算图拓扑特征 (GraphFrames)
        graph_features_df = compute_graph_topology_features(spark, df_user_device, df_invites)
        print("✅ 图拓扑特征 (ConnectedComponents + PageRank) 计算完成")

        # 5. 特征组装与综合评分计算
        # 以 user_id 和 device_id 为基准关联
        joined_df = sql_features_df.join(
            graph_features_df,
            on="user_id",
            how="left"
        ).fillna({
            "network_size": 1,
            "pagerank": 0.15
        })

        # 综合评分逻辑: (device_shared_users * 15) + (network_size * 5) + (pagerank * 10)
        user_device_scored = joined_df.withColumn(
            "calc_score",
            (col("device_shared_users") * 15.0) + (col("network_size") * 5.0) + (col("pagerank") * 10.0)
        )

        # 仅保留 feature_device_id 和 history_risk_score，按设备汇总最高风险值
        final_device_features = user_device_scored.groupBy(
            col("device_id").alias("feature_device_id")
        ).agg(
            round(spark_max("calc_score"), 2).alias("history_risk_score")
        ).select("feature_device_id", "history_risk_score")

        print("\n📊 离线设备风险特征库样例 (Top 10):")
        final_device_features.show(10, truncate=False)

        # 6. 持久化至 Parquet (overwrite 模式)
        os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)
        final_device_features.write \
            .mode("overwrite") \
            .parquet(output_parquet_path)

        print(f"💾 特征库已成功持久化至: {output_parquet_path}")

    finally:
        spark.stop()
        print("🏁 SparkSession 安全释放完毕")
        print("=" * 70)


if __name__ == "__main__":
    main()
