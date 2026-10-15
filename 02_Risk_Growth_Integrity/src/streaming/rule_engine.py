"""
02_Risk_Growth_Integrity - 实时风控规则与异常检测模型联动引擎
文件定位: src/streaming/rule_engine.py
功能说明: 
  1. 加载持久化的 PCA PipelineModel (VectorAssembler -> StandardScaler -> PCA)
  2. 对实时微批流数据进行 Transform，提取纯 Catalyst 矩阵投影的重构误差作为 PCA 异常分
  3. 执行多源风控分融合 (离线图特征*0.3 + 实时聚合规则分*0.3 + PCA模型异常分*0.4)
  4. 实施阶梯拦截治理策略 (>=85 BLOCK, >=60 CAPTCHA, <60 PASS)
"""

import os
import sys
import numpy as np

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, sqrt, round, least, greatest, when


def get_default_model_path() -> str:
    """获取默认持久化模型路径"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    return os.path.join(project_root, "data", "models", "pca_pipeline_model")


def load_pca_pipeline_model(model_path: str = None) -> PipelineModel:
    """
    加载生产级 PCA Pipeline 模型
    包含阶段: VectorAssembler -> StandardScalerModel -> PCAModel
    """
    if not model_path:
        model_path = get_default_model_path()
        if not os.path.exists(model_path):
            model_path = "./data/models/pca_pipeline_model"

    if not os.path.exists(model_path):
        raise FileNotFoundError(f"❌ 未找到已保存的 PCA 模型目录: {model_path}，请先运行 src/models/pca_reconstruction.py")

    print(f"📦 [RuleEngine] 成功加载 PCA 模型库: {model_path}")
    pipeline_model = PipelineModel.load(model_path)
    return pipeline_model


def apply_pca_anomaly_scoring(df: DataFrame, pipeline_model: PipelineModel) -> DataFrame:
    """
    对实时流数据执行 PCA Pipeline Transform，并使用纯 Catalyst 算子计算重构误差异常分
    (100% 避开 Windows 下 Python Worker 序列化通信缺陷)
    
    输入特征对齐: order_freq, total_amount, refund_count, history_risk_score
    """
    # 1. 确保输入字段存在默认值，保证 Pipeline 转换稳定性
    aligned_df = df \
        .withColumn("order_freq", col("order_count").cast("double")) \
        .withColumn("total_amount", col("total_amount").cast("double")) \
        .withColumn("refund_count", col("refund_count").cast("double")) \
        .withColumn("history_risk_score", col("history_risk_score").cast("double"))

    # 2. 执行 Pipeline 前向变换 (VectorAssembler -> StandardScaler -> PCA)
    transformed_df = pipeline_model.transform(aligned_df)

    # 3. 提取 PCA 阶段的投影矩阵 V (num_features x k) 并计算重构矩阵 P = V @ V.T
    pca_stage = pipeline_model.stages[-1]
    V = pca_stage.pc.toArray()
    P = np.dot(V, V.T)
    num_features = len(P)

    # 4. 向量展开为数组并构建 Catalyst 重构误差表达式: ||x_scaled - x_scaled @ P||_2
    df_with_arr = transformed_df.withColumn("scaled_features_arr", vector_to_array(col("scaled_features")))

    recon_sq_error = lit(0.0)
    for i in range(num_features):
        x_hat_i = sum(col("scaled_features_arr")[j] * float(P[j, i]) for j in range(num_features))
        recon_sq_error = recon_sq_error + (col("scaled_features_arr")[i] - x_hat_i) ** 2

    # 重构误差按经验系数映射至 0 ~ 100 分
    pca_scored_df = df_with_arr.withColumn(
        "pca_anomaly_score",
        round(
            least(
                lit(100.0),
                greatest(lit(0.0), sqrt(recon_sq_error + 1e-12) * 35.0)
            ), 2
        )
    )

    return pca_scored_df


def apply_rule_and_decision(df: DataFrame) -> DataFrame:
    """
    多源风控分动态融合与阶梯拦截决策:
    1. 实时聚合规则分 (realtime_rule_score): 结合领券频率与退单率
    2. 融合总分: Final_Score = 离线图特征分*0.3 + 实时聚合规则分*0.3 + PCA模型异常分*0.4
    3. 决策阶梯: >=85 BLOCK, >=60 CAPTCHA, <60 PASS
    """
    # 1. 计算实时聚合异常规则分 (0 ~ 100)
    df_with_rule = df.withColumn(
        "realtime_rule_score",
        round(
            least(
                lit(100.0),
                (col("coupon_freq") * 10.0) +
                (col("refund_rate") * 100.0) +
                when(col("total_amount") > 3000.0, 25.0).otherwise(0.0)
            ), 2
        )
    )

    # 2. 融合最终风险分 (Final_Score)
    # Final_Score = 离线图特征分*0.3 + 实时聚合异常规则分*0.3 + PCA模型异常分*0.4
    df_with_final = df_with_rule.withColumn(
        "final_score",
        round(
            (col("history_risk_score") * 0.3) +
            (col("realtime_rule_score") * 0.3) +
            (col("pca_anomaly_score") * 0.4),
            2
        )
    )

    # 3. 阶梯拦截策略判定
    final_decision_df = df_with_final.withColumn(
        "decision",
        when(col("final_score") >= 85.0, "BLOCK (高危直接拦截)")
        .when((col("final_score") >= 60.0) & (col("final_score") < 85.0), "CAPTCHA (中危校验人机验证码)")
        .otherwise("PASS (低危放行)")
    )

    return final_decision_df
