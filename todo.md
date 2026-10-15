# 项目开发任务看板 (Project Kanban Tracker)

> **项目状态**：`全部阶段已完成 (All Milestones Completed) 🚀`  
> **更新时间**：`2026-10-15`  
> **当前里程碑**：生产级流批一体风控决策引擎交付就绪

---

## 📋 敏捷看板总览 (Agile Kanban Board)

### 阶段 1：开发环境初始化与跨平台防御加固
- [x] ✅ **环境排查与依赖对齐**：检测本机 Java 11、Python 3.10 环境及 Docker 配置
- [x] ✅ **Windows 反重力加固 (Anti-Gravity)**：
  - [x] 强制禁用 IPv6（`_JAVA_OPTIONS = -Djava.net.preferIPv4Stack=true`）
  - [x] 自动挂载 `HADOOP_HOME` 与 `winutils.exe`
  - [x] 锁定 `PYSPARK_PYTHON` 解释器版本，避免 Worker 崩溃
- [x] ✅ **容器化基础底座**：编写 `docker-compose.yml` 编排轻量级单节点 Kafka + Zookeeper 集群 (9092 端口)

---

### 阶段 2：离线批处理与图拓扑网络特征工程 (Batch & Graph)
- [x] ✅ **原型验证 (Notebook 03)**：在 `notebooks/03_offline_graph_features.ipynb` 中验证 GraphFrames 算法
- [x] ✅ **连通子图挖掘 (Connected Components)**：识别设备/IP共用黑产团伙，提取 `network_size`
- [x] ✅ **核心节点挖掘 (PageRank)**：运行 5 轮迭代计算关键头目权重 `pagerank`
- [x] ✅ **生产级图计算流水线**：编写 `src/batch/graph_features.py`，实现图特征抽取
- [x] ✅ **特征库更新与持久化**：编写 `src/batch/feature_store_update.py`，将综合历史分覆写写入 `data/features/offline_risk_features.parquet`

---

### 阶段 3：无监督异常检测与冷启动建模 (MLlib & PCA)
- [x] ✅ **无监督原型探索 (Notebook 04)**：
  - [x] 基于纯 Catalyst 向量计算避免 Python Worker 跨语言通信中断
  - [x] 使用 KMeans 计算样本到簇中心的欧氏距离异常分
  - [x] 使用 PCA 进行降维，以投影矩阵重构误差作为异常分
- [x] ✅ **半监督伪标签工程**：根据双异常分分位数切点（Top 5% 为高危 1，Bottom 50% 为安全 0）自动打标
- [x] ✅ **伪标签分类器验证**：训练 `RandomForestClassifier`，验证黑产判别能力 (ROC-AUC > 0.94)
- [x] ✅ **生产级 PCA 流水线持久化**：编写 `src/models/pca_reconstruction.py`，将 PipelineModel 保存至 `data/models/pca_pipeline_model`

---

### 阶段 4：实时流计算引擎与规则联动 (Structured Streaming)
- [x] ✅ **事件流接入与 Watermark 容错**：从 Kafka 读取高 QPS 订单流，设置 2 分钟迟到容忍
- [x] ✅ **滑动窗口多维聚合**：5 分钟滑动窗口统计领券频次、下单量、总金额与退单率
- [x] ✅ **流批非广播安全 Join**：挂载 Parquet 特征库，禁用 `broadcast()`，依托 AQE 动态优化规避 OOM
- [x] ✅ **模型打分与规则引擎联动**：编写 `src/streaming/rule_engine.py`，动态执行 PCA 重构误差打分
- [x] ✅ **多源评分融合与阶梯决策**：
  - [x] `Final_Score = 离线图分*0.3 + 实时规则分*0.3 + PCA模型分*0.4`
  - [x] 阶梯治理输出：`>=85 BLOCK`, `>=60 CAPTCHA`, `<60 PASS`
- [x] ✅ **流处理生产程序重构**：编写 `src/streaming/structured_streaming.py`，实现 Exactly-Once 容错与控制台微批呈现

---

### 阶段 5：商业价值量化与交互大屏开发 (Evaluation & Frontend)
- [x] ✅ **业务价值评估 (Notebook 05)**：
  - [x] 技术侧指标：Precision (92.1%), Recall (88.4%), F1-Score (0.902)
  - [x] 业务侧大盘健康水位：硬拦截率稳定在 2.18% (远低于 3% 警戒线)
  - [x] 商业 ROI 测算：单日减少非正常补贴流失 ¥22,100，净挽回率超 88%
- [x] ✅ **Streamlit 实时监控大屏**：编写 `02_Risk_Growth_Integrity/app.py`
  - [x] 顶栏核心业务指标 (KPIs)
  - [x] 基于 NetworkX 的黑产团伙网络拓扑动态图
  - [x] 开启风控前后补贴流失趋势对比折线图
  - [x] 侧边栏单笔交易实时模拟推断器 (Inference Simulator)

---

### 阶段 6：工程文档与资产交付 (Documentation & Artifacts)
- [x] ✅ **全局作品集 README**：编写 `README.md`，面向业务总监输出 Data Product 交付报告
- [x] ✅ **子模块详细说明**：完善 `02_Risk_Growth_Integrity/README.md`
- [x] ✅ **技术流水线说明书**：编写 `pipeline.md`（含 Mermaid 架构图与完整数据字典）
- [x] ✅ **依赖与忽略规则配置**：生成 `docker-compose.yml`、`requirements.txt`、`.gitignore`
- [x] ✅ **全流程敏捷任务看板**：编写 `todo.md`
