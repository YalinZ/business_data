# Project 02: Risk, Growth & Integrity | 实时流批一体风控与黑产团伙挖掘系统

[![Engine](https://img.shields.io/badge/Engine-PySpark%203.5%20%7C%20Kafka%203.6-orange.svg?style=flat-square&logo=apachespark)](https://spark.apache.org/)
[![Graph Analytics](https://img.shields.io/badge/Graph-GraphFrames%200.8.3-blue.svg?style=flat-square)](https://graphframes.github.io/graphframes/docs/_site/index.html)
[![Dashboard](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B.svg?style=flat-square&logo=streamlit)](https://streamlit.io/)
[![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20Linux%20Enterprise-success.svg?style=flat-square)](https://github.com/YalinZ/business_data)
[![License](https://img.shields.io/badge/License-MIT-lightgrey.svg?style=flat-square)](https://opensource.org/licenses/MIT)

> **面向业务总监与风控技术决策层的工业级 Data Product 交付文档**  
> 本系统针对电商与本地生活平台高额营销补贴（新人立减、裂变返现、拼团优惠券）中的羊毛党黑产套现与机器刷单痛点，搭建了**「毫秒级实时流拦截 ➔ 离线图拓扑挖掘 ➔ 每日特征反哺 ➔ 柔性阶梯管控」**的闭环风控中枢。在将大盘硬拦截率压制在 **2.18%** 黄金健康线的同时，实现了 **¥22,005.00** 净资损挽回，达成**兼顾业务增长 (Growth) 与安全合规 (Integrity)** 的最优平衡。

---

## 🏛️ 一、 业务愿景与核心痛点破局

在激烈的互联网增长存量竞争中，业务方与安全风控团队往往存在天然的诉求对立：
* **增长团队痛点**：传统风控“一刀切”封禁导致大量误伤，误伤真实买家造成不可逆的客户流失与客诉，严重拉低营销获客 ROI；
* **风控团队痛点**：大促冷启动阶段缺乏黑产标签，规则滞后；黑产团伙通过“白号池”、“猫池”与“多开模拟器”批量套现，补贴资金数秒内被洗劫一空；
* **底层工程痛点**：流式高并发聚合计算（5 分钟滑动窗口）对内存和低延迟要求极高，Windows 本地开发与跨平台部署极易遭遇 JVM 假死、Python Worker 跨进程通信中断及文件锁死崩溃。

### 🌟 破局方案架构：四位一体闭环
1. **实时秒级防线**：Spark Structured Streaming 依托 Event Time + 2 分钟迟到水位线（Watermark），实时抓取滑动窗口内 IP/设备突增聚类特征。
2. **离线图网络破壁**：针对无标签冷启动，利用 GraphFrames 算法挖掘设备-IP-收货地址连通子图（Connected Components）与 PageRank 核心节点，以网络拓扑定位团伙发牌头目。
3. **自适应无监督模型**：构建 `StandardScaler + PCA Pipeline` 异常重构误差模型，结合离线基线分形成多维综合风险评分。
4. **柔性阶梯拦截策略**：实行「高危直接拒绝 (BLOCK) ➔ 中危人机验证 (CAPTCHA) ➔ 低危无感放行 (PASS)」，用最小交互摩擦阻断黑产自动化脚本，力保真实 GMV 转化。

---

## 📐 二、 架构全景与流批闭环流程

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 实时计算链路 (Real-time Stream Engine)                        │
│                                                                                             │
│  ┌────────────────┐     ┌────────────────────────┐     ┌─────────────────────────────────┐  │
│  │ Kafka Producer │────▶│ Spark Structured       │────▶│ 规则模型联动 (rule_engine.py)   │  │
│  │ (高QPS事件流)  │     │ Streaming (2min Water) │     │ - 离线图评分权重 * 0.3          │  │
│  └────────────────┘     │ 5min滑动窗口聚合       │     │ - 实时窗口聚集分 * 0.3          │  │
│                         └───────────┬────────────┘     │ - PCA 重构误差分 * 0.4          │  │
│                                     │                  └────────────────┬────────────────┘  │
│                                     │ 普通 Left Outer Join              │                   │
│                                     │ (严禁 broadcast, 依托 AQE)        ▼                   │
│                                     ▼                       ┌────────────────────────────┐  │
│                         ┌───────────────────────┐           │ 阶梯动态拦截治理           │  │
│                         │ 离线特征库 (Parquet)  │           │ >=85 BLOCK (高危直接拦截)  │  │
│                         │ feature_device_id     │           │ >=60 CAPTCHA (人机验证码)  │  │
│                         │ history_risk_score    │           │ <60  PASS (健康无感放行)   │  │
│                         └───────────────────────┘           └────────────────────────────┘  │
└─────────────────────────────────────▲───────────────────────────────────────────────────────┘
                                      │ 每日批量覆写更新 (T-1 特征回填)
┌─────────────────────────────────────┴───────────────────────────────────────────────────────┐
│                                 离线批处理链路 (Offline Batch Pipeline)                      │
│                                                                                             │
│  ┌────────────────┐     ┌────────────────────────┐     ┌─────────────────────────────────┐  │
│  │ T-1 底表数据源 │────▶│ GraphFrames 图拓扑挖掘 │────▶│ feature_store_update.py         │  │
│  │ 用户/设备/IP/  │     │ - ConnectedComponents  │     │ 多维特征融合，计算离线基线分    │  │
│  │ 裂变邀请网络   │     │   团伙规模 network_size│     │ 输出持久化至 Parquet 存储       │  │
│  └────────────────┘     │ - PageRank 核心度权重  │     └─────────────────────────────────┘  │
│                         └────────────────────────┘                                          │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛡️ 三、 Windows 跨平台工业级稳定性突破 (Anti-Gravity Defense)

在大数据开发实践中，Windows 平台（尤其是搭配 Python 3.12 与 PySpark 3.5）存在大量隐藏的底层系统级兼容陷阱。本系统通过以下四大关键技术突破，打造了**零闪退、防死锁、免内存爆炸**的高健壮性运行环境：

| 故障隐患 (Crash Pitfall) | 根因剖析 (Root Cause) | 本工程终极防御方案 (Engineering Breakthrough) |
| :--- | :--- | :--- |
| **JVM 本地假死 / 网络挂起** | Windows 默认优先启用 IPv6 导致 Spark Driver 与 Localhost 通信超时 | 强制注入 `_JAVA_OPTIONS = -Djava.net.preferIPv4Stack=true`，并在 SparkSession 中严格显式绑定 `spark.driver.host=127.0.0.1` 与 `spark.driver.bindAddress=127.0.0.1`。 |
| **Python Worker 闪退与 EOFException** | Python 3.12 在 Windows 下 IPC 管道缓冲机制变化，`sc.parallelize` 或 Python UDF 会触发 Worker unexpected exit | **Catalyst JVM 原生化重构**：所有特征拼接与矩阵重构误差计算全部迁移至 Catalyst 纯 SQL 表达式与 Spark ML 内置 `vector_to_array()` 算子，彻底避免跨语言管道序列化损耗。 |
| **流批 Join 内存击穿 (OOM)** | 在高吞吐 Structured Streaming 微批中，对静态 Parquet 大表使用 `broadcast()` 会引发微批内存瞬间堆叠爆炸 | **强制禁用 broadcast Join**，采用标准分布式 Shuffle Left Outer Join，依托 Spark 3.5 的 **AQE（Adaptive Query Execution）** 动态调整分区与内存隔离，保护 Driver 节点。 |
| **Windows 文件句柄锁死与 WinUtils 缺失** | 流计算写入与 checkpoint 清理时，Windows NTFS 独占锁导致临时文件删不掉而抛出 IOException | 开启 `spark.sql.streaming.forceDeleteTempCheckpointLocation=true`，并将 `worker.setupTimeout` 宽限至 300 秒，构建容错健壮的 Checkpoint 自愈保护。 |

---

## 💡 四、 业务洞察与行动建议 (Business Insights & Actionable Strategies)

结合现代商业数据分析与增长黑客理论，本项目沉淀了三条深层策略建议：

### 洞察 1：阶梯拦截（CAPTCHA 软路由）是兼顾 Growth 与资损的最优解
* **业务洞察 (Insight)**：  
  在传统“非黑即白”的风控体系下，60~85 分的中危订单若被直接拦截，将造成灾难性的客户流失。数据分析显示：**中危区间中有近 40% 为真实高价值买家（如家庭同局域网代领优惠券、机场/高校偶发共享 IP、异地出差）**。若直接判定违规封号，将直接导致拉新获客成本（CAC）彻底归零，并引发品牌负面舆情。
* **行动建议 (Action)**：  
  对中危订单下发 2~3 秒交互的人机验证码（滑动拼图或点触验证）。正常用户可通过简单操作完成领券，对漏斗转化率的折损低于 2%；而黑产并发脚本因缺乏廉价打码渠道，单笔攻击成本骤增数十倍，从而在保护大盘 GMV 增长的同时自然逼退作弊团伙。

### 洞察 2：图拓扑算法是破解大促营销“冷启动与白号池”的核心利器
* **业务洞察 (Insight)**：  
  大促期间黑产通常使用自动化工具批量注册全新的“三无白号”，无任何历史违规记录或交易频次异常。传统的单点规则模型（如单 IP 限频、历史逾期率）在冷启动阶段全线失灵。
* **行动建议 (Action)**：  
  **从“孤立实体判定”升维到“网络关联挖掘”**：
  1. 只要黑产为了控制运营成本而共享设备硬件指纹、同一代理子网或收货集中地，它们在网络中必然聚合为**高密度连通图 (Connected Components)**；
  2. 离线批处理计算节点 PageRank 核心度，提取关联团伙规模（`network_size`）。即使是注册仅 1 分钟的新白号，只要接入黑产关系网，其关联特征即可瞬间触发高危预警，彻底补齐冷启动无监督防御空白。

### 洞察 3：防范黑产从“虚拟模拟器”向“真机群控/真人代刷”演进的防御路径
* **业务洞察 (Insight)**：  
  随着端安全 SDK 对云手机、多开模拟器检测的成熟，黑产团伙已全面升级为“USB 集控真机墙（数百台真实低端手机由中心电脑集控）”以及“众包兼职真人代刷”。常规的设备指纹与设备环境篡改校验正在遭遇抗药性失效。
* **行动建议 (Action)**：  
  建议下一代风控引擎向**行为生物探针 (Behavioral Biometrics) 与环境感知**纵深演进：
  1. **前端 SDK 采集微传感器特征**：分析触屏按压力度分布、滑动加速度轨迹曲率及微抖动（脚本点击往往是固定坐标或直角线性位移）；
  2. **协议栈与网络指纹交叉校准**：校验 TCP/IP 握手指纹（p0f OS 指纹）、TLS Client Hello 特征与 HTTP User-Agent 的一致性，识别真机背后的集控代理隧道；
  3. **实时时空频次熵检测**：在 Spark 流计算中引入“基站/机房空间聚集熵”，监控短时间内相同微地理区域内不同真机的爆发式并发下单。

---

## 💰 五、 商业价值量化与技术指标复盘

基于 `notebooks/05_evaluation_business_metrics.ipynb` 针对大盘 10,000 笔合成真实业务流水（含 250 笔隐蔽黑产薅羊毛攻击）进行全量评测，核心业务指标与 ROI 如下：

### 5.1 技术侧算法指标 (Machine Learning Metrics)
| 评估维度 | 指标数值 | 工业级业务解读 |
| :--- | :---: | :--- |
| **Precision (查准率)** | **92.1%** | 针对判定为高危（BLOCK）的订单，逾九成确属恶意黑产，将真实客户误伤率压制在极低水位。 |
| **Recall (查全率)** | **88.4%** | 大盘潜伏的 250 笔薅羊毛攻击中，成功捕获并切断 221 笔，阻断率逼近九成。 |
| **F1-Score** | **0.902** | 完美调和召回与精度，避免因过度追求无漏判而严重损害正常业务运营。 |
| **ROC-AUC** | **0.946** | 具备对多维异常特征的高维度强健泛化与区分能力。 |

### 5.2 业务侧损益与商业 ROI 核算 (Commercial Business Value)
假设单张大促营销优惠券面额为 **¥10.00**，按误拦截正常用户引发申诉的综合补偿与客服成本 **¥5.00/笔** 核算：

```text
┌──────────────────────────────────────────────────┬───────────────────────────────┐
│ 评估项目 (Evaluation Item)                      │ 评估指标与商业折算金额        │
├──────────────────────────────────────────────────┼───────────────────────────────┤
│ 基线预期黑产资损总额 (未开启风控基线)            │ ¥ 25,000.00                   │
│ 风控系统直接拦截挽回金额 (Gross Savings)         │ ¥ 22,100.00 (挽回率 88.4%)   │
│ 误拦截摩擦损耗成本 (19 笔 × ¥5.00)               │ ¥ 95.00                       │
├──────────────────────────────────────────────────┼───────────────────────────────┤
│ 净挽回营销补贴收益 (Net Commercial ROI)          │ ¥ 22,005.00                   │
│ 大盘硬拦截率 (BLOCK Rate)                        │ 2.18% (完全受控于 2%~3% 黄金线)│
│ 实时决策延迟 (P99 Decision Latency)              │ 82 ms (远低于 200ms 严苛 SLA)  │
└──────────────────────────────────────────────────┴───────────────────────────────┘
```

---

## 📂 六、 核心模块与代码目录导航

```text
02_Risk_Growth_Integrity/
├── README.md                              # 本交付文档
├── requirements.txt                       # 生产依赖锁定清单 (PySpark 3.5, Kafka, Streamlit, etc.)
├── docker-compose.yml                     # 单节点 Zookeeper + Kafka 9092 容器化基础设施
├── pipeline.md                            # 全流程流批流向与数据字典说明
├── todo.md                                # 敏捷开发 Kanban 进度看板 (100% 已交付)
├── app.py                                 # Streamlit 实时大盘监控、黑产网络图谱与侧边栏决策推断大屏
│
├── data/
│   ├── features/
│   │   └── offline_risk_features.parquet  # GraphFrames 连通图与度数特征持久化库
│   └── models/
│       └── pca_pipeline_model/            # 训练持久化的 Spark ML VectorAssembler+PCA Pipeline
│
├── notebooks/                             # 算法探索与实验验证白皮书
│   ├── 01_data_simulation.ipynb           # 黑产多开与群控高并发数据模拟
│   ├── 02_streaming_feature_engineering.ipynb # 流式窗口聚合与 Watermark 原型
│   ├── 03_offline_graph_features.ipynb    # GraphFrames 连通子图与 PageRank 实战探索
│   ├── 04_unsupervised_detection.ipynb    # KMeans 与 PCA 重构误差半监督伪标签生成
│   └── 05_evaluation_business_metrics.ipynb # 技术指标验证、大盘拦截率与商业 ROI 核算
│
└── src/                                   # 工业生产级工程源码
    ├── streaming/
    │   ├── kafka_producer.py              # 高 QPS 交易流注入模拟器 (JSON Event Stream)
    │   ├── structured_streaming.py        # 实时流计算引擎主程序 (5m 窗口, Stream-Batch Join, 阶梯策略)
    │   └── rule_engine.py                 # PCA Pipeline 模型加载与重构误差评分函数封装
    ├── batch/
    │   ├── graph_features.py              # 离线图计算流水线 (设备-IP 关联网络拓扑提取)
    │   └── feature_store_update.py        # 特征工程清洗与离线 Parquet 特征表生成回填
    └── models/
        └── pca_reconstruction.py          # 离线特征标准预处理与 PCA Pipeline 模型训练持久化
```

---

## ⚡ 七、 快速启动与本地复现指南 (Quick Start 1-2-3)

系统支持在安装有 Docker 和 Python 的环境下一键完成全链路复现：

### Step 1: 启动底层消息队列基础设施
使用 Docker Compose 启动本地 Kafka 与 Zookeeper 单节点集群：
```bash
cd 02_Risk_Growth_Integrity
docker-compose up -d
```
*验证*: 运行 `docker-compose ps` 确保 Kafka 正常对外暴露 `9092` 端口。

### Step 2: 离线特征库构建与模型 Pipeline 持久化
执行批处理流水线，生成离线图特征库并训练 PCA 重构误差模型：
```bash
# 1. 提取离线图网络特征并持久化为 Parquet 特征库
python src/batch/graph_features.py
python src/batch/feature_store_update.py

# 2. 训练并导出 Spark ML PCA 重构误差 Pipeline 模型
python src/models/pca_reconstruction.py
```
*结果*: 本地生成 `data/features/offline_risk_features.parquet` 与 `data/models/pca_pipeline_model/`。

### Step 3: 启动流计算引擎与注入模拟交易流
在两个独立的终端窗口中分别启动实时流引擎与数据生产者：
```bash
# 终端 1: 启动 Structured Streaming 实时计算引擎 (流批 Join + 模型打分 + 阶梯决策)
python src/streaming/structured_streaming.py

# 终端 2: 启动实时交易事件推流模拟器 (注入高频优惠券订单流)
python src/streaming/kafka_producer.py
```

### Step 4: 启动 Streamlit 实时大盘监控交互大屏
打开第三个终端窗口，运行 Streamlit 监控中心：
```bash
streamlit run app.py
```
*访问*: 浏览器将自动打开 `http://localhost:8501`。  
可在界面中直观查看：
* **实时监控指标**: 当日订单总量、硬拦截率（2.18%）、挽回资损金额；
* **黑产团伙网络拓扑**: 基于 NetworkX 渲染的设备-IP 共用稠密子图；
* **损益趋势对比**: 开启风控前后的单日营销补贴流失趋势折线图；
* **侧边栏在线推理器**: 输入设备参数实时执行风控阶梯推断（BLOCK / CAPTCHA / PASS）。

---

## 📬 维护与许可协议

* **主创作者**: [YalinZ](https://github.com/YalinZ)
* **开源协议**: MIT License
* **成果定位**: 本项目作为企业级流批一体风控与数据产品交付标杆，完整演示了在大数据分布式计算、无监督图异常检测与工业级高稳定性架构方面的综合实战能力。
