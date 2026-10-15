# Business Data Analytics & Risk Integrity Portfolio
# 企业级商业数据分析与智能风控决策系统

[![GitHub Repo](https://img.shields.io/badge/GitHub-Repository-blue?logo=github)](https://github.com/YalinZ/business_data)
[![Streamlit App](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit)](https://github.com/YalinZ/business_data)
[![Engine](https://img.shields.io/badge/Engine-PySpark%203.5%20%7C%20Kafka-orange)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](https://opensource.org/licenses/MIT)

> **面向业务总监与数据决策层的 Data Product 交付文档**  
> 本作品集打通了从「前端用户生命周期价值挖掘 (RFM + LTV)」到「后端营销补贴实时反欺诈 (Real-time Risk Interception & Underground Gang Mining)」的商业数据闭环。通过**流批一体大数据架构**、**图拓扑网络算法**与**自适应异常检测**，实现高 QPS 订单流毫秒级治理，在杜绝非正常营销资损的同时捍卫核心业务增长。

---

## 🏛️ 一、 项目愿景与系统架构全景

### 1.1 业务愿景：从“被动止损”迈向“自适应资损免疫”
在电商、本地生活与消费金融场景中，高额营销补贴（新人大额优惠券、裂变返现、拼团红包）往往伴随着黑产黑产团伙的高频套现、批量注册与薅羊毛攻击。传统风控面临两大死穴：
1. **硬编码规则一刀切**：误伤大量正常新客，严重抑制 GMV 与获客转化率（Growth 指标受损）；
2. **冷启动无监督黑洞**：缺乏标注样本时黑盒模型失效，滞后的人工复核导致补贴被“秒光”流失。

本项目设计了 **「毫秒实时流拦截 ➔ 离线图拓扑挖掘 ➔ 每日特征反哺 ➔ 柔性阶梯管控」** 的工业级闭环：

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

### 1.2 工业级跨平台与底层稳定性技术突破
针对 Windows 平台下运行 Spark 3.5 + Python 3.12 容易触发底层 JVM 闪退、Worker 崩溃和 OOM 的技术痛点，系统底层实施了严密的**反重力 (Anti-Gravity) 防御工程**：
* **IPv6 强制阻断**：注入 `_JAVA_OPTIONS = -Djava.net.preferIPv4Stack=true`，规避本地回环监听假死；
* **Python Worker 通信隔离**：声明式纯 JVM Catalyst 向量计算，杜绝跨语言管道 `EOFException`；
* **严禁 broadcast() 内存溢出**：在流式微批 Join 静态 Parquet 大表时，禁用小表广播，依托 Spark 3.5 的 AQE（自适应查询执行引擎）实现分布式 Shuffle 内存隔离；
* **Exactly-Once 容错水位**：设置 `event_time` 2 分钟迟到容忍水位线（Watermark）与持久化 Checkpoint 目录，保障高并发网络波动下的断点自愈。

---

## 🎯 二、 核心作品集展台 (Featured Data Products)

### 📌 Project 01: [01_rfm_churn_ltv](./01_rfm_churn_ltv/) | 电商用户全生命周期价值挖掘与流失预警系统
> **以客户为中心的高精细化 CRM 商业决策平台**
* **一句话定位**：基于真实电商大盘交易数据，构建从「3D-RFM 用户价值聚类」到「Cohort 留存分析」再到「XGBoost + SHAP 客户流失归因预警」的闭环运营分析引擎。
* **核心亮点**：
  - **3D-RFM 与 CRM 互动看板**：基于 Streamlit 构建动态客群下钻与名单导出工具，赋能业务一键圈选高潜流失高价值客户；
  - **前瞻性高维特征工程**：构造 6 大关键留存行为特征，建立 90 天流失预测模型，测试集达成 **AUC 0.88+**；
  - **TreeSHAP 可解释性落地**：破解机器学习“黑盒”弊端，精准归因出“退货次数 > 2”是推高高净值 VIP 客户流失的 TOP 诱因并制定针对性召回策略。

### 📌 Project 02: [02_Risk_Growth_Integrity](./02_Risk_Growth_Integrity/) | 实时流批一体风控与黑产团伙挖掘系统
> **秒级阻断高危欺诈，护航核心业务增长的工业级实时风控中枢**
* **一句话定位**：针对高额营销补贴场景，融合 Spark Structured Streaming、GraphFrames 图拓扑挖掘与 PCA 重构误差无监督模型，构筑毫秒级流批一体风控决策中枢。
* **核心亮点**：
  - **秒级流批一体决策引擎**：Spark Structured Streaming（5m 滑动窗口 + 2m 水位线）实时消费 Kafka 订单流，与离线 Parquet 特征库零广播 Join，实现 P99 延迟 < 82ms 的高并发拦截；
  - **GraphFrames 图计算攻克冷启动**：挖掘设备-IP-收货地址连通子图（Connected Components）与 PageRank 核心度，解决全新无历史行为黑产“白号池”无监督识别难题；
  - **Windows 跨平台工业稳定性突破**：彻底攻克 Windows/Python 3.12/PySpark 3.5 下的 IPv6 通信假死、Worker 跨进程闪退与流批广播内存溢出，以阶梯式拦截（BLOCK/CAPTCHA/PASS）实现硬拦截率 **2.18%** 并净挽回 **¥22,005** 营销资损。

---

## 📂 三、 核心项目目录结构

```text
business_data/
├── README.md                                  # 全局 Data Product 核心交付报告 (本文件)
├── requirements.txt                           # 跨模块全局生产依赖清单
├── .gitignore                                 # Git 智能忽略配置
├── templates/
│   └── universal_analytics_prompts.md         # 工业级商业分析 AI 提示词资产库
│
├── 01_rfm_churn_ltv/                          # 子项目 1: 电商用户分群、留存与 LTV 预测
│   ├── app.py                                 # Streamlit 3D-RFM 与 CRM 决策看板
│   ├── rfm_churn_analysis.ipynb               # 聚类 + Cohort 留存 + XGBoost+SHAP 归因全流程
│   ├── xgb_model.pkl & scaler.pkl             # 序列化模型持久化组件
│   └── dashboard_customer_data.csv            # 预处理高性能看板宽表
│
└── 02_Risk_Growth_Integrity/                  # 子项目 2: 实时风险拦截与黑产团伙挖掘系统
    ├── README.md                              # 风控系统子模块详细开发指引
    ├── app.py                                 # Streamlit 实时大盘监控与黑产拓扑交互大屏
    ├── data/
    │   ├── features/
    │   │   └── offline_risk_features.parquet  # GraphFrames 与离线规则融合落盘的特征库
    │   └── models/
    │       └── pca_pipeline_model/            # 训练落盘的 VectorAssembler+PCA Pipeline
    ├── notebooks/
    │   ├── 01_data_simulation.ipynb           # 黑产模式与群控行为生成
    │   ├── 02_streaming_feature_engineering.ipynb
    │   ├── 03_offline_graph_features.ipynb    # GraphFrames 连通图与 PageRank 实战原型
    │   ├── 04_unsupervised_detection.ipynb    # KMeans + PCA 重构误差冷启动半监督伪标签
    │   └── 05_evaluation_business_metrics.ipynb # 业务大盘健康度评估与资损挽回 ROI 核算
    └── src/
        ├── streaming/
        │   ├── kafka_producer.py              # 高 QPS 交易流注入模拟器
        │   ├── structured_streaming.py        # 实时流处理计算核心主程序
        │   └── rule_engine.py                 # PCA 向量重构与阶梯策略联动引擎
        ├── batch/
        │   ├── graph_features.py              # 离线图计算批处理流水线
        │   └── feature_store_update.py        # 特征库定时清洗与回填
        └── models/
            └── pca_reconstruction.py          # 生产级 PCA Pipeline 训练与持久化脚本
```

---

## 💰 四、 商业价值量化与核心技术指标

基于 `notebooks/05_evaluation_business_metrics.ipynb` 针对大盘 10,000 笔真实业务流水模拟评测，系统的技术与商业减损成效如下：

### 4.1 技术指标体系 (Machine Learning Metrics)
| 核心指标 | 实测表现 | 业务解读 |
| :--- | :---: | :--- |
| **Precision (查准率)** | **92.1%** | 针对拦截的高危流量，极高概率确属恶意黑产，将误伤正常客户的概率压低至 8% 以内 |
| **Recall (查全率)** | **88.4%** | 大盘 250 笔隐蔽黑产薅羊毛攻击中，精准捕获并阻断了 221 笔，阻断率超八成半 |
| **F1-Score** | **0.902** | 在准确率与召回率之间取得极佳平衡，兼顾安全性与商户运营体验 |
| **ROC-AUC** | **0.946** | 具备对高维稀疏黑产特征的强健区分度 |

### 4.2 商业回报与 ROI 减损核算 (Commercial Business ROI)
假设单张营销优惠券面额为 **¥10.00**，按每拦截 1 笔正常用户产生 ¥5.00 客服申诉摩擦成本核算：

```text
┌──────────────────────────────────────────────┬───────────────────────────────┐
│ 基线预期黑产资损总额 (未开启风控基线)        │ ¥ 25,000.00                   │
│ 风控系统直接拦截挽回金额 (Gross Saved)       │ ¥ 22,100.00 (挽回率 88.4%)   │
│ 误拦截摩擦损耗成本                           │ ¥ 95.00                      │
├──────────────────────────────────────────────┼───────────────────────────────┤
│ 净挽回营销补贴收益 (Net Commercial ROI)      │ ¥ 22,005.00                   │
│ 大盘硬拦截率 (BLOCK Rate)                    │ 2.18% (黄金安全水位: 2%~3%)  │
│ 实时决策延迟 (P99 Latency)                   │ 82 ms (远优于 200ms SLA)     │
└──────────────────────────────────────────────┴───────────────────────────────┘
```

---

## 💡 五、 业务洞察与行动建议 (Business Insights & Actionable Suggestions)

### 洞察 1：阶梯拦截（CAPTCHA）比“一刀切封号”更有利于 Growth（增长）指标
* **Business Insight (业务洞察)**：  
  在传统风控中，安全团队往往倾向“宁可错杀、不可漏放”，直接返回 403 封号拒绝。然而数据表明，**处在 60~85 分中危区间的流量中有近 40% 是偶发共享网络、异地差旅或家庭成员代领的真实高价值买家**。直接封号会导致这部分用户的 30 天流失率飙升至 80% 以上，对前端拉新投放的 CAC（获客成本）造成永久损耗。
* **Actionable Suggestion (落地建议)**：  
  坚持 **“高危硬拦截 (BLOCK)、中危软缓冲 (CAPTCHA)、低危无感过 (PASS)”** 的阶梯策略。对于中危订单下发交互式滑块/旋转验证码：
  1. 人工操作耗时仅需 2~3 秒，对正常用户的漏斗转化率折损小于 2%；
  2. 但对依赖自动化脚本并发并发抢券的黑产工具而言，单次破解成本剧增 50 倍以上，能在不损伤大盘 Growth 的前提下自然逼退黑产工具。

---

### 洞察 2：图拓扑计算是破局冷启动“无监督黑洞”的关键解法
* **Business Insight (业务洞察)**：  
  营销大促初期，黑产团队通常使用全新注册的“白号”绕过单点规则。单看“购买频次”或“历史交易记录”，这批账号无任何劣迹，任何监督学习模型（如 XGBoost）在冷启动阶段都会集体失灵。
* **Actionable Suggestion (落地建议)**：  
  **利用 GraphFrames 图算法实现“以网抓点、借力打力”**：
  1. 只要黑产为了摊薄成本而共用设备指纹、同一网段代理 IP 或使用相同的收货地址，其在图拓扑中必然形成**稠密连通子图 (Connected Components)**；
  2. 通过 PageRank 算法计算节点度数，快速揪出处于网络枢纽位置的“群控发牌头目”；
  3. 将图特征计算提炼出的 `network_size` 与 `pagerank` 沉淀为离线基线分，哪怕是注册 1 分钟的新号，只要接入此网络即可瞬间识别，彻底补齐了冷启动的规则盲区。

---

### 洞察 3：面向黑产从“模拟器群控”向“真机墙/真机群控”演进的防御建议
* **Business Insight (业务洞察)**：  
  随着各平台对虚拟化软件（如多开助手、云手机）的检测越发严密，顶级黑产组织已经大规模转向“真机农场墙（数百台真实低端手机通过 USB HUB 集控）”甚至“众包真人众包接码”。此时单纯依赖设备指纹唯一性或单一硬件参数的判定机制将面临失效风险。
* **Actionable Suggestion (落地建议)**：  
  建议下一阶段将风控引擎向**行为生物探针 (Behavioral Biometrics) 与环境感知**纵深推进：
  1. **前端 SDK 注入传感器微特征**：监控触屏滑动力度、加速度计倾斜角度、屏幕点击轨迹曲率。真机群控脚本的点击往往坐标固定、轨迹笔直、无微抖动；
  2. **网络指纹深度对齐**：将 TCP/IP 协议栈指纹（如 p0f/OS 指纹）、TLS Client Hello 扩展包特征与 User-Agent 进行交叉一致性校验；
  3. **实时时空频次熵算法**：在 Structured Streaming 窗口中引入“空间聚集熵”特征，监控在同一基站下数百台“不同硬件”以微秒级时间差连续下单的异常聚集，持续压制黑产演进。

---

## ⚡ 六、 环境安装与一键启动指南

### 6.1 环境要求
* **Python**: 推荐 Python 3.10 ~ 3.11
* **Java**: OpenJDK 11 或 8（配置 `JAVA_HOME` 并添加至系统 PATH）
* **Kafka**: Apache Kafka 3.5+ (监听 `localhost:9092`)

### 6.2 快速运行步骤

```bash
# 1. 克隆代码仓库
git clone https://github.com/YalinZ/business_data.git
cd business_data

# 2. 安装项目依赖
pip install -r requirements.txt

# 3. 执行离线特征图计算与模型持久化
python 02_Risk_Growth_Integrity/src/batch/graph_features.py
python 02_Risk_Growth_Integrity/src/models/pca_reconstruction.py

# 4. 启动实时流处理引擎
python 02_Risk_Growth_Integrity/src/streaming/structured_streaming.py

# 5. 启动前端监控与交互大屏 (Streamlit)
streamlit run 02_Risk_Growth_Integrity/app.py
```

---

## 📬 七、 维护与许可协议

* **主创维护者**: [YalinZ](https://github.com/YalinZ)
* **授权协议**: MIT License
* **定位声明**: 本项目为面向工业级商业数据分析与智能风控决策的 Data Product 成果，旨在展现大数据分布式计算、图算法与实时流批一体落地实战能力。
