# 通用商业数据分析 AI Prompt 模板 (Universal Business Analytics Prompts)

本模板整理了一套适用于通用商业交易流数据集的 AI Prompt。通过将这些 Prompt 提供给大语言模型（LLM），可以引导其自动、规范地完成从探索性数据分析（EDA）到最终交互式看板设计的全流程商业敏捷决策项目。

---

## 📋 阶段 1：探索性数据分析 (EDA) & 数据诊断

**Prompt**:
```markdown
你现在是一名资深商业数据分析师（DA）。我有一个交易流水数据集，字段包括：`Invoice`（订单号）、`StockCode`（商品编码）、`Description`（商品描述）、`Quantity`（购买数量）、`InvoiceDate`（交易时间）、`UnitPrice`（单价）、`CustomerID`（客户唯一标识）、`Country`（国家）。

请为我编写一段 Python 代码，对该数据集进行全面的 EDA 与数据质量诊断：
1. 计算数据集的维度（行数、列数）和基本字段类型。
2. 统计各字段的缺失值比例，特别是 CustomerID 的缺失比例。
3. 统计数值型特征（Quantity, UnitPrice）的异常值（如负数、零值、极高值），并输出描述性统计分析。
4. 探索交易流水中的退货数据（如以 'C' 开头的订单或 Quantity <= 0），计算退货订单占比。
5. 使用 Seaborn/Matplotlib 绘制交易量随月份的变化趋势图。
```

---

## 🧹 阶段 2：SQL/DuckDB ETL 数据提取与 RFM 清洗

**Prompt**:
```markdown
你现在是一名数据仓库工程师。请编写一段 Python 代码，使用 `duckdb` 在内存中对交易流水 CSV 文件建立查询引擎，模拟数仓 ETL 清洗与指标提取逻辑：
1. 编写 SQL 过滤掉 CustomerID 为空、Quantity <= 0 以及 UnitPrice <= 0 的无效/异常流水。
2. 使用 SQL 聚合查询，以最大交易日期加1天为“观察点日期”，按客户 CustomerID 汇总计算以下原始 RFM 指标：
   - Recency (最近一次消费间隔天数) = 观察点日期 - MAX(InvoiceDate)
   - Frequency (消费去重订单数) = COUNT(DISTINCT Invoice)
   - Monetary (累计消费金额) = SUM(Quantity * UnitPrice)
3. 将查询结果转为 Pandas DataFrame 并打印描述统计信息。
```

---

## 🤖 阶段 3：数据标准化与 K-Means 客户分群

**Prompt**:
```markdown
你现在是一名机器学习建模工程师。请基于上一步提取的 RFM 原始指标：
1. 绘制 RFM 各指标的分布直方图，检查其偏态分布（右偏）。
2. 应用对数变换 `np.log1p` 对数据进行平滑处理，缓解偏态分布影响。
3. 使用 `sklearn.preprocessing.StandardScaler` 对变换后的特征进行标准化，消除量纲影响。
4. 使用 `KMeans` 聚类算法将客户划分为 4 个核心客群（设定 random_state=42），并动态地根据各聚类的 Monetary 均值及 Recency 均值，将群组分别赋予业务标签：
   - '高价值 VIP 客户' (Monetary 最高)
   - '低价值流失客户' (Monetary 最低)
   - '沉睡高价值客户' (Recency 较大且 Monetary 较高)
   - '一般保持客户' (Recency 较小且 Monetary 较低)
5. 统计并输出各客群的客户人数占比及 GMV 贡献占比，深入剖析企业营收是否存在帕累托二八定律。
```

---

## 📊 阶段 4：Cohort 队列矩阵构建与 1年预期 LTV 预测

**Prompt**:
```markdown
你现在是一名增长分析师（Growth Hacker）。请基于清洗后的流水数据和 RFM 指标：
1. 计算每个客户的首次消费月份（CohortMonth）与后续各次交易月份（TransactionMonth），计算月份差 CohortIndex。
2. 构建月度客户留存矩阵，并使用 `seaborn.heatmap` 绘制带百分比标注的 Cohort 留存热力图。
3. 计算不同 Cohort 群体在 30天、90天、180天及365天内的平均累计消费金额与累计贡献 GMV。
4. 引入 `lifetimes` 库，提取 RFM 汇总数据，拟合 `BetaGeoFitter` (BG/NBD) 与 `GammaGammaFitter` (Gamma-Gamma) 概率模型，预测每位客户未来 1 年内的预期生命周期价值（LTV），并将预测结果合并回主客户 DataFrame 中。
```

---

## 🔮 阶段 5：XGBoost 流失预测与 SHAP 归因可解释性

**Prompt**:
```markdown
你现在是一名机器学习专家。请构建流失预警分类器：
1. 以数据集倒数第 90 天为切片点（Cutoff Date）。在此之前的交易数据用于特征提取，最后 90 天内没有任何交易的客户标记为流失（Churn = 1），否则为留存（Churn = 0）。
2. 提取观察点前的衍生特征：Frequency（购买频次）、AvgMonetary（平均客单价）、Recency（最近一次购买天数）、UniqueItems（购买商品种类数）、RefundCount（退货总次数）以及 RefundRate（订单退货率）。
3. 按照 80/20 比例划分训练集与测试集（使用 stratify=y 保持类别平衡），并进行 StandardScaler 标准化。
4. 训练 `XGBClassifier` 分类器。为了应对 xgboost 2.0+ 与 shap 库中 `base_score` ubjson 反序列化报错（ValueError），请在计算 SHAP 前编写 python 代码重载 `shap.explainers._tree.float` 处理器，消除版本解析冲突。
5. 评估测试集上的 ROC-AUC 得分、Confusion Matrix（混淆矩阵）和 Recall 召回率。
6. 使用 `shap.TreeExplainer` 计算特征贡献值，绘制全局重要性条形图（Summary Plot）和单个典型高风险客户的 Waterfall 归因图，直观展示哪些行为推高了该客户的流失概率。
7. 使用 `joblib` 将训练好的 XGBClassifier 与 Scaler 特征组件保存为本地 `.pkl` 文件。
```

---

## 🖥️ 阶段 6：Streamlit 商业敏捷决策看板开发

**Prompt**:
```markdown
你现在是一名全栈开发工程师。请帮我编写一个 Streamlit 应用（`app.py`），打造一个现代化的商业分析看板：
1. **页面布局**：顶部展示 4 个关键商业 KPI 卡片（总客户数、平均预期 1年 LTV、VIP 客户占比、预测整体流失率）。
2. **多维筛选器**：侧边栏支持按国家（Country）、客群标签（Customer_Tag）进行动态多选。
3. **数据可视化**：
   - 用 Plotly 绘制 3D 立体散点图，直观呈现在 Log 空间中 4 大客群的分布分界。
   - 实现动态 Cohort 留存计算，响应侧边栏的国家筛选，实时绘制 Plotly Cohort 留存百分比热力图。
4. **单体流失风险模拟器**：
   - 侧边栏/主页面提供滑动条输入单体客户指标（Recency, Frequency, Monetary, UniqueItems, RefundCount），自动计算平均客单价与 RefundRate。
   - 加载本地保存的 `scaler.pkl` 和 `xgb_model.pkl`，对输入数据进行标准化后送入模型进行预测。
   - 用 Plotly 仪表盘 (Gauge Chart) 展示该客户的流失概率，并输出预警等级（高、中、低）和针对性的客服运营建议。
5. **CRM 名单导出**：
   - 过滤流失概率 >= 70% 且属于 VIP 客户的名单，提供 `st.download_button` 允许运营人员一键导出为 CSV 名单进行精准推送。
```
