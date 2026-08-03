import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import joblib
import io
import os

# Resolve paths dynamically relative to this script's directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def get_path(filename):
    return os.path.join(BASE_DIR, filename)

# ----------------- PAGE CONFIGURATION -----------------
st.set_page_config(
    page_title="电商客户分群与流失预警看板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for Premium Design
st.markdown("""
<style>
    .kpi-card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
        border-left: 5px solid #636EFA;
        margin-bottom: 20px;
    }
    .kpi-title {
        font-size: 14px;
        color: #6c757d;
        text-transform: uppercase;
        font-weight: bold;
    }
    .kpi-value {
        font-size: 28px;
        font-weight: bold;
        color: #212529;
        margin-top: 5px;
    }
    .main-title {
        font-family: 'Outfit', sans-serif;
        color: #1E3A8A;
        font-weight: 800;
        margin-bottom: 10px;
    }
</style>
""", unsafe_allow_html=True)

# ----------------- DATA LOADING -----------------
@st.cache_data
def load_dashboard_data():
    df_cust = pd.read_csv(get_path('dashboard_customer_data.csv'))
    df_tx = pd.read_csv(get_path('dashboard_transaction_data.csv'))
    df_tx['InvoiceDate'] = pd.to_datetime(df_tx['InvoiceDate'])
    return df_cust, df_tx

df_cust, df_tx = load_dashboard_data()

# Load models
@st.cache_resource
def load_models():
    scaler = joblib.load(get_path('scaler.pkl'))
    model = joblib.load(get_path('xgb_model.pkl'))
    return scaler, model

scaler, xgb_model = load_models()

# ----------------- SIDEBAR FILTERS -----------------
st.sidebar.markdown("## 🔍 商业分析筛选器")

# Country filter
countries = sorted(df_cust['Country'].unique())
selected_countries = st.sidebar.multiselect(
    "选择国家",
    options=countries,
    default=["United Kingdom"] if "United Kingdom" in countries else [countries[0]]
)

# If none selected, fallback to all
if not selected_countries:
    selected_countries = countries

# Customer Tag filter
tags = sorted(df_cust['Customer_Tag'].unique())
selected_tags = st.sidebar.multiselect(
    "选择客群类型",
    options=tags,
    default=tags
)

if not selected_tags:
    selected_tags = tags

# Filtered dataframes
df_cust_filtered = df_cust[
    (df_cust['Country'].isin(selected_countries)) &
    (df_cust['Customer_Tag'].isin(selected_tags))
]

df_tx_filtered = df_tx[
    (df_tx['Country'].isin(selected_countries))
]

# ----------------- HEADER & MAIN TITLE -----------------
st.markdown("<h1 class='main-title'>📊 电商客户分群与流失预警看板</h1>", unsafe_allow_html=True)
st.markdown("基于 RFM 聚类模型、Cohort 留存矩阵与 XGBoost & SHAP 概率模型的商业敏捷决策支持系统")
st.markdown("---")

# ----------------- KPI TOP BAR -----------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_cust = df_cust_filtered.shape[0]
    st.markdown(f"""
    <div class="kpi-card" style="border-left-color: #636EFA;">
        <div class="kpi-title">总客户数 (Filtered)</div>
        <div class="kpi-value">{total_cust:,}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    avg_ltv = df_cust_filtered['expected_ltv_1y'].mean()
    st.markdown(f"""
    <div class="kpi-card" style="border-left-color: #00CC96;">
        <div class="kpi-title">平均 1年预期 LTV</div>
        <div class="kpi-value">£{avg_ltv:,.2f}</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    vip_count = df_cust_filtered[df_cust_filtered['Customer_Tag'] == '高价值 VIP 客户'].shape[0]
    vip_pct = (vip_count / total_cust * 100) if total_cust > 0 else 0
    st.markdown(f"""
    <div class="kpi-card" style="border-left-color: #EF553B;">
        <div class="kpi-title">VIP 客户占比</div>
        <div class="kpi-value">{vip_pct:.1f}%</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    pred_churn_rate = df_cust_filtered['churn_proba'].mean() * 100
    st.markdown(f"""
    <div class="kpi-card" style="border-left-color: #AB63FA;">
        <div class="kpi-title">预测整体流失率</div>
        <div class="kpi-value">{pred_churn_rate:.1f}%</div>
    </div>
    """, unsafe_allow_html=True)

# ----------------- TABS SETUP -----------------
tab1, tab2, tab3 = st.tabs(["📈 客群探索与留存矩阵", "🔮 个人流失风险模拟器", "📥 高危 VIP 客户导出"])

# ----------------- TAB 1: EXPLORATION & COHORT -----------------
with tab1:
    st.subheader("客群立体分布与 Cohort 留存追踪")
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.markdown("##### 1. RFM 客户分群 3D 立体分布图")
        # Color mapping dict
        colors_map = {
            '高价值 VIP 客户': '#EF553B',
            '沉睡高价值客户': '#AB63FA',
            '一般保持客户': '#636EFA',
            '低价值流失客户': '#B6B6B6'
        }
        
        # Draw 3D scatter plot
        fig_3d = px.scatter_3d(
            df_cust_filtered,
            x=np.log1p(df_cust_filtered['Recency']),
            y=np.log1p(df_cust_filtered['Frequency']),
            z=np.log1p(df_cust_filtered['Monetary']),
            color='Customer_Tag',
            color_discrete_map=colors_map,
            hover_data=['CustomerID', 'Recency', 'Frequency', 'Monetary', 'expected_ltv_1y', 'churn_proba'],
            labels={
                'x': 'Log(Recency)',
                'y': 'Log(Frequency)',
                'z': 'Log(Monetary)'
            },
            opacity=0.6,
            height=600
        )
        
        fig_3d.update_layout(
            margin=dict(l=0, r=0, b=0, t=30),
            scene=dict(
                xaxis_title='Log(Recency)',
                yaxis_title='Log(Frequency)',
                zaxis_title='Log(Monetary)'
            ),
            legend=dict(
                yanchor="top",
                y=0.9,
                xanchor="left",
                x=0.1
            )
        )
        st.plotly_chart(fig_3d, use_container_width=True)
        
    with col_chart2:
        st.markdown("##### 2. 动态 Cohort 留存热力图")
        
        if df_tx_filtered.empty:
            st.warning("所选筛选条件下无交易数据，无法构建 Cohort。")
        else:
            # Dynamic Cohort Calculation based on filtered transaction data
            df_tx_filtered = df_tx_filtered.copy()
            df_tx_filtered['TransactionMonth'] = df_tx_filtered['InvoiceDate'].dt.to_period('M')
            df_tx_filtered['CohortMonth'] = df_tx_filtered.groupby('CustomerID')['InvoiceDate'].transform('min').dt.to_period('M')
            df_tx_filtered['CohortIndex'] = (df_tx_filtered['TransactionMonth'].dt.year - df_tx_filtered['CohortMonth'].dt.year) * 12 + \
                                            (df_tx_filtered['TransactionMonth'].dt.month - df_tx_filtered['CohortMonth'].dt.month)
            
            cohort_counts = df_tx_filtered.groupby(['CohortMonth', 'CohortIndex'])['CustomerID'].nunique().reset_index()
            cohort_pivot = cohort_counts.pivot(index='CohortMonth', columns='CohortIndex', values='CustomerID')
            cohort_sizes = cohort_pivot.iloc[:, 0]
            retention_matrix = cohort_pivot.divide(cohort_sizes, axis=0)
            
            # Format matrix for Plotly heatmap
            # Exclude Index 0 (100% retention) to show variance
            ret_matrix_sub = retention_matrix.iloc[:, 1:]
            
            # Reset Period type to string for plot compatibility
            y_labels = [str(m) for m in ret_matrix_sub.index]
            x_labels = [f"M+{col}" for col in ret_matrix_sub.columns]
            
            # Create interactive heatmap using go.Heatmap
            fig_hm = go.Figure(data=go.Heatmap(
                z=ret_matrix_sub.values * 100,
                x=x_labels,
                y=y_labels,
                colorscale='YlGnBu',
                text=np.round(ret_matrix_sub.values * 100, 1),
                texttemplate="%{text}%",
                hoverongaps=False,
                colorbar=dict(title="留存率 (%)")
            ))
            
            fig_hm.update_layout(
                yaxis=dict(autorange="reversed"),
                margin=dict(l=50, r=20, b=50, t=30),
                height=600,
                xaxis_title="Cohort 周期",
                yaxis_title="首次购买月份"
            )
            st.plotly_chart(fig_hm, use_container_width=True)

# ----------------- TAB 2: INFERENCE SIMULATOR -----------------
with tab2:
    st.subheader("单体客户流失风险模拟评估")
    st.markdown("通过以下滑块动态输入客户指标，模型将实时预测其流失率，并给出对应的警报等级和运营召回建议。")
    
    sim_col1, sim_col2 = st.columns([1, 1])
    
    with sim_col1:
        st.markdown("##### 🛠️ 输入客户行为特征")
        
        recency_val = st.slider("最近一次购买距今天数 (Recency)", min_value=1, max_value=730, value=90, step=1)
        frequency_val = st.slider("历史购买频次 (Frequency / 订单数)", min_value=1, max_value=100, value=5, step=1)
        monetary_val = st.slider("累计消费金额 (£) (Monetary)", min_value=5.0, max_value=5000.0, value=850.0, step=10.0)
        unique_items_val = st.slider("购买商品种类数 (UniqueItems)", min_value=1, max_value=200, value=30, step=1)
        refund_count_val = st.slider("退货总次数 (RefundCount)", min_value=0, max_value=50, value=2, step=1)
        
        # Calculate dynamic inputs
        avg_monetary_val = monetary_val / frequency_val
        refund_rate_val = refund_count_val / (frequency_val + refund_count_val + 1e-5)
        
        st.markdown("---")
        st.write(f"**自动计算衍生特征：**")
        st.write(f"- 平均订单客单价: `£{avg_monetary_val:.2f}`")
        st.write(f"- 订单退货率 (Refund Rate): `{refund_rate_val:.2%}`")
        
    with sim_col2:
        st.markdown("##### 🔮 模型评估预测")
        
        if st.button("评估流失风险", type="primary", use_container_width=True):
            # Input align and scale
            input_df = pd.DataFrame([{
                'Frequency': frequency_val,
                'AvgMonetary': avg_monetary_val,
                'Recency': recency_val,
                'UniqueItems': unique_items_val,
                'RefundCount': refund_count_val,
                'RefundRate': refund_rate_val
            }])
            
            input_scaled = scaler.transform(input_df)
            input_scaled_df = pd.DataFrame(input_scaled, columns=input_df.columns)
            
            # Predict Churn probability
            proba = xgb_model.predict_proba(input_scaled_df)[0, 1]
            
            # Gauge chart visualization
            fig_gauge = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = proba * 100,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "客户流失概率预测 (%)", 'font': {'size': 20}},
                gauge = {
                    'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                    'bar': {'color': "#EF553B" if proba > 0.7 else ("#FMCB00" if proba > 0.3 else "#00CC96")},
                    'bgcolor': "white",
                    'borderwidth': 2,
                    'bordercolor': "gray",
                    'steps': [
                        {'range': [0, 30], 'color': 'rgba(0, 204, 150, 0.1)'},
                        {'range': [30, 75], 'color': 'rgba(255, 203, 0, 0.1)'},
                        {'range': [75, 100], 'color': 'rgba(239, 85, 59, 0.1)'}
                    ]
                }
            ))
            
            fig_gauge.update_layout(height=350, margin=dict(l=20, r=20, b=20, t=50))
            st.plotly_chart(fig_gauge, use_container_width=True)
            
            # Alert Level & Suggestions
            if proba >= 0.75:
                st.error("🚨 **高危流失风险客户 (Risk Level: HIGH)**")
                st.markdown("""
                * **业务特征**：该客户近期长期未交易，或者近期多次发生退货事件，客户满意度严重下滑。
                * **行动策略**：
                  1. **立即接入专属人工回访**，核查其近期退货单处理时效。
                  2. 电话致歉并为其账户主动补偿一张 **大额无门槛尊享代金券** (如 £50)，防止客户流失流向竞品。
                """)
            elif proba >= 0.30:
                st.warning("⚠️ **中度流失风险客户 (Risk Level: MEDIUM)**")
                st.markdown("""
                * **业务特征**：客户交易活跃度有所放缓，存在开始流失的迹象。
                * **行动策略**：
                  1. 利用关联推荐算法为其推送 **搭配购买组合优惠**。
                  2. 触发自动化邮件推送，提供“买赠”任务福利卡。
                """)
            else:
                st.success("✅ **低流失风险健康客户 (Risk Level: LOW)**")
                st.markdown("""
                * **业务特征**：交易频次正常，退货少，活跃度优秀。
                * **行动策略**：
                  1. 维持常规营销沟通，减少打折骚扰。
                  2. 邀请其参加 VIP 客户升级活动，引导其成为长期超级忠诚客户。
                """)
        else:
            st.info("💡 请点击“评估流失风险”按钮开始运行机器学习预测。")

# ----------------- TAB 3: CRM DOWNLOAD & EXPORT -----------------
with tab3:
    st.subheader("高危高价值 VIP 客户 CRM 导出工具")
    st.markdown("通过集成 XGBoost 预测概率与 RFM 聚类标签，筛选出 **流失概率 > 70%** 且属于 **高价值 VIP 客户 / 沉睡高价值客户** 的名单，用于精准召回营销。")
    
    # Filter high risk high value
    crm_filter = (df_cust['churn_proba'] >= 0.70) & (df_cust['Customer_Tag'].isin(['高价值 VIP 客户', '沉睡高价值客户']))
    df_crm = df_cust[crm_filter].sort_values(by='churn_proba', ascending=False)
    
    # Select columns to display
    display_cols = [
        'CustomerID', 'Country', 'Customer_Tag', 
        'Recency', 'Frequency', 'Monetary', 
        'expected_ltv_1y', 'churn_proba'
    ]
    
    col_export_stats1, col_export_stats2 = st.columns([1, 2])
    
    with col_export_stats1:
        st.metric("筛选出的高危高价值客户数", f"{df_crm.shape[0]} 人")
        
        # Prepare CSV download button
        csv_buffer = io.StringIO()
        df_crm[display_cols].to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_data = csv_buffer.getvalue()
        
        st.download_button(
            label="📥 一键导出 CRM 召回名单 (CSV)",
            data=csv_data,
            file_name="high_risk_vip_customers.csv",
            mime="text/csv",
            type="primary",
            use_container_width=True
        )
        
        st.markdown("""
        > **📋 营销推荐动作**：
        > 1. 此名单中包含的均为企业价值高、但流失倾向大的头部客群。
        > 2. 可通过 **EDM、短信触达通道** 为名单内客户定向发放大额无门槛召回津贴。
        > 3. 对流失概率极高者（如 > 85%）建议由 VIP 客服进行专线回访调研。
        """)
        
    with col_export_stats2:
        st.markdown("##### 🔍 筛选名单示例 (Top 100)")
        st.dataframe(
            df_crm[display_cols].head(100).style.format({
                'Monetary': '£{:.2f}',
                'expected_ltv_1y': '£{:.2f}',
                'churn_proba': '{:.1%}'
            }),
            use_container_width=True
        )
