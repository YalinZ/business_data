import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import networkx as nx
import os

# ==============================================================================
# 页面基础配置 (宽屏沉浸式布局)
# ==============================================================================
st.set_page_config(
    page_title="实时风险拦截与黑产团伙挖掘大屏",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定义 CSS 样式加固
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1f2937 0%, #111827 100%);
        border-left: 5px solid #3b82f6;
        padding: 18px;
        border-radius: 10px;
        color: white;
        margin-bottom: 15px;
    }
    .metric-val {
        font-size: 28px;
        font-weight: 800;
        color: #60a5fa;
    }
    .metric-sub {
        font-size: 13px;
        color: #9ca3af;
    }
    .decision-block {
        background-color: #fee2e2;
        border: 2px solid #ef4444;
        color: #991b1b;
        padding: 12px;
        border-radius: 8px;
        font-weight: bold;
    }
    .decision-captcha {
        background-color: #fef3c7;
        border: 2px solid #f59e0b;
        color: #92400e;
        padding: 12px;
        border-radius: 8px;
        font-weight: bold;
    }
    .decision-pass {
        background-color: #d1fae5;
        border: 2px solid #10b981;
        color: #065f46;
        padding: 12px;
        border-radius: 8px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 侧边栏：单笔交易风控实时模拟推断 (Inference Simulator)
# ==============================================================================
st.sidebar.image("https://img.icons8.com/fluency/96/shield.png", width=70)
st.sidebar.title("⚡ 风险推断模拟器")
st.sidebar.markdown("模拟生产网关实时注入单笔交易特征，进行毫秒级模型与阶梯策略推理。")

with st.sidebar.form("simulate_form"):
    st.subheader("1. 进件特征输入")
    input_device_id = st.text_input("设备指纹 (Device ID)", value="dev_gang_01")
    input_coupon_1h = st.slider("1小时内领券频次 (Coupon Count)", 0, 30, 8)
    input_shared_acc = st.slider("设备关联账号数 (Shared Accounts)", 1, 20, 5)
    input_refund_count = st.slider("近期退单数 (Refund Count)", 0, 10, 2)
    input_amount = st.number_input("订单交易金额 (¥)", min_value=1.0, max_value=50000.0, value=1280.0, step=50.0)

    # 是否加载离线特征库基线
    base_graph_score = st.slider("离线图特征基线分 (History Graph Score)", 0.0, 100.0, 78.5)

    submit_btn = st.form_submit_button("🚀 执行实时风控推断", use_container_width=True)

# 侧边栏计算逻辑
if submit_btn:
    # 1. 实时聚合规则分 (0 ~ 100)
    refund_rate = input_refund_count / (input_shared_acc + 1e-5)
    realtime_rule_score = min(100.0, (input_coupon_1h * 6.5) + (input_shared_acc * 8.0) + (refund_rate * 40.0))
    
    # 2. PCA 降维重构误差分 (模拟异常偏离度)
    pca_anomaly_score = min(100.0, (input_shared_acc * 9.5) + (input_coupon_1h * 3.5) + (input_amount / 800.0 * 5.0))
    
    # 3. 最终多源融合总分: 离线图特征分*0.3 + 实时规则分*0.3 + PCA模型分*0.4
    final_score = round((base_graph_score * 0.3) + (realtime_rule_score * 0.3) + (pca_anomaly_score * 0.4), 2)

    st.sidebar.markdown("---")
    st.sidebar.subheader("🎯 实时推断结果")
    st.sidebar.metric("综合风控评分 (Final Score)", f"{final_score} / 100")
    
    # 阶梯决策
    if final_score >= 85.0:
        st.sidebar.markdown("""
        <div class="decision-block">
            🚨 决策: BLOCK (高危直接拦截)<br>
            <span style="font-size:12px; font-weight:normal;">触发黑产团伙与领券聚集特征，已拒绝本次领券/交易请求，自动加入阻断黑名单。</span>
        </div>
        """, unsafe_allow_html=True)
    elif final_score >= 60.0:
        st.sidebar.markdown("""
        <div class="decision-captcha">
            ⚠️ 决策: CAPTCHA (中危弹验证码)<br>
            <span style="font-size:12px; font-weight:normal;">存在一定设备聚集可疑度，下发交互式图形验证码二次人机校验。</span>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.sidebar.markdown("""
        <div class="decision-pass">
            ✅ 决策: PASS (低危健康放行)<br>
            <span style="font-size:12px; font-weight:normal;">用户行为与设备拓扑处于基线正常区间，毫秒级无感放行。</span>
        </div>
        """, unsafe_allow_html=True)

# ==============================================================================
# 主界面 Header
# ==============================================================================
st.title("🛡️ 02_Risk_Growth_Integrity | 实时风险拦截与黑产团伙挖掘大屏")
st.markdown("基于 **Spark Structured Streaming + GraphFrames + PCA 异常检测** 的流批一体自适应风控决策中台")
st.markdown("---")

# ==============================================================================
# 模块 1：实时风险监控仪表盘 (Top KPIs)
# ==============================================================================
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="当日总进件量 (Transactions)",
        value="128,450 笔",
        delta="+14.2% 较昨日"
    )

with col2:
    st.metric(
        label="大盘硬拦截率 (BLOCK Rate)",
        value="2.18 %",
        delta="-0.32% (受控健康水位: 2%~3%)",
        delta_color="normal"
    )

with col3:
    st.metric(
        label="累计挽回营销资损 (Loss Saved)",
        value="¥ 184,620",
        delta="+¥ 24,800 今日拦截"
    )

with col4:
    st.metric(
        label="实时流决策耗时 (P99 Latency)",
        value="82 ms",
        delta="低于 200ms SLA"
    )

st.markdown("<br>", unsafe_allow_html=True)

# ==============================================================================
# 模块 2 与 模块 3：黑产团伙图网络可视化 & 资损流失趋势对比
# ==============================================================================
col_graph, col_chart = st.columns([1.1, 1.0])

with col_graph:
    st.subheader("🕸️ 离线黑产团伙关联网络图谱 (GraphFrames)")
    st.caption("展示基于设备指纹、收货地址与裂变邀请链路挖掘出的「羊毛党黑产群控网络」拓扑。")

    # 构建 NetworkX 拓扑图
    G = nx.Graph()

    # 团伙 1: 设备群控套现团伙 (节点: 手机设备 dev_gang_01, dev_gang_02, 用户 u101~u106, 共享IP)
    gang_nodes = [
        ("dev_gang_01", {"type": "device", "risk": "high"}),
        ("dev_gang_02", {"type": "device", "risk": "high"}),
        ("IP_10.0.0.1", {"type": "ip", "risk": "medium"}),
        ("u101", {"type": "user", "risk": "high"}),
        ("u102", {"type": "user", "risk": "high"}),
        ("u103", {"type": "user", "risk": "high"}),
        ("u104", {"type": "user", "risk": "high"}),
        ("u105", {"type": "user", "risk": "high"}),
        ("u106", {"type": "user", "risk": "high"}),
    ]

    # 正常对照子图
    norm_nodes = [
        ("dev_norm_01", {"type": "device", "risk": "low"}),
        ("u301", {"type": "user", "risk": "low"}),
        ("IP_172.16.0.1", {"type": "ip", "risk": "low"}),
    ]

    for n, attr in gang_nodes + norm_nodes:
        G.add_node(n, **attr)

    edges = [
        ("u101", "dev_gang_01"), ("u102", "dev_gang_01"), ("u103", "dev_gang_01"),
        ("dev_gang_01", "IP_10.0.0.1"), ("dev_gang_02", "IP_10.0.0.1"),
        ("u104", "dev_gang_02"), ("u105", "dev_gang_02"), ("u106", "dev_gang_02"),
        ("u101", "u102"), ("u102", "u104"), ("u103", "u105"), # 裂变链条
        ("u301", "dev_norm_01"), ("dev_norm_01", "IP_172.16.0.1")
    ]
    G.add_edges_from(edges)

    # 布局计算
    pos = nx.spring_layout(G, seed=42, k=0.6)

    # 抽取边坐标
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=1.2, color='#94a3b8'),
        hoverinfo='none',
        mode='lines'
    )

    # 抽取点坐标
    node_x = []
    node_y = []
    node_colors = []
    node_texts = []
    node_sizes = []

    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        ntype = G.nodes[node]['type']
        nrisk = G.nodes[node]['risk']
        
        node_texts.append(f"节点: {node}<br>类型: {ntype.upper()}<br>风险评级: {nrisk.upper()}")
        
        if nrisk == 'high':
            node_colors.append('#ef4444')
            node_sizes.append(20 if ntype == 'device' else 14)
        elif nrisk == 'medium':
            node_colors.append('#f59e0b')
            node_sizes.append(16)
        else:
            node_colors.append('#10b981')
            node_sizes.append(14)

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        hoverinfo='text',
        text=[n if 'dev' in n or 'IP' in n else '' for n in G.nodes()],
        textposition="top center",
        marker=dict(
            color=node_colors,
            size=node_sizes,
            line_width=1.5,
            line_color='#ffffff'
        )
    )
    node_trace.hovertext = node_texts

    fig_network = go.Figure(data=[edge_trace, node_trace],
                 layout=go.Layout(
                    showlegend=False,
                    hovermode='closest',
                    margin=dict(b=10, l=10, r=10, t=20),
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    height=450,
                    plot_bgcolor='#f8fafc'
                ))

    st.plotly_chart(fig_network, use_container_width=True)
    st.markdown("""
    <div style="font-size: 13px; color: #4b5563;">
        🔴 <b>红色节点</b>: 高危黑产团伙成员 / 核心群控设备 | 
        🟡 <b>黄色节点</b>: 共用代理 IP 节点 | 
        🟢 <b>绿色节点</b>: 正常孤立买家
    </div>
    """, unsafe_allow_html=True)

with col_chart:
    st.subheader("📉 开启风控系统前后补贴流失趋势对比")
    st.caption("单日补贴流失对比 (近 14 天)：蓝线为历史无风控基线，绿线为风控系统实时拦截后残留漏损。")

    # 生成 14 天对比数据
    days = [f"10月{i}日" for i in range(1, 15)]
    np.random.seed(99)
    baseline_loss = np.random.normal(loc=18000, scale=1200, size=14) # 无风控，每天损失 1.8 万元
    # 开启风控后，拦截率 88%，残留损失仅 12% 左右
    with_risk_loss = baseline_loss * np.random.uniform(0.09, 0.14, size=14)
    saved_loss = baseline_loss - with_risk_loss

    df_trend = pd.DataFrame({
        "日期": days,
        "未开风控 (基线资损)": baseline_loss,
        "开启风控 (残余漏损)": with_risk_loss,
        "单日挽回资损": saved_loss
    })

    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(
        x=df_trend["日期"], y=df_trend["未开风控 (基线资损)"],
        mode='lines+markers', name='未开启风控 (基线流失)',
        line=dict(color='#ef4444', width=3, dash='dash')
    ))
    fig_line.add_trace(go.Scatter(
        x=df_trend["日期"], y=df_trend["开启风控 (残余漏损)"],
        mode='lines+markers', name='开启风控后 (实际流出)',
        line=dict(color='#10b981', width=3)
    ))
    fig_line.add_trace(go.Bar(
        x=df_trend["日期"], y=df_trend["单日挽回资损"],
        name='单日挽回补贴额', marker_color='rgba(59, 130, 246, 0.3)'
    ))

    fig_line.update_layout(
        height=450,
        margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis=dict(title="金额 (¥ RMB)", gridcolor="#e2e8f0"),
        xaxis=dict(gridcolor="#e2e8f0"),
        plot_bgcolor='#ffffff'
    )
    st.plotly_chart(fig_line, use_container_width=True)

# ==============================================================================
# 模块 5：系统状态与日志流水
# ==============================================================================
st.markdown("---")
st.subheader("📋 实时流批引擎运行状态")
status_col1, status_col2, status_col3 = st.columns(3)

with status_col1:
    st.success("🟢 **Spark Structured Streaming**: 运行中 (Active)")
    st.caption("Kafka Topic: `realtime_risk_events` | 消费语义: Exactly-Once")

with status_col2:
    st.info("🔵 **GraphFrames 离线图计算库**: T+0 已同步")
    st.caption("连通子图聚类算法 | PageRank 权重已落盘 Parquet")

with status_col3:
    st.warning("🟠 **PCA 异常降维模型**: 生产版本 v1.2 已挂载")
    st.caption("重构误差动态计算 | 矩阵维度: 4 x 2")
