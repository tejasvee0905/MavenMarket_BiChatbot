import os
import sys
import json
import streamlit as st
import plotly.graph_objects as go

# Add project root to path so we can import ai modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from ai.rag_chain import get_rag_chain, ask_with_history


# ═══════════════════════════════════════════════════════════════
# CHART RENDERER
# ═══════════════════════════════════════════════════════════════

CHART_COLORS = ['#4facfe', '#00f2fe', '#78ffd6', '#a78bfa', '#f472b6',
                '#fbbf24', '#34d399', '#f87171', '#60a5fa', '#c084fc']

CHART_LAYOUT = dict(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font=dict(color='#e0e8ff', family='Inter, sans-serif', size=13),
    title=dict(font=dict(size=16, color='#e0e8ff')),
    margin=dict(l=50, r=30, t=50, b=50),
    xaxis=dict(gridcolor='rgba(255,255,255,0.06)', zerolinecolor='rgba(255,255,255,0.1)'),
    yaxis=dict(gridcolor='rgba(255,255,255,0.06)', zerolinecolor='rgba(255,255,255,0.1)'),
    legend=dict(bgcolor='rgba(0,0,0,0)'),
    height=380,
)


def render_chart(chart_data: dict, key: str = None):
    """Render a Plotly chart from chart spec JSON."""
    if not chart_data:
        return

    chart_type = chart_data.get("chart_type", "bar")
    title = chart_data.get("title", "")
    data = chart_data.get("data", [])
    x_label = chart_data.get("x_label", "")
    y_label = chart_data.get("y_label", "")

    labels = [d["label"] for d in data]
    values = [d["value"] for d in data]

    fig = go.Figure()

    if chart_type == "bar":
        fig.add_trace(go.Bar(
            x=labels, y=values,
            marker=dict(
                color=CHART_COLORS[:len(labels)],
                line=dict(width=0),
                cornerradius=6,
            ),
            text=[f'{v:,.0f}' if v > 100 else f'{v:,.1f}' for v in values],
            textposition='outside',
            textfont=dict(color='#e0e8ff', size=11),
        ))

    elif chart_type == "horizontal_bar":
        fig.add_trace(go.Bar(
            y=labels, x=values, orientation='h',
            marker=dict(
                color=CHART_COLORS[:len(labels)],
                line=dict(width=0),
                cornerradius=6,
            ),
            text=[f'{v:,.0f}' if v > 100 else f'{v:,.1f}' for v in values],
            textposition='outside',
            textfont=dict(color='#e0e8ff', size=11),
        ))

    elif chart_type == "line":
        fig.add_trace(go.Scatter(
            x=labels, y=values, mode='lines+markers+text',
            line=dict(color='#4facfe', width=3),
            marker=dict(size=8, color='#4facfe', line=dict(width=2, color='#020c1b')),
            text=[f'{v:,.0f}' for v in values],
            textposition='top center',
            textfont=dict(color='#e0e8ff', size=10),
            fill='tozeroy',
            fillcolor='rgba(79, 172, 254, 0.08)',
        ))

    elif chart_type == "pie":
        fig.add_trace(go.Pie(
            labels=labels, values=values,
            marker=dict(colors=CHART_COLORS[:len(labels)], line=dict(color='#0a1628', width=2)),
            textinfo='label+percent',
            textfont=dict(size=12, color='#e0e8ff'),
            hole=0.4,
        ))

    layout = {**CHART_LAYOUT, "title": dict(text=title, font=dict(size=16, color='#e0e8ff'))}
    if x_label:
        layout["xaxis"]["title"] = x_label
    if y_label:
        layout["yaxis"]["title"] = y_label

    fig.update_layout(**layout)
    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False}, key=key)

# ── Page Config ──
st.set_page_config(
    page_title="MavenMarket BI Assistant",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ═══════════════════════════════════════════════════════════════
# MODERN GLASSMORPHISM CSS
# ═══════════════════════════════════════════════════════════════
st.markdown("""
<style>
/* ── Import Google Font ── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* ── Global ── */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* ── Animated Gradient Background ── */
.stApp {
    background: linear-gradient(135deg, #020c1b 0%, #0a1628 35%, #0d2137 65%, #091a2a 100%);
    background-size: 400% 400%;
    animation: gradientShift 15s ease infinite;
}

@keyframes gradientShift {
    0% { background-position: 0% 50%; }
    50% { background-position: 100% 50%; }
    100% { background-position: 0% 50%; }
}

/* ── Sidebar Glassmorphism ── */
section[data-testid="stSidebar"] {
    background: rgba(255, 255, 255, 0.05) !important;
    backdrop-filter: blur(20px) !important;
    -webkit-backdrop-filter: blur(20px) !important;
    border-right: 1px solid rgba(255, 255, 255, 0.1) !important;
}

section[data-testid="stSidebar"] .stMarkdown,
section[data-testid="stSidebar"] .stSlider label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] h1, section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 {
    color: #e0e0ff !important;
}

/* ── Main Header ── */
.main-header {
    text-align: center;
    padding: 1.5rem 0 1rem 0;
}
.main-header h1 {
    background: linear-gradient(135deg, #4facfe, #00f2fe, #78ffd6);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    font-size: 2.2rem;
    font-weight: 700;
    margin-bottom: 0.2rem;
}
.main-header p {
    color: rgba(255,255,255,0.5);
    font-size: 0.95rem;
    font-weight: 300;
}

/* ── Chat Messages ── */
.stChatMessage {
    background: rgba(255, 255, 255, 0.06) !important;
    backdrop-filter: blur(12px) !important;
    -webkit-backdrop-filter: blur(12px) !important;
    border: 1px solid rgba(255, 255, 255, 0.08) !important;
    border-radius: 16px !important;
    margin-bottom: 1rem !important;
    padding: 1rem !important;
    transition: all 0.3s ease !important;
}

.stChatMessage:hover {
    background: rgba(255, 255, 255, 0.09) !important;
    border-color: rgba(255, 255, 255, 0.15) !important;
    transform: translateY(-1px);
    box-shadow: 0 8px 32px rgba(79, 172, 254, 0.1);
}

/* ── User message accent ── */
.stChatMessage[data-testid="stChatMessage"]:has([data-testid="stChatMessageAvatarUser"]) {
    border-left: 3px solid rgba(79, 172, 254, 0.6) !important;
}

/* ── Assistant message accent ── */
.stChatMessage[data-testid="stChatMessage"]:has([data-testid="stChatMessageAvatarAssistant"]) {
    border-left: 3px solid rgba(0, 242, 254, 0.5) !important;
}

/* ── Chat text color ── */
.stChatMessage p, .stChatMessage li, .stChatMessage td, .stChatMessage th,
.stChatMessage h1, .stChatMessage h2, .stChatMessage h3, .stChatMessage h4 {
    color: #e8e8f0 !important;
}

.stChatMessage code {
    background: rgba(255,255,255,0.1) !important;
    color: #b8b8ff !important;
    border-radius: 4px;
    padding: 2px 6px;
}

/* ── Tables in chat ── */
.stChatMessage table {
    border-collapse: collapse;
    width: 100%;
}
.stChatMessage th {
    background: rgba(79, 172, 254, 0.2) !important;
    padding: 8px 12px !important;
    border-bottom: 1px solid rgba(255,255,255,0.15) !important;
}
.stChatMessage td {
    padding: 6px 12px !important;
    border-bottom: 1px solid rgba(255,255,255,0.05) !important;
}

/* ── Chat Input ── */
.stChatInput {
    border-radius: 16px !important;
    overflow: hidden;
}
.stChatInput > div {
    background: rgba(255, 255, 255, 0.07) !important;
    backdrop-filter: blur(12px) !important;
    border: 1px solid rgba(255, 255, 255, 0.12) !important;
    border-radius: 16px !important;
}
.stChatInput textarea {
    color: #e8e8f0 !important;
    caret-color: #4facfe !important;
}
.stChatInput textarea::placeholder {
    color: rgba(255,255,255,0.35) !important;
}

/* ── Expander (Sources) ── */
.streamlit-expanderHeader {
    background: rgba(255,255,255,0.04) !important;
    border-radius: 10px !important;
    color: rgba(255,255,255,0.6) !important;
    font-size: 0.85rem !important;
}
details {
    background: rgba(255,255,255,0.03) !important;
    border: 1px solid rgba(255,255,255,0.06) !important;
    border-radius: 10px !important;
}

/* ── Spinner ── */
.stSpinner > div {
    color: #4facfe !important;
}

/* ── Slider styling ── */
.stSlider > div > div > div {
    background: linear-gradient(90deg, #4facfe, #00f2fe) !important;
}

/* Hide min/max labels on sliders */
.stSlider [data-testid="stTickBarMin"],
.stSlider [data-testid="stTickBarMax"],
.stSlider div[data-testid="stTickBarMin"],
.stSlider div[data-testid="stTickBarMax"] {
    display: none !important;
    visibility: hidden !important;
    height: 0 !important;
    overflow: hidden !important;
}

/* Force hide all slider range value elements */
section[data-testid="stSidebar"] .stSlider > div > div:last-child,
section[data-testid="stSidebar"] .stSlider [data-baseweb="slider"] + div {
    display: none !important;
}

/* Slider track */
.stSlider [data-baseweb="slider"] [role="slider"] {
    background: #4facfe !important;
    border-color: #4facfe !important;
    box-shadow: 0 0 8px rgba(79, 172, 254, 0.4) !important;
    width: 16px !important;
    height: 16px !important;
}

/* Slider track background */
.stSlider [data-baseweb="slider"] > div:first-child {
    background: rgba(255, 255, 255, 0.08) !important;
    border-radius: 4px !important;
}

/* Slider active track */
.stSlider [data-baseweb="slider"] > div > div:first-child {
    background: linear-gradient(90deg, #4facfe, #00f2fe) !important;
    border-radius: 4px !important;
}

/* Slider current value */
.stSlider [data-testid="stThumbValue"] {
    color: #4facfe !important;
    font-size: 0.8rem !important;
    font-weight: 500 !important;
}

/* Slider tooltip bubble */
.stSlider [data-baseweb="tooltip"],
.stSlider [data-baseweb="tooltip"] div,
[data-baseweb="tooltip"] [role="tooltip"],
[data-baseweb="tooltip"] [role="tooltip"] div {
    background-color: rgba(10, 22, 40, 0.95) !important;
    color: #e0e8ff !important;
    border: 1px solid rgba(79, 172, 254, 0.3) !important;
    border-radius: 8px !important;
    font-size: 0.82rem !important;
    font-weight: 500 !important;
    backdrop-filter: blur(10px) !important;
}

/* ── Button ── */
.stButton > button {
    background: linear-gradient(135deg, #4facfe 0%, #0083B0 100%) !important;
    color: white !important;
    border: none !important;
    border-radius: 12px !important;
    padding: 0.5rem 1.5rem !important;
    font-weight: 500 !important;
    transition: all 0.3s ease !important;
    width: 100%;
}
.stButton > button:hover {
    transform: translateY(-2px) !important;
    box-shadow: 0 6px 20px rgba(79, 172, 254, 0.3) !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar {
    width: 6px;
}
::-webkit-scrollbar-track {
    background: transparent;
}
::-webkit-scrollbar-thumb {
    background: rgba(255,255,255,0.15);
    border-radius: 3px;
}
::-webkit-scrollbar-thumb:hover {
    background: rgba(255,255,255,0.25);
}

/* ── Sidebar divider ── */
section[data-testid="stSidebar"] hr {
    border-color: rgba(255,255,255,0.1) !important;
}

/* ── Status pill badges ── */
.status-pill {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 500;
}
.status-online {
    background: rgba(46, 204, 113, 0.15);
    color: #2ecc71;
    border: 1px solid rgba(46, 204, 113, 0.3);
}

/* ── Tip cards ──*/
.tip-card {
    background: rgba(255,255,255,0.05);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 1rem 1.2rem;
    margin: 0.5rem 0;
    transition: all 0.3s ease;
}
.tip-card:hover {
    background: rgba(255,255,255,0.08);
    border-color: rgba(79, 172, 254, 0.3);
}
.tip-card .tip-icon {
    font-size: 1.3rem;
    margin-right: 0.5rem;
}
.tip-card .tip-text {
    color: rgba(255,255,255,0.7);
    font-size: 0.88rem;
}
.tip-card .tip-title {
    color: #e0e0ff;
    font-weight: 500;
    font-size: 0.95rem;
}
</style>
""", unsafe_allow_html=True)

# ── Sidebar ──
with st.sidebar:
    st.markdown('<div style="text-align:center; padding: 0.5rem 0;">'
                '<span style="font-size:2rem;">📊</span><br>'
                '<span style="font-size:1.1rem; font-weight:600; color:#e0e0ff;">MavenMarket AI</span>'
                '</div>', unsafe_allow_html=True)
    st.markdown("---")

    # ── Model Parameters ──
    st.markdown('<p style="color:#b8b8ff; font-weight:600; font-size:0.9rem; margin-bottom:0.5rem;">⚙️ Model Parameters</p>', unsafe_allow_html=True)
    temperature = st.slider(
        "Temperature",
        min_value=0.0, max_value=2.0, value=0.0, step=0.1,
        help="0 = deterministic & focused, 2 = creative & random"
    )
    max_tokens = st.slider(
        "Max Tokens",
        min_value=64, max_value=4096, value=1024, step=64,
        help="Maximum response length (~0.75 words per token)"
    )
    top_p = st.slider(
        "Top P",
        min_value=0.0, max_value=1.0, value=1.0, step=0.05,
        help="Nucleus sampling — lower = more focused"
    )
    top_k = st.slider(
        "Retrieval Chunks",
        min_value=1, max_value=20, value=15, step=1,
        help="Number of knowledge chunks to retrieve"
    )

    st.markdown("---")
    st.markdown('<p style="color:#b8b8ff; font-weight:600; font-size:0.9rem;">💡 Try asking</p>', unsafe_allow_html=True)
    st.markdown(
        '<div style="color:rgba(255,255,255,0.55); font-size:0.85rem; line-height:1.8;">'
        '• What is the total revenue?<br>'
        '• How many married customers in USA?<br>'
        '• Compare revenue by country<br>'
        '• Which store has the highest profit?<br>'
        '• Revenue trend 1997 vs 1998<br>'
        '• What does Profit Margin measure?<br>'
        '• Average product weight by brand?'
        '</div>', unsafe_allow_html=True
    )
    st.markdown("---")
    if st.button("🗑️ Clear Chat"):
        st.session_state.messages = []
        st.session_state.chat_history = []
        st.rerun()

    st.markdown(
        '<div style="text-align:center; padding-top:0.5rem;">'
        '<span class="status-pill status-online">● Online</span>'
        '<p style="color:rgba(255,255,255,0.3); font-size:0.72rem; margin-top:0.5rem;">'
        'GPT-4o · LangChain · FAISS<br>119 insights + Live CSV</p>'
        '</div>', unsafe_allow_html=True
    )

# ── Init / rebuild RAG chain when params change ──
if "messages" not in st.session_state:
    st.session_state.messages = []

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

current_params = (temperature, max_tokens, top_p, top_k)
if "rag_params" not in st.session_state or st.session_state.rag_params != current_params:
    with st.spinner("Loading knowledge base..."):
        st.session_state.rag_components = get_rag_chain(
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            top_k=top_k,
        )
        st.session_state.rag_params = current_params

# ── Main Header (shown only when chat is empty) ──
if not st.session_state.messages:
    st.markdown(
        '<div class="main-header">'
        '<h1>MavenMarket BI Assistant</h1>'
        '<p>Ask anything about your dashboard — revenue, customers, stores, products & more</p>'
        '</div>', unsafe_allow_html=True
    )

    # Quick-start cards
    cols = st.columns(3)
    tips = [
        ("📈", "KPI Overview", "What are the overall KPIs?"),
        ("🌎", "Country Analysis", "Break down revenue by country"),
        ("🏪", "Store Performance", "Which store performs best?"),
    ]
    for col, (icon, title, text) in zip(cols, tips):
        with col:
            st.markdown(
                f'<div class="tip-card">'
                f'<span class="tip-icon">{icon}</span>'
                f'<span class="tip-title">{title}</span><br>'
                f'<span class="tip-text">{text}</span>'
                f'</div>', unsafe_allow_html=True
            )

# ── Display chat history ──
for i, msg in enumerate(st.session_state.messages):
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg["role"] == "assistant":
            if "chart" in msg and msg["chart"]:
                render_chart(msg["chart"], key=f"chart_history_{i}")
            if "sources" in msg:
                with st.expander("📎 Sources"):
                    for src in msg["sources"]:
                        st.caption(f"• {src}")

# ── Chat input ──
if question := st.chat_input("Ask about MavenMarket dashboard..."):
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("🔍 Analyzing..."):
            result = ask_with_history(
                st.session_state.rag_components,
                question,
                chat_history=st.session_state.chat_history,
            )
            answer = result["answer"]
            sources = result["sources"]
            chart = result.get("chart")

        st.markdown(answer)
        if chart:
            render_chart(chart, key="chart_new")
        if sources:
            with st.expander("📎 Sources"):
                for src in sources:
                    st.caption(f"• {src}")

    st.session_state.chat_history.append((question, answer))
    st.session_state.messages.append({
        "role": "assistant",
        "content": answer,
        "chart": chart,
        "sources": sources
    })
