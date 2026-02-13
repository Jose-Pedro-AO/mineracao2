"""
dashboard.py — Dashboard Interativa de Mineração de Dados
Streamlit app com teste de modelo em tempo real e visualizações interativas.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os

# Adicionar src ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from model_trainer import (
    load_data, prepare_classification_data, train_models,
    predict_satisfaction, compute_rfm,
    FEATURE_COLS, FEATURE_LABELS_PT
)

# ─── Configuração da Página 
st.set_page_config(
    page_title="Mineração de Dados — Dashboard",
    page_icon="MD",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── CSS 
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

/* Global */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

/* Hide default streamlit branding and sidebar */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
section[data-testid="stSidebar"] { display: none; }

/* Main background */
.stApp {
    background: linear-gradient(135deg, #0f0c29 0%, #1a1a3e 40%, #24243e 100%);
    padding-bottom: 100px;
}

/* Top Navigation Bar */
.top-nav {
    background: linear-gradient(135deg, rgba(20, 20, 40, 0.95) 0%, rgba(28, 28, 58, 0.95) 100%);
    border-bottom: 1px solid rgba(99, 102, 241, 0.25);
    border-radius: 16px;
    padding: 50px 24px;
    margin: -1rem -1rem 24px -1rem;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 16px;
    backdrop-filter: blur(20px);
}
.top-nav-title {
    font-size: 1.3rem;
    font-weight: 700;
    background: linear-gradient(135deg, #6366f1, #ec4899);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-right: 32px;
    white-space: nowrap;
}
.nav-btn {
    display: inline-block;
    padding: 8px 22px;
    border-radius: 10px;
    color: #a5b4fc;
    font-weight: 600;
    font-size: 0.9rem;
    text-decoration: none;
    cursor: pointer;
    transition: all 0.25s ease;
    border: 1px solid transparent;
    white-space: nowrap;
}
.nav-btn:hover {
    background: rgba(99, 102, 241, 0.15);
    border-color: rgba(99, 102, 241, 0.3);
    color: #c7d2fe;
}
.nav-btn.active {
    background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
    color: white;
    box-shadow: 0 4px 15px rgba(99, 102, 241, 0.3);
    border-color: transparent;
}

/* Custom Footer */
.custom-footer {
    position: fixed;
    bottom: 0;
    left: 0;
    right: 0;
    background: linear-gradient(135deg, rgba(20, 20, 40, 0.97) 0%, rgba(28, 28, 58, 0.97) 100%);
    border-top: 1px solid rgba(99, 102, 241, 0.25);
    backdrop-filter: blur(20px);
    padding: 10px 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    z-index: 999;
    font-family: 'Inter', sans-serif;
}
.footer-stats {
    display: flex;
    gap: 24px;
    align-items: center;
}
.footer-stat {
    color: #94a3b8;
    font-size: 0.78rem;
}
.footer-stat strong {
    color: #c7d2fe;
    font-weight: 600;
}
.footer-credit {
    color: #475569;
    font-size: 0.72rem;
}



/* Metric cards */
div[data-testid="stMetric"] {
    background: linear-gradient(135deg, rgba(99, 102, 241, 0.15) 0%, rgba(168, 85, 247, 0.1) 100%);
    border: 1px solid rgba(99, 102, 241, 0.25);
    border-radius: 16px;
    padding: 20px;
    backdrop-filter: blur(10px);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}
div[data-testid="stMetric"]:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 32px rgba(99, 102, 241, 0.2);
}

div[data-testid="stMetric"] label {
    color: #a5b4fc !important;
    font-weight: 600;
    text-transform: uppercase;
    font-size: 0.75rem !important;
    letter-spacing: 0.5px;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #f1f5f9 !important;
    font-weight: 700;
    font-size: 1.8rem !important;
}

/* Headers */
h1, h2, h3 {
    color: #f1f5f9 !important;
    font-weight: 700 !important;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
    background: rgba(99, 102, 241, 0.08);
    border-radius: 12px;
    padding: 4px;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 10px;
    color: #a5b4fc;
    font-weight: 600;
    padding: 10px 20px;
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%) !important;
    color: white !important;
}

/* Buttons */
.stButton > button {
    background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
    color: white;
    border: none;
    border-radius: 12px;
    padding: 12px 28px;
    font-weight: 600;
    font-size: 1rem;
    transition: all 0.3s ease;
    box-shadow: 0 4px 15px rgba(99, 102, 241, 0.3);
}
.stButton > button:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 25px rgba(99, 102, 241, 0.4);
}

/* Selectbox / Slider */
.stSelectbox label, .stSlider label, .stNumberInput label {
    color: #c7d2fe !important;
    font-weight: 500;
}

/* Expander */
.streamlit-expanderHeader {
    background: rgba(99, 102, 241, 0.1);
    border-radius: 12px;
    color: #c7d2fe !important;
    font-weight: 600;
}

/* Divider */
hr {
    border-color: rgba(99, 102, 241, 0.2) !important;
}

/* DataFrame */
.stDataFrame {
    border-radius: 12px;
    overflow: hidden;
}

/* Success/Warning/Error boxes */
.stAlert {
    border-radius: 12px;
}

/* Custom hero card */
.hero-card {
    background: linear-gradient(135deg, rgba(99, 102, 241, 0.2) 0%, rgba(168, 85, 247, 0.15) 50%, rgba(236, 72, 153, 0.1) 100%);
    border: 1px solid rgba(99, 102, 241, 0.3);
    border-radius: 20px;
    padding: 32px;
    margin-bottom: 24px;
    backdrop-filter: blur(20px);
}

.prediction-card {
    border-radius: 16px;
    padding: 24px;
    text-align: center;
    backdrop-filter: blur(10px);
    transition: transform 0.2s ease;
}
.prediction-card:hover {
    transform: translateY(-3px);
}
.pred-satisfeito {
    background: linear-gradient(135deg, rgba(34, 197, 94, 0.2) 0%, rgba(16, 185, 129, 0.15) 100%);
    border: 1px solid rgba(34, 197, 94, 0.3);
}
.pred-insatisfeito {
    background: linear-gradient(135deg, rgba(239, 68, 68, 0.2) 0%, rgba(244, 63, 94, 0.15) 100%);
    border: 1px solid rgba(239, 68, 68, 0.3);
}
</style>
""", unsafe_allow_html=True)

PLOTLY_LAYOUT = dict(
    template='plotly_dark',
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family='Inter, sans-serif', color='#e2e8f0'),
    margin=dict(l=40, r=40, t=50, b=40),
)

COLORS = {
    'primary': '#6366f1',
    'secondary': '#8b5cf6',
    'accent': '#ec4899',
    'success': '#22c55e',
    'warning': '#f59e0b',
    'danger': '#ef4444',
    'info': '#06b6d4',
    'gradient': ['#6366f1', '#8b5cf6', '#a855f7', '#c084fc', '#d8b4fe'],
    'clusters': ['#ef4444', '#3b82f6', '#22c55e', '#f59e0b'],
    'cluster_names': {
        'VIP': '#f59e0b',
        'Novos / Promissores': '#22c55e',
        'Em Risco': '#ef4444',
        'Esporádicos': '#3b82f6',
    }
}


# ─── Cache: carregar dados e treinar modelos
@st.cache_data(show_spinner=False)
def init_data():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dados = load_data(base)
    return dados

@st.cache_resource(show_spinner=False)
def init_models(_dados):
    df_clean = prepare_classification_data(_dados)
    models = train_models(df_clean)
    return models, df_clean

@st.cache_data(show_spinner=False)
def init_rfm(_dados):
    return compute_rfm(_dados)


# ─── Inicialização ──────────────────────────────────────────────────────────
with st.spinner("Carregando dados e treinando modelos..."):
    dados = init_data()
    models_dict, df_clean = init_models(dados)
    rfm_dict = init_rfm(dados)

# ─── Navegação no Topo ────────────────────────────────────────────────────────
PAGES = ["Teste do Modelo", "Gráficos — Classificação", "Segmentação de Clientes"]
if 'page' not in st.session_state:
    st.session_state.page = PAGES[0]

def set_page(p):
    st.session_state.page = p

nav_buttons = ""
for p in PAGES:
    active_cls = "active" if p == st.session_state.page else ""
    nav_buttons += f'<span class="nav-btn {active_cls}" id="nav-{p}">{p}</span>'

st.markdown(f"""
<div class="top-nav">
    <span class="top-nav-title">Mineração de Dados</span>
</div>
""", unsafe_allow_html=True)

# Navigation columns with Streamlit buttons (functional)
nav_cols = st.columns(len(PAGES))
for i, p in enumerate(PAGES):
    with nav_cols[i]:
        btn_type = "primary" if p == st.session_state.page else "secondary"
        if st.button(p, key=f"nav_{p}", use_container_width=True, type=btn_type):
            st.session_state.page = p
            st.rerun()

page = st.session_state.page


# PÁGINA 1
if page == "Teste do Modelo":
    
    st.markdown("""
    <div class="hero-card">
        <h1 style="margin:0 0 8px; font-size:2rem;">Teste do Modelo em Tempo Real</h1>
        <p style="color:#94a3b8; margin:0; font-size:1rem;">
            Insira dados de um pedido para prever a satisfação do cliente.
            Os modelos Logistic Regression e Random Forest fazem a previsão simultaneamente.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Métricas de desempenho no topo
    m_lr = models_dict['metrics']['lr']
    m_rf = models_dict['metrics']['rf']
    
    st.markdown("### Desempenho dos Modelos no Conjunto de Teste")
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("LR Accuracy", f"{m_lr['accuracy']:.1%}")
    col2.metric("LR F1-Score", f"{m_lr['f1']:.3f}")
    col3.metric("LR ROC-AUC", f"{m_lr['roc_auc']:.3f}")
    col4.metric("RF Accuracy", f"{m_rf['accuracy']:.1%}")
    col5.metric("RF F1-Score", f"{m_rf['f1']:.3f}")
    col6.metric("RF ROC-AUC", f"{m_rf['roc_auc']:.3f}")
    
    st.divider()
    
    # Duas colunas: inputs e resultado
    col_input, col_result = st.columns([3, 2])
    
    with col_input:
        st.markdown("### Dados do Pedido")
        
        # Botão para preencher com amostra aleatória
        if st.button("Preencher com Amostra Aleatória do Teste", use_container_width=True):
            sample = models_dict['X_test'].sample(1, random_state=np.random.randint(10000))
            for col_name in FEATURE_COLS:
                st.session_state[f'input_{col_name}'] = float(sample[col_name].values[0])
        
        # Estatísticas para ajustar ranges
        stats = df_clean[FEATURE_COLS].describe()
        
        # Grid de inputs (3 colunas)
        input_values = {}
        cols = st.columns(3)
        for idx, feat in enumerate(FEATURE_COLS):
            col = cols[idx % 3]
            label = FEATURE_LABELS_PT.get(feat, feat)
            min_val = float(stats.loc['min', feat])
            max_val = float(stats.loc['max', feat])
            mean_val = float(stats.loc['mean', feat])
            default = st.session_state.get(f'input_{feat}', mean_val)
            
            if feat == 'is_late':
                input_values[feat] = float(col.selectbox(
                    label, [0, 1],
                    index=int(default) if default in [0, 1] else 0,
                    key=f'widget_{feat}'
                ))
            elif feat == 'payment_installments':
                input_values[feat] = float(col.number_input(
                    label, min_value=1, max_value=24,
                    value=int(default), step=1,
                    key=f'widget_{feat}'
                ))
            else:
                input_values[feat] = col.number_input(
                    label,
                    min_value=min_val,
                    max_value=max_val * 1.5,
                    value=round(default, 2),
                    step=round((max_val - min_val) / 100, 2) or 0.01,
                    key=f'widget_{feat}'
                )
    
    with col_result:
        st.markdown("### Previsão")
        
        if st.button("Prever Satisfação", use_container_width=True, type="primary"):
            result = predict_satisfaction(models_dict, input_values)
            
            # Logistic Regression
            lr_r = result['lr']
            rf_r = result['rf']
            
            st.markdown("#### Logistic Regression")
            if lr_r['prediction'] == 1:
                st.markdown(f"""
                <div class="prediction-card pred-satisfeito">
                    <h2 style="color:#22c55e; margin:0;">Satisfeito</h2>
                    <p style="color:#86efac; font-size:2rem; font-weight:800; margin:8px 0;">
                        {lr_r['prob_satisfeito']:.1%}
                    </p>
                    <p style="color:#94a3b8; font-size:0.85rem;">Probabilidade de satisfação</p>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="prediction-card pred-insatisfeito">
                    <h2 style="color:#ef4444; margin:0;">Insatisfeito</h2>
                    <p style="color:#fca5a5; font-size:2rem; font-weight:800; margin:8px 0;">
                        {lr_r['prob_insatisfeito']:.1%}
                    </p>
                    <p style="color:#94a3b8; font-size:0.85rem;">Probabilidade de insatisfação</p>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("")
            st.markdown("#### Random Forest")
            if rf_r['prediction'] == 1:
                st.markdown(f"""
                <div class="prediction-card pred-satisfeito">
                    <h2 style="color:#22c55e; margin:0;">Satisfeito</h2>
                    <p style="color:#86efac; font-size:2rem; font-weight:800; margin:8px 0;">
                        {rf_r['prob_satisfeito']:.1%}
                    </p>
                    <p style="color:#94a3b8; font-size:0.85rem;">Probabilidade de satisfação</p>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class="prediction-card pred-insatisfeito">
                    <h2 style="color:#ef4444; margin:0;">Insatisfeito</h2>
                    <p style="color:#fca5a5; font-size:2rem; font-weight:800; margin:8px 0;">
                        {rf_r['prob_insatisfeito']:.1%}
                    </p>
                    <p style="color:#94a3b8; font-size:0.85rem;">Probabilidade de insatisfação</p>
                </div>
                """, unsafe_allow_html=True)
            
            # Barra comparativa
            st.markdown("")
            st.markdown("#### Comparação de Probabilidades")
            fig_comp = go.Figure()
            fig_comp.add_trace(go.Bar(
                name='Logistic Regression',
                x=['Insatisfeito', 'Satisfeito'],
                y=[lr_r['prob_insatisfeito'], lr_r['prob_satisfeito']],
                marker_color=[COLORS['danger'], COLORS['success']],
                opacity=0.8,
                text=[f"{lr_r['prob_insatisfeito']:.1%}", f"{lr_r['prob_satisfeito']:.1%}"],
                textposition='auto'
            ))
            fig_comp.add_trace(go.Bar(
                name='Random Forest',
                x=['Insatisfeito', 'Satisfeito'],
                y=[rf_r['prob_insatisfeito'], rf_r['prob_satisfeito']],
                marker_color=[COLORS['accent'], COLORS['info']],
                opacity=0.8,
                text=[f"{rf_r['prob_insatisfeito']:.1%}", f"{rf_r['prob_satisfeito']:.1%}"],
                textposition='auto'
            ))
            fig_comp.update_layout(
                **PLOTLY_LAYOUT,
                barmode='group',
                height=300,
                title=None,
                yaxis_title='Probabilidade',
                showlegend=True,
                legend=dict(orientation='h', y=1.12)
            )
            st.plotly_chart(fig_comp, use_container_width=True)

        else:
            st.markdown("""
            <div style="text-align:center; padding:60px 20px; color:#64748b;">
                <p style="font-size:1.2rem; margin:0; color:#a5b4fc;">—</p>
                <p style="font-size:1.1rem; font-weight:500;">
                    Preencha os dados e clique em<br><strong style="color:#a5b4fc;">Prever Satisfação</strong>
                </p>
            </div>
            """, unsafe_allow_html=True)


# PÁGINA 2: GRÁFICOS — CLASSIFICAÇÃO
elif page == "Gráficos — Classificação":
    
    st.markdown("""
    <div class="hero-card">
        <h1 style="margin:0 0 8px; font-size:2rem;">Gráficos Resumo — Classificação</h1>
        <p style="color:#94a3b8; margin:0;">
            Visualizações interativas dos resultados do modelo de previsão de satisfação.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Matriz de Confusão",
        "Curva ROC",
        "Feature Importance",
        "Distribuições",
        "Correlação"
    ])
    
    # ── Tab 1: Matriz de Confusão 
    with tab1:
        st.markdown("### Matrizes de Confusão — Previsões vs Resultados Reais")
        
        col1, col2 = st.columns(2)
        
        labels = ['Insatisfeito', 'Satisfeito']
        
        with col1:
            cm_lr = models_dict['cm_lr']
            fig_cm_lr = go.Figure(data=go.Heatmap(
                z=cm_lr, x=labels, y=labels,
                text=cm_lr, texttemplate="%{text}",
                textfont=dict(size=20, color='white'),
                colorscale=[[0, '#1e1b4b'], [0.5, '#4338ca'], [1, '#818cf8']],
                showscale=False,
                hovertemplate='Real: %{y}<br>Previsto: %{x}<br>Contagem: %{z}<extra></extra>'
            ))
            fig_cm_lr.update_layout(
                **PLOTLY_LAYOUT,
                title=dict(text='Logistic Regression', font=dict(size=16)),
                xaxis_title='Previsto',
                yaxis_title='Real',
                height=400,
                yaxis=dict(autorange='reversed')
            )
            st.plotly_chart(fig_cm_lr, use_container_width=True)
        
        with col2:
            cm_rf = models_dict['cm_rf']
            fig_cm_rf = go.Figure(data=go.Heatmap(
                z=cm_rf, x=labels, y=labels,
                text=cm_rf, texttemplate="%{text}",
                textfont=dict(size=20, color='white'),
                colorscale=[[0, '#052e16'], [0.5, '#15803d'], [1, '#4ade80']],
                showscale=False,
                hovertemplate='Real: %{y}<br>Previsto: %{x}<br>Contagem: %{z}<extra></extra>'
            ))
            fig_cm_rf.update_layout(
                **PLOTLY_LAYOUT,
                title=dict(text='Random Forest', font=dict(size=16)),
                xaxis_title='Previsto',
                yaxis_title='Real',
                height=400,
                yaxis=dict(autorange='reversed')
            )
            st.plotly_chart(fig_cm_rf, use_container_width=True)
        
        # Métricas detalhadas
        st.markdown("### Métricas Detalhadas")
        m_lr = models_dict['metrics']['lr']
        m_rf = models_dict['metrics']['rf']
        
        metrics_df = pd.DataFrame({
            'Métrica': ['Accuracy', 'Precision', 'Recall', 'F1-Score', 'ROC-AUC', 'CV ROC-AUC (média ± std)'],
            'Logistic Regression': [
                f"{m_lr['accuracy']:.4f}", f"{m_lr['precision']:.4f}", f"{m_lr['recall']:.4f}",
                f"{m_lr['f1']:.4f}", f"{m_lr['roc_auc']:.4f}", f"{m_lr['cv_mean']:.4f} ± {m_lr['cv_std']:.4f}"
            ],
            'Random Forest': [
                f"{m_rf['accuracy']:.4f}", f"{m_rf['precision']:.4f}", f"{m_rf['recall']:.4f}",
                f"{m_rf['f1']:.4f}", f"{m_rf['roc_auc']:.4f}", f"{m_rf['cv_mean']:.4f} ± {m_rf['cv_std']:.4f}"
            ]
        })
        st.dataframe(metrics_df, use_container_width=True, hide_index=True)
    
    # ── Tab 2: Curva ROC 
    with tab2:
        st.markdown("### Curva ROC — Comparação de Modelos")
        
        fig_roc = go.Figure()
        fig_roc.add_trace(go.Scatter(
            x=models_dict['fpr_lr'], y=models_dict['tpr_lr'],
            name=f"Logistic Regression (AUC = {m_lr['roc_auc']:.3f})",
            line=dict(color=COLORS['primary'], width=3),
            fill='tonexty',
            fillcolor='rgba(99, 102, 241, 0.1)',
            hovertemplate='FPR: %{x:.3f}<br>TPR: %{y:.3f}<extra>LR</extra>'
        ))
        fig_roc.add_trace(go.Scatter(
            x=models_dict['fpr_rf'], y=models_dict['tpr_rf'],
            name=f"Random Forest (AUC = {m_rf['roc_auc']:.3f})",
            line=dict(color=COLORS['success'], width=3),
            hovertemplate='FPR: %{x:.3f}<br>TPR: %{y:.3f}<extra>RF</extra>'
        ))
        fig_roc.add_trace(go.Scatter(
            x=[0, 1], y=[0, 1],
            name='Random (AUC = 0.500)',
            line=dict(color='#475569', width=1, dash='dash'),
        ))
        fig_roc.update_layout(
            **PLOTLY_LAYOUT,
            title=dict(text='Curva ROC — Comparação', font=dict(size=18)),
            xaxis_title='Taxa de Falsos Positivos (FPR)',
            yaxis_title='Taxa de Verdadeiros Positivos (TPR)',
            height=550,
            legend=dict(x=0.55, y=0.1, bgcolor='rgba(0,0,0,0.5)', bordercolor='rgba(99,102,241,0.3)', borderwidth=1),
            xaxis=dict(gridcolor='rgba(148,163,184,0.1)'),
            yaxis=dict(gridcolor='rgba(148,163,184,0.1)'),
        )
        st.plotly_chart(fig_roc, use_container_width=True)
    
    # ── Tab 3: Feature Importance ────────────────────────────────────────────
    with tab3:
        st.markdown("### Importância das Features — Random Forest")
        
        importances = models_dict['feature_importances']
        labels_pt = [FEATURE_LABELS_PT.get(f, f) for f in importances.index]
        
        fig_fi = go.Figure(go.Bar(
            x=importances.values,
            y=labels_pt,
            orientation='h',
            marker=dict(
                color=importances.values,
                colorscale=[[0, '#312e81'], [0.3, '#4338ca'], [0.6, '#6366f1'], [0.8, '#a855f7'], [1, '#ec4899']],
                line=dict(width=0),
            ),
            text=[f'{v:.4f}' for v in importances.values],
            textposition='outside',
            textfont=dict(color='#c7d2fe'),
            hovertemplate='%{y}: %{x:.4f}<extra></extra>'
        ))
        fig_fi.update_layout(
            **PLOTLY_LAYOUT,
            title=dict(text='Feature Importance (Random Forest)', font=dict(size=18)),
            xaxis_title='Importância',
            height=550,
            xaxis=dict(gridcolor='rgba(148,163,184,0.1)'),
            yaxis=dict(gridcolor='rgba(148,163,184,0.1)'),
        )
        st.plotly_chart(fig_fi, use_container_width=True)
        
        st.info("**Insight:** As features relacionadas à entrega (delivery_delay, delivery_days) são as mais importantes para prever a satisfação do cliente.")
    
    # ── Tab 4: Distribuições ─────────────────────────────────────────────────
    with tab4:
        st.markdown("### Distribuição das Variáveis")
        
        selected_feat = st.selectbox(
            "Selecione a variável:",
            FEATURE_COLS,
            format_func=lambda x: FEATURE_LABELS_PT.get(x, x),
            key='dist_feat'
        )
        
        show_by_class = st.checkbox("Separar por classe (Satisfeito / Insatisfeito)", value=True)
        
        if show_by_class:
            fig_dist = go.Figure()
            for label_val, name, color in [(0, 'Insatisfeito', COLORS['danger']), (1, 'Satisfeito', COLORS['success'])]:
                subset = df_clean[df_clean['satisfeito'] == label_val][selected_feat]
                fig_dist.add_trace(go.Histogram(
                    x=subset, name=name,
                    marker_color=color, opacity=0.7,
                    nbinsx=50,
                    hovertemplate=f'{name}<br>{FEATURE_LABELS_PT.get(selected_feat, selected_feat)}: ' + '%{x}<br>Contagem: %{y}<extra></extra>'
                ))
            fig_dist.update_layout(barmode='overlay')
        else:
            fig_dist = go.Figure(go.Histogram(
                x=df_clean[selected_feat],
                marker_color=COLORS['primary'],
                opacity=0.8,
                nbinsx=50,
            ))
        
        fig_dist.update_layout(
            **PLOTLY_LAYOUT,
            title=dict(text=f'Distribuição: {FEATURE_LABELS_PT.get(selected_feat, selected_feat)}', font=dict(size=18)),
            xaxis_title=FEATURE_LABELS_PT.get(selected_feat, selected_feat),
            yaxis_title='Contagem',
            height=450,
            xaxis=dict(gridcolor='rgba(148,163,184,0.1)'),
            yaxis=dict(gridcolor='rgba(148,163,184,0.1)'),
        )
        st.plotly_chart(fig_dist, use_container_width=True)
        
        # Estatísticas descritivas
        with st.expander("Estatísticas Descritivas"):
            desc = df_clean[selected_feat].describe().round(3)
            st.dataframe(desc.to_frame().T, use_container_width=True)
    
    # ── Tab 5: Correlação ────────────────────────────────────────────────────
    with tab5:
        st.markdown("### Matriz de Correlação")
        
        selected_corr_feats = st.multiselect(
            "Selecione as variáveis para a matriz de correlação:",
            FEATURE_COLS + ['satisfeito'],
            default=FEATURE_COLS[:6] + ['satisfeito'],
            format_func=lambda x: FEATURE_LABELS_PT.get(x, x)
        )
        
        if len(selected_corr_feats) >= 2:
            corr = df_clean[selected_corr_feats].corr().round(3)
            labels_corr = [FEATURE_LABELS_PT.get(f, f) for f in selected_corr_feats]
            
            fig_corr = go.Figure(data=go.Heatmap(
                z=corr.values,
                x=labels_corr,
                y=labels_corr,
                text=corr.values,
                texttemplate='%{text:.2f}',
                textfont=dict(size=11),
                colorscale=[[0, '#ef4444'], [0.5, '#1e1b4b'], [1, '#22c55e']],
                zmid=0,
                hovertemplate='%{x} × %{y}<br>Correlação: %{z:.3f}<extra></extra>'
            ))
            fig_corr.update_layout(
                **PLOTLY_LAYOUT,
                title=dict(text='Matriz de Correlação', font=dict(size=18)),
                height=550,
                yaxis=dict(autorange='reversed'),
            )
            st.plotly_chart(fig_corr, use_container_width=True)
        else:
            st.warning("Selecione pelo menos 2 variáveis.")


# PÁGINA 3: SEGMENTAÇÃO DE CLIENTES
elif page == "Segmentação de Clientes":
    
    st.markdown("""
    <div class="hero-card">
        <h1 style="margin:0 0 8px; font-size:2rem;">Segmentação de Clientes (RFM + K-Means)</h1>
        <p style="color:#94a3b8; margin:0;">
            Análise de segmentos baseada em <strong>Recência</strong>, <strong>Frequência</strong> e valor <strong>Monetário</strong>.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    rfm = rfm_dict['rfm']
    perfil = rfm_dict['perfil']
    
    # Métricas resumo
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Clientes", f"{len(rfm):,}")
    col2.metric("Nº Segmentos", "4")
    col3.metric("Silhouette Score", f"{rfm_dict['silhouette_score']:.4f}")
    col4.metric("Segmento Maior", perfil.loc[perfil['N_Clientes'].idxmax(), 'Nome_Segmento'])
    
    st.divider()
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "Scatter Clusters",
        "Distribuição",
        "Perfil dos Segmentos",
        "Histogramas RFM"
    ])
    
    # ── Tab 1: Scatter ───────────────────────────────────────────────────────
    with tab1:
        st.markdown("### Segmentos: Recência vs Monetário")
        
        rfm_plot = rfm.copy()
        rfm_plot['Segmento'] = rfm_plot['Cluster'].map(
            {i: perfil.loc[i, 'Nome_Segmento'] for i in perfil.index}
        )
        
        # Limitar outliers visuais
        q95 = rfm_plot['Monetario'].quantile(0.95)
        rfm_plot_filtered = rfm_plot[rfm_plot['Monetario'] <= q95]
        
        fig_scatter = px.scatter(
            rfm_plot_filtered,
            x='Recencia', y='Monetario',
            color='Segmento',
            color_discrete_map=COLORS['cluster_names'],
            opacity=0.4,
            hover_data=['Frequencia'],
            labels={
                'Recencia': 'Recência (dias)',
                'Monetario': 'Monetário (R$)',
                'Frequencia': 'Frequência'
            }
        )
        fig_scatter.update_traces(marker=dict(size=5))
        fig_scatter.update_layout(
            **PLOTLY_LAYOUT,
            title=dict(text='Segmentos de Clientes — Recência vs Monetário', font=dict(size=18)),
            height=550,
            xaxis=dict(gridcolor='rgba(148,163,184,0.1)'),
            yaxis=dict(gridcolor='rgba(148,163,184,0.1)'),
            legend=dict(
                bgcolor='rgba(0,0,0,0.5)',
                bordercolor='rgba(99,102,241,0.3)',
                borderwidth=1,
                font=dict(size=12)
            )
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    # ── Tab 2: Distribuição ──────────────────────────────────────────────────
    with tab2:
        st.markdown("### Distribuição de Clientes por Segmento")
        
        nomes_seg = [perfil.loc[i, 'Nome_Segmento'] for i in perfil.index]
        cores_seg = [COLORS['cluster_names'].get(n, '#6366f1') for n in nomes_seg]
        
        fig_bar = go.Figure(go.Bar(
            x=nomes_seg,
            y=perfil['N_Clientes'].values,
            marker_color=cores_seg,
            text=[f"{v:,}<br>({p}%)" for v, p in zip(perfil['N_Clientes'].values, perfil['Pct_Clientes'].values)],
            textposition='outside',
            textfont=dict(color='#e2e8f0', size=13),
            hovertemplate='%{x}<br>Clientes: %{y:,}<extra></extra>'
        ))
        fig_bar.update_layout(
            **PLOTLY_LAYOUT,
            title=dict(text='Distribuição de Clientes por Segmento', font=dict(size=18)),
            yaxis_title='Nº de Clientes',
            height=450,
            xaxis=dict(gridcolor='rgba(148,163,184,0.1)'),
            yaxis=dict(gridcolor='rgba(148,163,184,0.1)'),
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # ── Tab 3: Perfil ────────────────────────────────────────────────────────
    with tab3:
        st.markdown("### Perfil Médio dos Segmentos")
        
        perfil_display = perfil[['Nome_Segmento', 'N_Clientes', 'Pct_Clientes', 'Recencia', 'Frequencia', 'Monetario']].copy()
        perfil_display.columns = ['Segmento', 'Nº Clientes', '% Clientes', 'Recência (dias)', 'Frequência', 'Monetário (R$)']
        st.dataframe(perfil_display, use_container_width=True)
        
        # Radar chart
        st.markdown("### Radar — Perfil Normalizado dos Segmentos")
        
        perfil_norm = perfil[['Recencia', 'Frequencia', 'Monetario']].copy()
        for col_name in perfil_norm.columns:
            r = perfil_norm[col_name].max() - perfil_norm[col_name].min()
            if col_name == 'Recencia':
                perfil_norm[col_name] = 1 - (perfil_norm[col_name] - perfil_norm[col_name].min()) / (r if r > 0 else 1)
            else:
                perfil_norm[col_name] = (perfil_norm[col_name] - perfil_norm[col_name].min()) / (r if r > 0 else 1)
        
        categories = ['Recência (Recente)', 'Frequência', 'Monetário']
        
        fig_radar = go.Figure()
        for cluster_id in perfil.index:
            nome = perfil.loc[cluster_id, 'Nome_Segmento']
            vals = perfil_norm.loc[cluster_id].values.tolist()
            vals += vals[:1]
            color = COLORS['cluster_names'].get(nome, '#6366f1')
            # Convert hex to rgba for fill transparency
            hex_c = color.lstrip('#')
            r, g, b = int(hex_c[:2], 16), int(hex_c[2:4], 16), int(hex_c[4:6], 16)
            fill_rgba = f'rgba({r},{g},{b},0.1)'
            fig_radar.add_trace(go.Scatterpolar(
                r=vals,
                theta=categories + [categories[0]],
                name=nome,
                fill='toself',
                fillcolor=fill_rgba,
                line=dict(color=color, width=2),
            ))
        
        fig_radar.update_layout(
            **PLOTLY_LAYOUT,
            title=dict(text='Perfil dos Segmentos (Radar RFM)', font=dict(size=18)),
            height=500,
            polar=dict(
                bgcolor='rgba(0,0,0,0)',
                radialaxis=dict(visible=True, gridcolor='rgba(148,163,184,0.15)', linecolor='rgba(148,163,184,0.15)'),
                angularaxis=dict(gridcolor='rgba(148,163,184,0.15)', linecolor='rgba(148,163,184,0.15)')
            ),
            legend=dict(bgcolor='rgba(0,0,0,0.5)', bordercolor='rgba(99,102,241,0.3)', borderwidth=1),
        )
        st.plotly_chart(fig_radar, use_container_width=True)
    
    # ── Tab 4: Histogramas RFM ───────────────────────────────────────────────
    with tab4:
        st.markdown("### Distribuição das Métricas RFM")
        
        rfm_metric = st.selectbox(
            "Selecione a métrica:",
            ['Recencia', 'Frequencia', 'Monetario'],
            format_func=lambda x: {'Recencia': 'Recência (dias)', 'Frequencia': 'Frequência', 'Monetario': 'Monetário (R$)'}[x]
        )
        
        color_map = {'Recencia': COLORS['primary'], 'Frequencia': COLORS['success'], 'Monetario': COLORS['danger']}
        
        fig_rfm_hist = go.Figure(go.Histogram(
            x=rfm[rfm_metric],
            nbinsx=60,
            marker_color=color_map[rfm_metric],
            opacity=0.8,
            hovertemplate=f'{rfm_metric}: ' + '%{x}<br>Contagem: %{y}<extra></extra>'
        ))
        fig_rfm_hist.update_layout(
            **PLOTLY_LAYOUT,
            title=dict(text=f'Distribuição: {rfm_metric}', font=dict(size=18)),
            xaxis_title=rfm_metric,
            yaxis_title='Contagem',
            height=400,
            xaxis=dict(gridcolor='rgba(148,163,184,0.1)'),
            yaxis=dict(gridcolor='rgba(148,163,184,0.1)'),
        )
        st.plotly_chart(fig_rfm_hist, use_container_width=True)
        
        with st.expander("📋 Estatísticas RFM"):
            st.dataframe(rfm[['Recencia', 'Frequencia', 'Monetario']].describe().round(2), use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# FOOTER
# ═════════════════════════════════════════════════════════════════════════════
st.markdown(f"""
<div class="custom-footer">
    <div class="footer-stats">
        <span class="footer-stat">📊 <strong>{dados.shape[0]:,}</strong> registros</span>
        <span class="footer-stat">📋 <strong>{dados.shape[1]}</strong> colunas</span>
        <span class="footer-stat">🤖 <strong>{len(df_clean):,}</strong> amostras modelo</span>
        <span class="footer-stat">👥 <strong>{len(rfm_dict['rfm']):,}</strong> clientes segmentados</span>
    </div>
    <span class="footer-credit">Dashboard Interativa — ISPTEC 2026 · Desenvolvido por José Pedro</span>
</div>
""", unsafe_allow_html=True)
