"""
model_trainer.py — Módulo de Treino e Avaliação de Modelos
Encapsula toda a lógica de treino para uso na dashboard Streamlit.
"""

import pandas as pd
import numpy as np
import os
import warnings
warnings.filterwarnings('ignore')

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    classification_report, confusion_matrix, roc_auc_score,
    roc_curve, accuracy_score, f1_score, precision_score, recall_score
)
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


# ─── Features usadas pelo modelo ────────────────────────────────────────────
FEATURE_COLS = [
    'price', 'freight_value', 'payment_value', 'payment_installments',
    'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm',
    'delivery_days', 'delivery_delay', 'is_late', 'freight_ratio'
]

FEATURE_LABELS_PT = {
    'price': 'Preço do Produto (R$)',
    'freight_value': 'Valor do Frete (R$)',
    'payment_value': 'Valor do Pagamento (R$)',
    'payment_installments': 'Nº Parcelas',
    'product_weight_g': 'Peso do Produto (g)',
    'product_length_cm': 'Comprimento (cm)',
    'product_height_cm': 'Altura (cm)',
    'product_width_cm': 'Largura (cm)',
    'delivery_days': 'Dias de Entrega',
    'delivery_delay': 'Atraso na Entrega (dias)',
    'is_late': 'Entregue com Atraso (0/1)',
    'freight_ratio': 'Proporção Frete/Preço'
}


def load_data(base_dir=None):
    """Carrega e prepara o dataset integrado."""
    if base_dir is None:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    csv_path = os.path.join(base_dir, 'dados_integrados_preprocessados.csv')
    dados = pd.read_csv(csv_path)
    
    # Converter timestamps
    date_cols = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in date_cols:
        if col in dados.columns:
            dados[col] = pd.to_datetime(dados[col], errors='coerce')
    
    return dados


def prepare_classification_data(dados, max_samples=20000):
    """Feature engineering e preparação dos dados para classificação."""
    # Filtrar apenas pedidos entregues com review
    df_model = dados[
        (dados['order_status'] == 'delivered') &
        (dados['review_score'].notna()) &
        (dados['order_delivered_customer_date'].notna()) &
        (dados['order_estimated_delivery_date'].notna())
    ].copy()
    
    # Feature Engineering
    df_model['delivery_days'] = (
        df_model['order_delivered_customer_date'] - df_model['order_purchase_timestamp']
    ).dt.total_seconds() / 86400
    
    df_model['delivery_delay'] = (
        df_model['order_delivered_customer_date'] - df_model['order_estimated_delivery_date']
    ).dt.total_seconds() / 86400
    
    df_model['is_late'] = (df_model['delivery_delay'] > 0).astype(int)
    df_model['freight_ratio'] = df_model['freight_value'] / (df_model['price'] + 1)
    
    # Variável alvo
    df_model['satisfeito'] = (df_model['review_score'] >= 4).astype(int)
    
    # Limpar NaNs
    df_clean = df_model[FEATURE_COLS + ['satisfeito']].dropna()
    
    # Amostra para performance
    if len(df_clean) > max_samples:
        df_clean = df_clean.sample(max_samples, random_state=42)
    
    return df_clean


def train_models(df_clean):
    """Treina Logistic Regression e Random Forest, retorna dict com tudo."""
    X = df_clean[FEATURE_COLS]
    y = df_clean['satisfeito']
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )
    
    # Scaler
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # --- Logistic Regression ---
    lr_model = LogisticRegression(max_iter=1000, random_state=42)
    lr_model.fit(X_train_scaled, y_train)
    y_pred_lr = lr_model.predict(X_test_scaled)
    y_prob_lr = lr_model.predict_proba(X_test_scaled)[:, 1]
    
    # --- Random Forest ---
    rf_model = RandomForestClassifier(
        n_estimators=200, max_depth=10, random_state=42, n_jobs=-1
    )
    rf_model.fit(X_train_scaled, y_train)
    y_pred_rf = rf_model.predict(X_test_scaled)
    y_prob_rf = rf_model.predict_proba(X_test_scaled)[:, 1]
    
    # Cross-validation
    cv_lr = cross_val_score(lr_model, X_train_scaled, y_train, cv=5, scoring='roc_auc')
    cv_rf = cross_val_score(rf_model, X_train_scaled, y_train, cv=5, scoring='roc_auc')
    
    # ROC Curves
    fpr_lr, tpr_lr, _ = roc_curve(y_test, y_prob_lr)
    fpr_rf, tpr_rf, _ = roc_curve(y_test, y_prob_rf)
    
    # Métricas
    metrics = {
        'lr': {
            'accuracy': accuracy_score(y_test, y_pred_lr),
            'f1': f1_score(y_test, y_pred_lr),
            'precision': precision_score(y_test, y_pred_lr),
            'recall': recall_score(y_test, y_pred_lr),
            'roc_auc': roc_auc_score(y_test, y_prob_lr),
            'cv_mean': cv_lr.mean(),
            'cv_std': cv_lr.std(),
        },
        'rf': {
            'accuracy': accuracy_score(y_test, y_pred_rf),
            'f1': f1_score(y_test, y_pred_rf),
            'precision': precision_score(y_test, y_pred_rf),
            'recall': recall_score(y_test, y_pred_rf),
            'roc_auc': roc_auc_score(y_test, y_prob_rf),
            'cv_mean': cv_rf.mean(),
            'cv_std': cv_rf.std(),
        }
    }
    
    return {
        'lr_model': lr_model,
        'rf_model': rf_model,
        'scaler': scaler,
        'X_train': X_train, 'X_test': X_test,
        'y_train': y_train, 'y_test': y_test,
        'y_pred_lr': y_pred_lr, 'y_prob_lr': y_prob_lr,
        'y_pred_rf': y_pred_rf, 'y_prob_rf': y_prob_rf,
        'cm_lr': confusion_matrix(y_test, y_pred_lr),
        'cm_rf': confusion_matrix(y_test, y_pred_rf),
        'fpr_lr': fpr_lr, 'tpr_lr': tpr_lr,
        'fpr_rf': fpr_rf, 'tpr_rf': tpr_rf,
        'feature_importances': pd.Series(
            rf_model.feature_importances_, index=FEATURE_COLS
        ).sort_values(ascending=True),
        'metrics': metrics,
    }


def predict_satisfaction(models_dict, input_values):
    """
    Prevê satisfação para novos dados.
    input_values: dict com keys = FEATURE_COLS, values = float
    Retorna dict com previsões de ambos os modelos.
    """
    scaler = models_dict['scaler']
    lr = models_dict['lr_model']
    rf = models_dict['rf_model']
    
    X_new = pd.DataFrame([input_values])[FEATURE_COLS]
    X_scaled = scaler.transform(X_new)
    
    lr_pred = lr.predict(X_scaled)[0]
    lr_prob = lr.predict_proba(X_scaled)[0]
    
    rf_pred = rf.predict(X_scaled)[0]
    rf_prob = rf.predict_proba(X_scaled)[0]
    
    return {
        'lr': {'prediction': int(lr_pred), 'prob_insatisfeito': lr_prob[0], 'prob_satisfeito': lr_prob[1]},
        'rf': {'prediction': int(rf_pred), 'prob_insatisfeito': rf_prob[0], 'prob_satisfeito': rf_prob[1]},
    }


def compute_rfm(dados):
    """Calcula RFM e aplica K-Means clustering."""
    df_rfm = dados[
        (dados['order_status'] == 'delivered') &
        (dados['order_purchase_timestamp'].notna()) &
        (dados['customer_unique_id'].notna()) &
        (dados['payment_value'].notna())
    ].copy()
    
    ref_date = df_rfm['order_purchase_timestamp'].max() + pd.Timedelta(days=1)
    
    rfm = df_rfm.groupby('customer_unique_id').agg({
        'order_purchase_timestamp': lambda x: (ref_date - x.max()).days,
        'order_id': 'nunique',
        'payment_value': 'sum'
    }).rename(columns={
        'order_purchase_timestamp': 'Recencia',
        'order_id': 'Frequencia',
        'payment_value': 'Monetario'
    })
    
    # Normalizar e clusterizar
    scaler_rfm = StandardScaler()
    rfm_scaled = scaler_rfm.fit_transform(rfm)
    
    K_FINAL = 4
    kmeans = KMeans(n_clusters=K_FINAL, random_state=42, n_init=10)
    rfm['Cluster'] = kmeans.fit_predict(rfm_scaled)
    
    # Perfil
    perfil = rfm.groupby('Cluster').agg({
        'Recencia': 'mean',
        'Frequencia': 'mean',
        'Monetario': 'mean'
    }).round(2)
    perfil['N_Clientes'] = rfm.groupby('Cluster').size()
    perfil['Pct_Clientes'] = (perfil['N_Clientes'] / len(rfm) * 100).round(1)
    
    # Nomeação
    def nomear_cluster(row):
        rec_med = rfm['Recencia'].median()
        mon_med = rfm['Monetario'].median()
        freq_med = rfm['Frequencia'].median()
        if row['Recencia'] <= rec_med and row['Monetario'] >= mon_med:
            return 'VIP'
        elif row['Recencia'] <= rec_med and row['Monetario'] < mon_med:
            return 'Novos / Promissores'
        elif row['Recencia'] > rec_med and row['Frequencia'] >= freq_med:
            return 'Em Risco'
        else:
            return 'Esporádicos'
    
    perfil['Nome_Segmento'] = perfil.apply(nomear_cluster, axis=1)
    
    # Silhouette
    sample_size = min(15000, len(rfm_scaled))
    np.random.seed(42)
    idx = np.random.choice(len(rfm_scaled), sample_size, replace=False)
    sil_score = silhouette_score(rfm_scaled[idx], rfm['Cluster'].values[idx])
    
    # Elbow data
    K_range = range(2, 9)
    inertias = []
    silhouettes = []
    rfm_sample = rfm_scaled[idx]
    for k in K_range:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(rfm_sample)
        inertias.append(km.inertia_)
        silhouettes.append(silhouette_score(rfm_sample, labels))
    
    return {
        'rfm': rfm,
        'rfm_scaled': rfm_scaled,
        'perfil': perfil,
        'silhouette_score': sil_score,
        'elbow_data': {'K_range': list(K_range), 'inertias': inertias, 'silhouettes': silhouettes},
    }
