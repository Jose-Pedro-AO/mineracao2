import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
from sklearn.ensemble import RandomForestRegressor
import os

# Configurações
os.makedirs('reports/figures', exist_ok=True)
sns.set(style='whitegrid')
plt.rcParams['figure.figsize'] = (12, 6)

def generate_eda_plots():
    print("Caregando dados...")
    dados = pd.read_csv('dados_integrados_preprocessados.csv')
    dados['order_purchase_timestamp'] = pd.to_datetime(dados['order_purchase_timestamp'])
    dados['mes_compra'] = dados['order_purchase_timestamp'].dt.to_period('M')

    print("Gerando Gráfico 1: Distribuição de Preços...")
    plt.figure(figsize=(10,6))
    sns.histplot(dados['price'], bins=50, kde=True, color='blue')
    plt.title('Distribuição de Preço do Produto')
    plt.xlim(0, 500)
    plt.savefig('reports/figures/eda_1_distribuicao_precos.png')
    plt.close()

    print("Gerando Gráfico 2: Vendas Mensais...")
    vendas_mensais = dados.groupby('mes_compra')['payment_value'].sum().reset_index()
    vendas_mensais['mes_compra'] = vendas_mensais['mes_compra'].dt.to_timestamp()
    
    plt.figure(figsize=(12,6))
    plt.plot(vendas_mensais['mes_compra'], vendas_mensais['payment_value'], marker='o', linestyle='-')
    plt.title('Evolução do Valor Total de Vendas')
    plt.grid(True)
    plt.savefig('reports/figures/eda_2_vendas_mensais.png')
    plt.close()

    print("Gerando Gráfico 3: Correlação...")
    cols_corr = ['price', 'freight_value', 'payment_value', 'payment_installments', 'review_score']
    corr_matrix = dados[cols_corr].corr()
    plt.figure(figsize=(8, 6))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1)
    plt.title('Correlação entre Variáveis')
    plt.savefig('reports/figures/eda_3_correlacao.png')
    plt.close()

    print("Gerando Gráfico 4: Mapa de Calor (HTML)...")
    if 'geolocation_lat' in dados.columns:
        geo_sample = dados.dropna(subset=['geolocation_lat', 'geolocation_lng']).sample(min(5000, len(dados)))
        fig = px.density_mapbox(geo_sample, lat='geolocation_lat', lon='geolocation_lng', z='payment_value', radius=10,
                        center=dict(lat=-14.235, lon=-51.925), zoom=3,
                        mapbox_style="open-street-map", title='Mapa de Calor de Vendas (Brasil)')
        fig.write_html('reports/figures/eda_4_mapa_vendas.html')

    print("Gerando Gráfico 5: Feature Importance...")
    df_rf = dados[['review_score', 'price', 'freight_value', 'payment_value', 'payment_installments']].dropna().sample(min(5000, len(dados)))
    X = df_rf.drop('review_score', axis=1)
    y = df_rf['review_score']
    rf = RandomForestRegressor(n_estimators=50, max_depth=5, random_state=42)
    rf.fit(X, y)
    importances = pd.Series(rf.feature_importances_, index=X.columns).sort_values()
    
    plt.figure(figsize=(10,6))
    importances.plot(kind='barh')
    plt.title('Importância das Features para Review Score')
    plt.savefig('reports/figures/eda_5_feature_importance.png')
    plt.close()

    print("Concluído! Gráficos salvos em reports/figures/")

if __name__ == "__main__":
    generate_eda_plots()
