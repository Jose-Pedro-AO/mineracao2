import pandas as pd
import hashlib
import logging

# Configurar logging
logging.basicConfig(filename='logs/preprocessamento_log.txt', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def integrar_fontes():
    try:
        # Carregar fontes padronizadas
        logging.info("Carregando datasets...")
        df_customers = pd.read_csv('data/fonte_customers.csv')
        df_orders = pd.read_csv('data/fonte_orders.csv')
        df_items = pd.read_csv('data/fonte_items.csv')
        df_reviews = pd.read_csv('data/fonte_reviews.csv')
        df_products = pd.read_csv('data/fonte_products.csv')
        df_sellers = pd.read_csv('data/fonte_sellers.csv')
        df_payments = pd.read_csv('data/fonte_payments.csv')
        df_geo = pd.read_csv('data/fonte_geolocation.csv')
        
        logging.info("Datasets carregados. Padronizando colunas...")
        # Padronização de colunas
        dfs = [df_customers, df_orders, df_items, df_reviews, df_products, df_sellers, df_payments, df_geo]
        for df in dfs:
            df.columns = df.columns.str.strip().str.lower()
            if 'fonte' in df.columns:
                df.drop(columns=['fonte'], inplace=True)

        # 0. Preparar Geolocalização (Agrupar por CEP para evitar explosão de linhas)
        geo_agg = df_geo.groupby('geolocation_zip_code_prefix').agg({
            'geolocation_lat': 'mean',
            'geolocation_lng': 'mean'
        }).reset_index()
        
        # 1. Enriquecer Customers com Geolocalização
        df_customers = df_customers.merge(geo_agg, left_on='customer_zip_code_prefix', right_on='geolocation_zip_code_prefix', how='left')

        # 2. Enriquecer Items com Products e Sellers
        df_items = df_items.merge(df_products, on='product_id', how='left')
        df_items = df_items.merge(df_sellers, on='seller_id', how='left')
        
        # 2. Agregar pagamentos por pedido (se houver multiplos pagamentos, somar valor, pegar tipo principal?)
        # Simplificação: pegar o payment_type do maior valor e somar payment_value
        pagamentos_agregados = df_payments.groupby('order_id').agg({
            'payment_value': 'sum',
            'payment_type': lambda x: x.mode()[0] if not x.mode().empty else x.iloc[0],
            'payment_installments': 'max'
        }).reset_index()

        # 3. Integração Principal (Base: Orders)
        dados = df_orders.merge(df_customers, on='customer_id', how='left')
        dados = dados.merge(df_items, on='order_id', how='left')
        dados = dados.merge(df_reviews, on='order_id', how='left')
        dados = dados.merge(pagamentos_agregados, on='order_id', how='left')
        
        # Limpeza
        dados['price'] = pd.to_numeric(dados['price'], errors='coerce')
        dados['freight_value'] = pd.to_numeric(dados['freight_value'], errors='coerce')
        dados['payment_value'] = pd.to_numeric(dados['payment_value'], errors='coerce')
        dados['review_score'] = pd.to_numeric(dados['review_score'], errors='coerce')
        dados['order_purchase_timestamp'] = pd.to_datetime(dados['order_purchase_timestamp'], errors='coerce')
        
        # Remover duplicatas geradas por múltiplos items ou reviews
        dados.drop_duplicates(inplace=True)
        
        # Transformação
        dados['preco_total'] = dados['price'] + dados['freight_value']
        dados['mes_compra'] = dados['order_purchase_timestamp'].dt.to_period('M')
        
        # Anonimização
        dados['customer_unique_id'] = dados['customer_unique_id'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notnull(x) else x)
        
        # Exportar
        dados.to_csv('dados_integrados_preprocessados.csv', index=False)
        logging.info(f'Pré-processamento concluído. Linhas: {len(dados)}')
        return dados
    
    except Exception as e:
        logging.error(f'Erro no pré-processamento: {e}')
        raise e

# Execução principal
if __name__ == "__main__":
    dados = integrar_fontes()
    print("Pré-processamento concluído! Verifique logs em logs/preprocessamento_log.txt")
