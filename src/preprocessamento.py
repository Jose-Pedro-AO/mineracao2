import pandas as pd
import hashlib
import logging

# Configurar logging
logging.basicConfig(filename='logs/preprocessamento_log.txt', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def integrar_fontes(df_customers, df_orders, df_items, df_reviews):
    try:
        # Padronização de colunas
        for df in [df_customers, df_orders, df_items, df_reviews]:
            df.columns = df.columns.str.strip().str.lower()

        for df in [df_customers, df_orders, df_items, df_reviews]:
            df.columns = df.columns.str.strip().str.lower()
            if 'fonte' in df.columns:
                df.drop(columns=['fonte'], inplace=True)

        
        # Integração (merges)
        dados = df_orders.merge(df_customers, on='customer_id', how='left')
        dados = dados.merge(df_items, on='order_id', how='left')
        dados = dados.merge(df_reviews, on='order_id', how='left')
        
        # Limpeza
        dados['price'] = pd.to_numeric(dados['price'], errors='coerce')
        dados['freight_value'] = pd.to_numeric(dados['freight_value'], errors='coerce')
        dados['review_score'] = pd.to_numeric(dados['review_score'], errors='coerce')
        dados['order_purchase_timestamp'] = pd.to_datetime(dados['order_purchase_timestamp'], errors='coerce')
        dados.drop_duplicates(inplace=True)
        dados = dados.dropna(subset=['price', 'order_id'])  # Chaves essenciais
        
        # Transformação
        dados['preco_total'] = dados['price'] + dados['freight_value']  # Métrica derivada
        dados['mes_compra'] = dados['order_purchase_timestamp'].dt.to_period('M')
        # Anonimização
        dados['customer_unique_id'] = dados['customer_unique_id'].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest() if pd.notnull(x) else x)
        
        # Redução: Selecionar colunas relevantes
        colunas_relevantes = ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp', 'customer_state', 
                              'price', 'freight_value', 'preco_total', 'review_score', 'review_comment_message', 'mes_compra']
        dados = dados[colunas_relevantes]
        
        # Exportar
        dados.to_csv('dados_integrados_preprocessados.csv', index=False)
        logging.info('Pré-processamento concluído.')
        return dados
    
    except Exception as e:
        logging.error(f'Erro no pré-processamento: {e}')
        return pd.DataFrame()

# Execução principal
if __name__ == "__main__":
    df_customers = pd.read_csv('data/fonte_customers.csv')
    df_orders = pd.read_csv('data/fonte_orders.csv')
    df_items = pd.read_csv('data/fonte_items.csv')
    df_reviews = pd.read_csv('data/fonte_reviews.csv')
    dados = integrar_fontes(df_customers, df_orders, df_items, df_reviews)
    print("Pré-processamento concluído! Verifique logs em logs/preprocessamento_log.txt")