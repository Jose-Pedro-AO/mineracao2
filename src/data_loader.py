import pandas as pd
import logging

# Configurar logging
logging.basicConfig(filename='logs/data_loader_log.txt', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def load_customers(file_path='data/olist_customers_dataset.csv'):
    try:
        df = pd.read_csv(file_path)
        df['fonte'] = 'customers'
        logging.info('Carga de customers concluída.')
        df.to_csv('data/fonte_customers.csv', index=False)
        return df
    except Exception as e:
        logging.error(f'Erro na carga de customers: {e}')
        return pd.DataFrame()

def load_orders(file_path='data/olist_orders_dataset.csv'):
    try:
        df = pd.read_csv(file_path)
        df['fonte'] = 'orders'
        logging.info('Carga de orders concluída.')
        df.to_csv('data/fonte_orders.csv', index=False)
        return df
    except Exception as e:
        logging.error(f'Erro na carga de orders: {e}')
        return pd.DataFrame()

def load_items(file_path='data/olist_order_items_dataset.csv'):
    try:
        df = pd.read_csv(file_path)
        df['fonte'] = 'items'
        logging.info('Carga de items concluída.')
        df.to_csv('data/fonte_items.csv', index=False)
        return df
    except Exception as e:
        logging.error(f'Erro na carga de items: {e}')
        return pd.DataFrame()

def load_reviews(file_path='data/olist_order_reviews_dataset.csv'):
    try:
        df = pd.read_csv(file_path)
        df['fonte'] = 'reviews'
        logging.info('Carga de reviews concluída.')
        df.to_csv('data/fonte_reviews.csv', index=False)
        return df
    except Exception as e:
        logging.error(f'Erro na carga de reviews: {e}')
        return pd.DataFrame()

# Execução principal
if __name__ == "__main__":
    df_customers = load_customers()
    df_orders = load_orders()
    df_items = load_items()
    df_reviews = load_reviews()
    print("Dados carregados! Verifique logs em logs/data_loader_log.txt")