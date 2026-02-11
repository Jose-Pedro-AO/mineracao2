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

def load_payments(file_path='data/olist_order_payments_dataset.csv'):
    try:
        df = pd.read_csv(file_path)
        df['fonte'] = 'payments'
        logging.info('Carga de payments concluída.')
        df.to_csv('data/fonte_payments.csv', index=False)
        return df
    except Exception as e:
        logging.error(f'Erro na carga de payments: {e}')
        return pd.DataFrame()

def load_products(file_path='data/olist_products_dataset.csv'):
    try:
        df = pd.read_csv(file_path)
        df['fonte'] = 'products'
        logging.info('Carga de products concluída.')
        df.to_csv('data/fonte_products.csv', index=False)
        return df
    except Exception as e:
        logging.error(f'Erro na carga de products: {e}')
        return pd.DataFrame()

def load_sellers(file_path='data/olist_sellers_dataset.csv'):
    try:
        df = pd.read_csv(file_path)
        df['fonte'] = 'sellers'
        logging.info('Carga de sellers concluída.')
        df.to_csv('data/fonte_sellers.csv', index=False)
        return df
    except Exception as e:
        logging.error(f'Erro na carga de sellers: {e}')
        return pd.DataFrame()

def load_geolocation(file_path='data/olist_geolocation_dataset.csv'):
    try:
        df = pd.read_csv(file_path)
        df['fonte'] = 'geolocation'
        logging.info('Carga de geolocation concluída.')
        df.to_csv('data/fonte_geolocation.csv', index=False)
        return df
    except Exception as e:
        logging.error(f'Erro na carga de geolocation: {e}')
        return pd.DataFrame()

# Execução principal
if __name__ == "__main__":
    load_customers()
    load_orders()
    load_items()
    load_reviews()
    load_payments()
    load_products()
    load_sellers()
    load_geolocation()
    print("Dados carregados! Verifique logs em logs/data_loader_log.txt")