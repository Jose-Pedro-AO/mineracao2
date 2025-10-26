import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import os

# Garante que a pasta 'logs' exista
os.makedirs('logs', exist_ok=True)
os.makedirs('data', exist_ok=True)

# Configurar logging
logging.basicConfig(
    filename='logs/scraping_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def scrape_site_a(base_url='http://books.toscrape.com/', max_pages=3):
    headers = {'User-Agent': 'Mozilla/5.0'}
    produtos = []
    current_url = base_url

    try:
        for page in range(1, max_pages + 1):
            response = requests.get(current_url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')

            for item in soup.select('article.product_pod'):
                nome = item.select_one('h3 a')['title']
                preco_str = item.select_one('p.price_color').text.strip()
                # Limpar caracteres indesejados (ex.: £ ou �)
                preco_clean = ''.join(filter(lambda x: x.isdigit() or x == '.', preco_str))  # Mantém apenas dígitos e ponto
                try:
                    preco = float(preco_clean) if preco_clean else None
                except ValueError as e:
                    logging.warning(f'Falha ao converter preço "{preco_str}" para float: {e}')
                    preco = None  # Define como None se falhar
                rating_class = item.select_one('p.star-rating')['class'][1]
                rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
                avaliacao = rating_map.get(rating_class, 0)
                produtos.append({'nome': nome, 'preco': preco, 'avaliacao': avaliacao, 'fonte': 'SiteA'})

            next_button = soup.select_one('li.next a')
            if next_button:
                next_page = next_button['href']
                current_url = requests.compat.urljoin(current_url, next_page)
            else:
                break

            logging.info(f'Página {page} scraped com sucesso.')

        df = pd.DataFrame(produtos)
        df.to_csv('data/fonte1.csv', index=False)
        logging.info('Scraping do Site A concluído.')
        return df

    except Exception as e:
        logging.error(f'Erro no scraping: {e}')
        return pd.DataFrame()

def load_site_b_csv(file_path='data/fonte2.csv'):
    """
    Carrega e pré-filtra o CSV do Site B (Kaggle e-commerce).
    """
    try:
        df = pd.read_csv(file_path, encoding='ISO-8859-1')  # Encoding comum para esse dataset
        df = df.rename(columns={'Description': 'nome', 'UnitPrice': 'preco'})
        df['fonte'] = 'SiteB'
        df['avaliacao'] = None  # Placeholder, pois não há ratings
        df = df[['nome', 'preco', 'avaliacao', 'fonte']]  # Reduzir colunas
        df.to_csv('data/fonte2.csv', index=False)  # Sobrescreve se necessário
        logging.info('Carga do Site B concluída.')
        return df
    except Exception as e:
        logging.error(f'Erro na carga do CSV: {e}')
        return pd.DataFrame()

# Execução principal (rode isso para coletar dados)
if __name__ == "__main__":
    df1 = scrape_site_a()
    df2 = load_site_b_csv()
    print("Dados coletados! Verifique logs em logs/scraping_log.txt")