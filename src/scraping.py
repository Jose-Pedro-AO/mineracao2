import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging

# Configurar logging
logging.basicConfig(filename='../logs/scraping_log.txt', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_site_a(base_url='http://books.toscrape.com/', max_pages=3):
    """
    Scraping robusto com paginação e tratamento de erros.
    """
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    produtos = []
    current_url = base_url
    
    try:
        for page in range(1, max_pages + 1):
            response = requests.get(current_url, headers=headers)
            response.raise_for_status()  # Levanta erro se falhar
            soup = BeautifulSoup(response.text, 'lxml')
            
            for item in soup.select('article.product_pod'):
                nome = item.select_one('h3 a')['title']
                preco_str = item.select_one('p.price_color').text.strip().replace('£', '').replace(',', '.')
                preco = float(preco_str) if preco_str else None
                rating_class = item.select_one('p.star-rating')['class'][1]
                rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
                avaliacao = rating_map.get(rating_class, 0)
                produtos.append({'nome': nome, 'preco': preco, 'avaliacao': avaliacao, 'fonte': 'SiteA'})
            
            # Paginação
            next_button = soup.select_one('li.next a')
            if next_button:
                next_page = next_button['href']
                current_url = base_url + 'catalogue/' + next_page if 'catalogue/' not in next_page else base_url + next_page
            else:
                break
            
            logging.info(f'Página {page} scraped com sucesso.')
        
        df = pd.DataFrame(produtos)
        df.to_csv('../data/fonte1.csv', index=False)
        logging.info('Scraping do Site A concluído.')
        return df
    
    except Exception as e:
        logging.error(f'Erro no scraping: {e}')
        return pd.DataFrame()

def load_site_b_csv(file_path='../data/fonte2.csv'):
    """
    Carrega e pré-filtra o CSV do Site B (Kaggle e-commerce).
    """
    try:
        df = pd.read_csv(file_path, encoding='ISO-8859-1')  # Encoding comum para esse dataset
        df = df.rename(columns={'Description': 'nome', 'UnitPrice': 'preco'})
        df['fonte'] = 'SiteB'
        df['avaliacao'] = None  # Placeholder, pois não há ratings
        df = df[['nome', 'preco', 'avaliacao', 'fonte']]  # Reduzir colunas
        df.to_csv('../data/fonte2.csv', index=False)  # Sobrescreve se necessário
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