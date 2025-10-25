import pandas as pd
import hashlib
import logging

# Configurar logging
logging.basicConfig(filename='../logs/preprocessamento_log.txt', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def integrar_fontes(df1, df2):
    """
    Integração, limpeza, transformação e redução.
    """
    try:
        # Padronização de colunas
        df1.columns = df1.columns.str.strip().str.lower()
        df2.columns = df2.columns.str.strip().str.lower()
        
        # Renomeação se necessário
        df2 = df2.rename(columns={'description': 'nome', 'unitprice': 'preco'})
        
        # Concatenação
        dados = pd.concat([df1, df2], ignore_index=True, sort=False)
        
        # Limpeza
        dados['preco'] = pd.to_numeric(dados['preco'], errors='coerce')
        dados.drop_duplicates(subset=['nome'], inplace=True)
        dados = dados.dropna(subset=['preco', 'nome'])
        
        # Transformação
        dados['nome'] = dados['nome'].astype(str).str.upper().str.strip()
        dados['preco_usd'] = dados['preco'] * 1.3  # Exemplo: converter £ para USD (ajuste taxa real)
        if 'avaliacao' in dados.columns:
            dados['avaliacao'] = pd.to_numeric(dados['avaliacao'], errors='coerce').fillna(0)
        
        # Anonimização (ex.: hash nomes se sensível)
        dados['nome_hash'] = dados['nome'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
        
        # Redução: Remover outliers (preços > 3 STD)
        mean_preco = dados['preco'].mean()
        std_preco = dados['preco'].std()
        dados = dados[(dados['preco'] > mean_preco - 3*std_preco) & (dados['preco'] < mean_preco + 3*std_preco)]
        
        # Exportar
        dados.to_csv('../dados_integrados_preprocessados.csv', index=False)
        logging.info('Pré-processamento concluído.')
        return dados
    
    except Exception as e:
        logging.error(f'Erro no pré-processamento: {e}')
        return pd.DataFrame()

# Execução principal
if __name__ == "__main__":
    df1 = pd.read_csv('../data/fonte1.csv')
    df2 = pd.read_csv('../data/fonte2.csv')
    dados = integrar_fontes(df1, df2)
    print("Pré-processamento concluído! Verifique logs em logs/preprocessamento_log.txt")