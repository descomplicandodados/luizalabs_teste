import pandas as pd
from sqlalchemy import create_engine

# Configurações de conexão
DATABASE_URL = f"postgresql://admin:admin@db:5432/mydatabase"
engine = create_engine(DATABASE_URL)

csv_files = [
    'tabela_a.csv',
    'tabela_b.csv'
]


# Ingestão de dados
for csv_file in csv_files:
    try:
        # Ler o CSV em um DataFrame com delimitador ','
        df = pd.read_csv(csv_file, sep=',', on_bad_lines='skip', engine='python')  # Ignora linhas problemáticas
                
        # Carregar o DataFrame no PostgreSQL
        df.to_sql(csv_file.split('.')[0], engine, if_exists='replace', index=False)
    except Exception as e:
        print(f"Erro ao processar {csv_file}: {e}")
