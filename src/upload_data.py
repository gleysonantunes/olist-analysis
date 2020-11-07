import os
import pandas as pd
import sqlalchemy

str_connection = 'sqlite:///{path}'

# Endereços do projeto e subpastas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

print("Meu diretório do projeto é:", BASE_DIR)
print("Meu diretório dos dados é:", DATA_DIR)

# Encontrando arquivo de dados
files_names = [arquivo for arquivo in os.listdir(DATA_DIR) if arquivo.endswith(".csv")]

# Abrindo conexão com banco
str_connection = str_connection.format(path = os.path.join(DATA_DIR, 'olist.db'))
connection = sqlalchemy.create_engine( str_connection )

# Para cada arquivo listado, uma incerção no banco de dados
for i in files_names:
    print('Coletando dados de {}'.format(i))
    df_tpm = pd.read_csv(os.path.join(DATA_DIR, i))
    TABLE_NAME = "tb_" + i.strip(".csv").replace('olist_','').replace('_dataset','')
    df_tpm.to_sql(TABLE_NAME, connection)

else:
    print("Dados coletados com sucesso")