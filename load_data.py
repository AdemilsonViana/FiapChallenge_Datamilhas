#%%
# import
import pandas as pd
from db_tools import db_connection, iterative_load_data
import os
from dotenv import load_dotenv

load_dotenv()

#%%
# load
conn = db_connection(
    usuario = os.environ['usuario'],
    senha = os.environ['senha'],
    host = os.environ['host'],
    porta = os.environ['porta'],
    nome_banco = os.environ['nome_banco'],
)

base = pd.read_parquet('final_data/base_clickbus.parquet')
clientes = pd.read_parquet('final_data/clientes_clickbus.parquet')

data = {
    'base': base,
    'clientes': clientes
}

#%%
    # função que carregar os dados de forma iterativa e como transação atômica
iterative_load_data(
    conn, schema=os.environ['schema'], ListaDFs=data
)