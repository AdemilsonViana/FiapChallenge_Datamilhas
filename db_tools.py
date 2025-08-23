#%%
from sqlalchemy import create_engine, text, inspect
from typing import Literal
import pandas as pd


def db_connection(
    sgbd,
    usuario,
    senha,
    host,
    porta,
    nome_banco,
):

    match sgbd:
        case 'postgresql':
            string_sgbd = 'postgresql+psycopg2://'
        case 'oracle':
            string_sgbd = 'oracle+oracledb://'
        case 'sql_server':
            string_sgbd = 'mssql+pyodbc://'

    engine = create_engine(
        f'{string_sgbd}{usuario}:{senha}@{host}:{porta}/{nome_banco}',
        isolation_level="AUTOCOMMIT")

    return engine


def iterative_load_data(engine_connection, schema,
                        ListaDFs: dict[str, pd.DataFrame]):

    with engine_connection.connect().execution_options(
            isolation_level="READ COMMITTED") as conn:
        trans = conn.begin()
        try:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            print(f"Schema '{schema}' garantido.")

            for nome_tabela, df in ListaDFs.items():
                conn.execute(
                    text(
                        f'TRUNCATE TABLE "{schema}"."{nome_tabela}" CASCADE;'))
                print(
                    f"Tabela '{schema}.{nome_tabela}' limpa para novos dados (append)."
                )

                df.to_sql(nome_tabela,
                          con=conn,
                          schema=schema,
                          if_exists='append',
                          index=False)
                print(
                    f"Dados do DataFrame salvos na tabela '{schema}.{nome_tabela}' com sucesso."
                )

            trans.commit()
        except Exception as e:
            trans.rollback()
            raise
