#%%
from sqlalchemy import create_engine, text, inspect
from typing import Literal
import pandas as pd


def db_connection(
    usuario,
    senha,
    host,
    porta,
    nome_banco,
):

    engine = create_engine(
        f'postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{nome_banco}',
        isolation_level="AUTOCOMMIT")

    return engine


def iterative_load_data(engine_connection, schema,
                        ListaDFs: dict[str, pd.DataFrame]):

    with engine_connection.connect().execution_options(
            isolation_level="READ COMMITTED") as conn:
        trans = conn.begin()
        try:
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
                          if_exists='replace',
                          index=False)
                print(
                    f"Dados do DataFrame salvos na tabela '{schema}.{nome_tabela}' com sucesso."
                )

            trans.commit()
        except Exception as e:
            trans.rollback()
            raise
