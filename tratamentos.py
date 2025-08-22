#%%
# imports
import pandas as pd
import datetime

#%%
# extração
base = pd.read_csv('data/base_clickbus_brutos.csv')

# %%
# tratamento
# tipos de dados
base['date_purchase'] = pd.to_datetime(base['date_purchase'], errors='coerce')

# rota
base['rota'] = base['place_origin_departure'] + " - " + base['place_destination_departure']

#%%
# função auxiliar
def media_dias_entre_compras(datas: pd.Series):
    datas = datas.sort_values()
    if len(datas) < 2:
        return pd.NA
    intervalos = datas.diff().dt.days.dropna()
    return intervalos.mean()

#%%
# janelas de tempo
ultima_data = base['date_purchase'].max()
data_ref = ultima_data - pd.Timedelta(days=30)

historico = base[base['date_purchase'] <= data_ref].copy()
futuro = base[(base['date_purchase'] > data_ref) & (base['date_purchase'] <= ultima_data)].copy()

#%%
# tabela de clientes
clientes = (
    historico
    .groupby('fk_contact')
    .agg(
        recencia=('date_purchase', lambda x: (data_ref - x.max()).days),
        frequencia=('date_purchase', 'count'),
        monetario=('gmv_success', 'sum')
    )
    .reset_index()
)

# rótulo
clientes['comprou_30d'] = clientes['fk_contact'].isin(futuro['fk_contact']).astype(int)

# ticket médio
clientes['ticket_medio'] = clientes['monetario'] / clientes['frequencia']

#%%
# tabela clientes_rotas
clientes_rotas = (
    historico
    .groupby(['fk_contact', 'rota'])
    .agg(
        freq_rota=('date_purchase', 'count'),
        monetario_rota=('gmv_success', 'sum'),
        recencia_rota=('date_purchase', lambda x: (data_ref - x.max()).days),
    )
    .reset_index()
)
clientes_rotas['ticket_medio_rota'] = clientes_rotas['monetario_rota'] / clientes_rotas['freq_rota']
# target
clientes_rotas['comprou_rota_30d'] = clientes_rotas.set_index(['fk_contact','rota']).index.isin(
    futuro[['fk_contact','rota']].drop_duplicates().set_index(['fk_contact','rota']).index
).astype(int)

#%%
# save data
base.to_parquet('data/base_clickbus_tratada.parquet', index=False)
clientes.to_parquet('data/clientes_clickbus.parquet', index=False)
clientes_rotas.to_parquet('data/clientes_rotas_clickbus.parquet', index=False)

# %%
