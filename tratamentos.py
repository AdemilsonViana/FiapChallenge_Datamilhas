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
base['date_purchase'] = pd.to_datetime(base['date_purchase'])

    # rota
base['rota'] = base['place_origin_departure'] + " - " + base['place_destination_departure']

#%%
    # clientes
data_ref = base['date_purchase'].max()
clientes = base.groupby('fk_contact').agg(
    recencia = ('date_purchase', lambda x: (data_ref - x.max()).days),
    frequencia = ('date_purchase', 'count'),
    monetario = ('gmv_success', 'sum')
).reset_index()

#%%
# save data
base.to_parquet('data/base_clickbus_tratada.parquet', index=False)
clientes.to_parquet('data/clientes_clickbus.parquet', index=False)