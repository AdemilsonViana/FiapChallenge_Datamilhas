#%%
import pandas as pd
import joblib

#%%
# data
base = pd.read_csv('data/base_clickbus_brutos.csv')

clientes = pd.read_parquet('data/clientes_clickbus.parquet')
features = ['recencia', 'frequencia', 'monetario', 'ticket_medio']

clientes_rotas = pd.read_parquet('data/clientes_rotas_clickbus.parquet')

#%%
# classificação clientes
mld_classificacao = joblib.load('models/pipeline_kmeans_clientes.joblib')
clientes['classificacao_cliente'] = mld_classificacao.predict(clientes[features])

#%%
# previsão próxima compra
mdl_compra = joblib.load('models/modelo_compra_30d.joblib')
clientes['prob_compra_30d'] = mdl_compra.predict_proba(clientes[features])[:, 1]
clientes['pred_compra_30d'] = (clientes['prob_compra_30d'] >= 0.5).astype(int)

#%%
# previsão próxima rota
mdl_rota = joblib.load('models/modelo_rota_30d.joblib')
features = ['freq_rota', 'monetario_rota', 'recencia_rota']
X = clientes_rotas[features].astype(float)
clientes_rotas['_prob_rota_30d'] = mdl_rota.predict_proba(X)[:, 1]
clientes_rotas['_pred_rota_30d'] = (clientes_rotas['_prob_rota_30d'] >= 0.5).astype(int)

#%%
# save data
    # remover rótulo
clientes.drop(columns='comprou_30d')

base.to_parquet('final_data/base_clickbus.parquet', index=False)
clientes_rotas.to_parquet('final_data/clientes_rotas.parquet', index=False)
clientes.to_parquet('final_data/clientes_clickbus.parquet', index=False)