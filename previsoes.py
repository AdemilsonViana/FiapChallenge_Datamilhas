#%%
import pandas as pd
import joblib

#%%
# data
base = pd.read_parquet('data/base_clickbus_tratada.parquet')

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

    # selecionando a melhor rota por cliente
idx = clientes_rotas.groupby('fk_contact')['_prob_rota_30d'].idxmax()
clientes_rotas = clientes_rotas.loc[idx].reset_index(drop=True)

colunas_rotas = ['fk_contact', 'rota', '_prob_rota_30d', '_pred_rota_30d']
clientes = clientes.merge(
    clientes_rotas[colunas_rotas],
    left_on='fk_contact',
    right_on='fk_contact',
    how='left'
)

base.to_csv('final_data/base_clickbus.csv', index=False)
clientes.to_csv('final_data/clientes_clickbus.csv', index=False)