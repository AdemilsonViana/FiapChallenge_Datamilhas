#%%
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
import joblib

#%%
# clientes_rotas
clientes_rotas = pd.read_parquet('data/clientes_rotas_clickbus.parquet')

features = ['freq_rota', 'monetario_rota', 'recencia_rota']
target = 'comprou_rota_30d'

X = clientes_rotas[features].astype(float)
y = clientes_rotas[target].astype(int)

#%%
# modelo simples
mdl_rota = LogisticRegression(max_iter=200)
mdl_rota.fit(X, y)

# previsões
clientes_rotas['_prob_rota_30d'] = mdl_rota.predict_proba(X)[:, 1]
clientes_rotas['_pred_rota_30d'] = (clientes_rotas['_prob_rota_30d'] >= 0.5).astype(int)

#%%
# save model
joblib.dump(mdl_rota, 'data/modelo_rota_30d.joblib')

# %%
