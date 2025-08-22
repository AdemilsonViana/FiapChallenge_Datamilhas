#%%
import pandas as pd
from sklearn.linear_model import LogisticRegression
import joblib
import numpy as np

#%%
# clientes
clientes = pd.read_parquet('data\clientes_clickbus.parquet')

features = ['recencia', 'frequencia', 'monetario', 'ticket_medio']
target = 'comprou_30d'

#%%
# model
X = clientes[features].astype(float)
y = clientes[target].astype(int)
mdl = LogisticRegression(max_iter=200)
mdl

#%%
# fit model 
mdl.fit(X, y)

#%%
# save model
joblib.dump(mdl, 'model/modelo_compra_30d.joblib')
