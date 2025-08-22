#%%
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline
from joblib import dump

#%%
# extract
base = pd.read_parquet('data/base_clickbus_tratada.parquet')
clientes = pd.read_parquet('data/clientes_clickbus.parquet')

#%%
# pipeline
pipeline = Pipeline([
    ('scaler', StandardScaler()),
    ('kmeans', KMeans(n_clusters=4, random_state=42))
])

# Treinar o pipeline
pipeline.fit(clientes[['recencia', 'frequencia', 'monetario', 'ticket_medio']])

#%%
# Salvar pipeline completo
dump(pipeline, 'models/pipeline_kmeans_clientes.joblib')