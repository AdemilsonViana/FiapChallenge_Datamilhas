#%%
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

#%%
# extract
base = pd.read_parquet('data/base_clickbus_tratada.parquet')
clientes = pd.read_parquet('data/clientes_clickbus.parquet')

#%%
# pipeline
scaler = StandardScaler()
clientes_scaled = scaler.fit_transform(clientes[['recencia', 'frequencia', 'monetario']])
kmeans = KMeans(n_clusters=4, random_state=42)
clientes['cluster'] = kmeans.fit_predict(clientes_scaled)

# clientes['cluster_label'] = clientes['cluster']