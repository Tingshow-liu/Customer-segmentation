import matplotlib.pylab as plt
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans


class construct_model:

    def __init__(self, rfm_data):
        self.rfm_data = rfm_data


    def best_k(self):
        rfm_df = self.rfm_data[['frequency', 'monetary', 'recency']]    

        # Normalize
        scalar = StandardScaler()
        rfm_df_scaled = scalar.fit_transform(rfm_df)
        rfm_df_scaled = pd.DataFrame(rfm_df_scaled)


        # Elbow method to find the best K
        ssd = []
        k_clusters = [2, 3, 4, 5, 6, 7, 8]  # k values
        for k in k_clusters:
            kmeans = KMeans(n_clusters=k, max_iter=1000)
            kmeans.fit(rfm_df_scaled)
            ssd.append(kmeans.inertia_)

        plt.plot(k_clusters, ssd, color='blue')
        plt.show()

        return (rfm_df_scaled, scalar)

    
    def kmeans_model(self, rfm_df_scaled, scalar):  # scalar: for inverse_transform

        # Construct model (k=4)
        kmeans = KMeans(n_clusters=4, max_iter=1000, random_state=0)
        y_clusters = kmeans.fit_predict(rfm_df_scaled)
        y_centors = kmeans.cluster_centers_

        # The Cluster of each member
        df_y_clusters = pd.DataFrame(y_clusters, columns=['cluster'])
        
        self.rfm_data.reset_index(inplace=True, drop=True)
        df_y_clusters.reset_index(inplace=True, drop=True)
        rfm_cluster = pd.concat([self.rfm_data, df_y_clusters], axis=1)  # self.rfm_data: dataframe before scaled

        rfm_columns = ["member", "frequency", "monetary", "recency", "cluster"]
        rfm_cluster.columns = rfm_columns
        print(rfm_cluster.shape)  # Verify

        # Centers of each cluster
        y_centors  
        original_centors = scalar.inverse_transform(y_centors) 
        centor_cols = ['frequency', 'monetary', 'recency']
        original_centors_df = pd.DataFrame(original_centors, columns=centor_cols)

        return (rfm_cluster, original_centors_df)  

    
    def results(self, rfm_cluster, original_centors_df):

        cluster_size = pd.DataFrame(rfm_cluster.value_counts('cluster')).reset_index()
        cluster_size.sort_values('cluster', inplace=True)
        cluster_size.reset_index(inplace=True, drop=True)

        cluster_center_size = pd.concat([original_centors_df, cluster_size], axis=1)
        cluster_info = cluster_center_size.sort_values(0)

        return cluster_info