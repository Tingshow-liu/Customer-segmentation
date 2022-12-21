import os
import pandas as pd
import numpy as np
from google.cloud import bigquery as bq
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"


# Upload RFM segmentation result to GBQ
class to_GBQ:
    
    def __init__(self, rfm_results, cluster_info, project, dataset, table, creator, campaign_lst, country):
        self.rfm_results = rfm_results  # Segmentation result
        self.cluster_info = cluster_info  
        self.project = project  
        self.dataset = dataset  
        self.table = table  
        # Follow company's table schema
        self.creator = creator
        self.campaign_lst = campaign_lst
        self.country = country

    
    def preprocess(self):  # Process RFM results to GBQ & campaign format
        cluster_ref = self.cluster_info.reset_index(drop=True)
        
        # Map clusters to campaign names
        dict_keys = cluster_ref["cluster"].tolist()
        dict_val = self.campaign_lst
        cluster_campaign_dict = dict(zip(dict_keys, dict_val))

        to_gbq_df = self.rfm_results[["member", "cluster"]]
        to_gbq_df["create_time"] = np.datetime64("today")
        to_gbq_df["creator"] = self.creator
        to_gbq_df["campaign"] = to_gbq_df["cluster"].map(cluster_campaign_dict)
        to_gbq_df["country"] = self.country
        to_gbq_df = to_gbq_df.loc[:, ["create_time", "creator", "campaign", "member", "country"]]

        return to_gbq_df

    
    def to_gbq(self, to_gbq_df):

        # Load client
        client = bq.Client(project=self.project)

        # Define table name, in format dataset.table_name
        name_seg = self.table
        table = self.dataset + name_seg

        # Delete the old table first, then upload the new table
        client.delete_table(table, not_found_ok=True)  # Make an API request.
        # Load data to BQ
        client.load_table_from_dataframe(to_gbq_df, table)

