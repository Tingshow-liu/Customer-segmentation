import pandas as pd
from datetime import datetime
import numpy as np
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
np.set_printoptions(suppress=True)


class Data_preprocess:

    def __init__(self, data, member_w_p, rfm_columns):
        self.data = data
        self.member_w_p = member_w_p  # Members that can do RFM segmentation
        self.rfm_columns = rfm_columns


    def preprocess(self):
        data_df = self.data.copy()

        # Only remain members that can do RFM
        data_df = data_df[data_df["member"].isin(self.member_w_p)]
        print("# of data for RFM modelling: {shape0}".format(shape0=data_df.shape[0]))
        print("# of total unique RFM member: {shape0}".format(shape0=data_df.value_counts("member").shape[0]))  # To verify

        # For "frequency"
        data_df['Count'] = 1

        # Remove time zone
        data_df["date"] = data_df["date"].dt.tz_localize(None)
        # Turn timestamp to datetime & only remain the date part
        data_df["date"] = pd.to_datetime(data_df["date"]).dt.normalize()
        # print(data_df.dtypes)

        return data_df

    
    def create_rfm(self, data_with_p):  # data_with_p: data_df

        # Monetary
        monetary = data_with_p.groupby("member").agg({"price": "sum"}).reset_index()
        sort_monetary = monetary.sort_values(by="price", ascending=False)  # from high to low
        print("Average purchase price (USD): {avg_price}".format(avg_price=round(sort_monetary["price"].mean(), 0)))
        print(sort_monetary.shape)  # To verify
        print(sort_monetary.head(10))  # check outliers
        print("-" * 100)

        # Frequency
        frequency = data_with_p.groupby("member").agg({"Count": "sum"}).reset_index()
        sort_frequency = frequency.sort_values(by="Count", ascending=False)
        print("Average purchase frequency: {avg_frequency}".format(avg_frequency=round(sort_frequency["Count"].mean(), 0)))
        print(sort_frequency.shape)  # To verify
        print(sort_frequency.head(10))  # check outliers
        print("-" * 100)

        # Recency
        today = np.datetime64("today")  
        data_with_p["recency"] = today - data_with_p["date"]
        data_with_p["recency"] = data_with_p["recency"].dt.days
        # Only remain the latest register time
        recency = data_with_p.groupby("member").agg({"recency": "min"}).reset_index()
        sort_recency = recency.sort_values(by="recency", ascending=False)
        print(sort_recency.shape)  # To verify
        print(sort_recency.head(10))  # check outliers
        print("-" * 100)
        
        # Concat R,F,M
        f_m = frequency.merge(monetary, left_on="member", right_on="member", how='inner', right_index=False)
        rfm_data = f_m.merge(recency, left_on="member", right_on="member", how='inner', right_index=False)
        rfm_data.columns = self.rfm_columns
        rfm_data.reset_index(drop=True, inplace=True)
        print("RFM table shape: {shape}".format(shape=rfm_data.shape))
        
        return rfm_data