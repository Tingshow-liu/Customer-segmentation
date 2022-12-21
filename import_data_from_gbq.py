import os
import pandas as pd
from google.cloud import bigquery as bq
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"  # Key for company's Google Cloud Platform BigQuery(GBQ) project

client = bq.Client()

# Import company member data from GBQ & calculate
class Import_data:

    def __init__(self, dataset, table1, table2):
        self.dataset = dataset  # Data source name stored on GBQ
        self.table1 = table1 
        self.table2 = table2  

    
    def query_gbq(self):
        query_ = f'''
        WITH 
            r1 AS(
            SELECT 
                member,
                MIN(date) AS date, 
                product,
                country
            FROM `Company_Project_Name.{self.dataset}.{self.table1}`
            WHERE country = "Country Selected"
            GROUP BY 1,3,4,5 
            ),

            r2 AS(
            SELECT
                member,
                MIN(date) AS date,
                product,
                price
            FROM `Company_Project_Name.{self.dataset}.{self.table2}`
            WHERE country_3D = "Country Selected"
            GROUP BY 1,3,4
            ),

            joined AS(
            SELECT r1.*, r2.price FROM r1
            LEFT JOIN r2 ON r1.member = r2.member
            AND r1.product = r2.product
            AND r1.date = r2.date
            )

        SELECT * FROM joined
        '''

        data = client.query(query_).result().to_arrow(create_bqstorage_client=True).to_pandas()
        print("# of total register data: {shape0}".format(shape0=data.shape[0]))
        
        return data

    
    def calculate(self, data):

        # Member
        ttl_member = data.value_counts("member")
        print("# of total unique member: {shape0}".format(shape0=ttl_member.shape[0]))

        # Members with price
        member_no_p = data[data["price"].isna()]["member"].drop_duplicates().reset_index()["member"].tolist()
        data_w_p = data[~data["member"].isin(member_no_p)]  
        member_w_p = data_w_p["member"].drop_duplicates().reset_index()["member"].tolist()
        print("# of total unique RFM member: {shape0}".format(shape0=len(member_w_p)))

        print("Member mapping rate: {rate}%".format(rate=round(len(member_w_p)/ttl_member.shape[0], 4)*100))

        # Product
        all_product = data.value_counts("product")
        print("# of total unique product: {shape0}".format(shape0=all_product.shape[0]))

        # Products with price
        product_w_p = data[~data["price"].isna()]["product"].drop_duplicates().reset_index()["product"].tolist()
        print("# of total products with price: {shape0}".format(shape0=len(product_w_p)))

        print("Product price mapping rate: {rate}%".format(rate=round(len(product_w_p)/all_product.shape[0], 4)*100))

        return member_w_p

    
# if __name__ == "__main__":
#     test_import = Import_data("MIS_data_integration", "Product_Registration_Detail", "Product_Registration_EBS_Price")
#     testing = test_import.query_gbq()
#     test_import.calculate(testing)

