from JP_rfm_from_gbq import Import_data
from JP_rfm_preprocess import Data_preprocess
from JP_rfm_model import construct_model
from JP_rfm_to_gbq import to_GBQ


if __name__ == "__main__":
    rfm_columns = ['MemberID', 'frequency', 'monetary', 'recency']

    # Import data and calculate
    import_from_gbq = Import_data("Company GBQ Project Name", "Company GBQ Table Name 1", "Company GBQ Table Name 2")
    data = import_from_gbq.query_gbq()
    member_w_p_lst = import_from_gbq.calculate(data)

    # Preprocess
    data_preprocess = Data_preprocess(data, member_w_p_lst, rfm_columns)
    data_df = data_preprocess.preprocess()
    rfm_data = data_preprocess.create_rfm(data_df)
    print(rfm_data.shape)
    print(rfm_data.head())

    # Construct model
    ml_model = construct_model(rfm_data)
    rfm_data_scaled, scalar = ml_model.best_k()  # scalar: for inverse_transform
    rfm_cluster, original_centors_df = ml_model.kmeans_model(rfm_data_scaled, scalar)  # rfm_cluster: Final segment result
    print(rfm_cluster.shape)
    print(rfm_cluster.head())
    print("----Modelling result----")
    cluster_info = ml_model.results(rfm_cluster, original_centors_df)
    print(type(cluster_info))
    print(cluster_info.head())

    # Upload to GBQ
    project = "Company GBQ Project Name"
    dataset = "Company GBQ Dataset Name"
    table = "Company GBQ Table Name"
    creator = "Claire"
    campaign_lst = ["Different campaign names"]
    country = "Selected Country"
    upload_to_gbq = to_GBQ(rfm_cluster, cluster_info, project, dataset, table, creator, campaign_lst, country)
    gbq_df = upload_to_gbq.preprocess()
    
    print("# of total members upload: {shape0}".format(shape0=gbq_df.shape[0]))
    print("----Modelling result to GBQ----")
    print(gbq_df.head())

    upload_to_gbq.to_gbq(gbq_df)

    


