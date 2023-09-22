import pandas as pd
import numpy as np
import hashlib

def create_references():
    import os
    if not os.path.isdir("/airflow/data/DataVaultTables/references"):
        os.makedirs("/airflow/data/DataVaultTables/references")
    if not os.path.isdir("/airflow/data/DataVaultTables/hubs"):
        os.makedirs("/airflow/data/DataVaultTables/hubs")
    if not os.path.isdir("/airflow/data/DataVaultTables/links"):
        os.makedirs("/airflow/data/DataVaultTables/links")
    if not os.path.isdir("/airflow/data/DataVaultTables/satelites"):
        os.makedirs("/airflow/data/DataVaultTables/satelites")

    hosts = pd.read_csv('/airflow/data/hosts_existing_7_Sep_2022.csv') # 5769
    listings = pd.read_csv('/airflow/data/listings_existing_7_Sep_2022.csv')
    def create_references_wrapper(target_df, target_column_name, target_ref_columns, target_file_path):
        ref_unique_values = target_df[target_column_name].unique()
        ref_ids = np.arange(1, len(ref_unique_values) + 1)
        ref_data = zip(ref_ids, ref_unique_values)
        ref_columns = target_ref_columns
        ref_locations = pd.DataFrame(ref_data, columns=ref_columns)
        ref_locations.to_csv(target_file_path)
    #create ref_source_systems
    ref_source_systems_columns = ['id', 'source_system_name']
    ref_source_systems_data = [(1, 'airbnb')]
    ref_source_systems = pd.DataFrame(ref_source_systems_data, columns=ref_source_systems_columns)
    ref_source_systems.to_csv('/airflow/data/DataVaultTables/references/ref_source_systems.csv')
    #create ref_locations
    create_references_wrapper(hosts, "host_location", ['id', 'location'], '/airflow/data/DataVaultTables/references/ref_locations.csv')
    #create ref_response_time
    create_references_wrapper(hosts, "host_response_time", ['id', 'response_time'], '/airflow/data/DataVaultTables/references/ref_response_time.csv')
    #create ref_verification
    create_references_wrapper(hosts, "host_verifications", ['id', 'verification'], '/airflow/data/DataVaultTables/references/ref_verification.csv')
    #create ref_neighborhood
    create_references_wrapper(listings, "neighbourhood", ['id', 'neighbourhood'], '/airflow/data/DataVaultTables/references/ref_neighborhood.csv')
    #create ref_neighbborhood_cleansed
    create_references_wrapper(listings, "neighbourhood_cleansed", ['id', 'neighbourhood_cleansed'], '/airflow/data/DataVaultTables/references/ref_neighborhood_cleansed.csv')
    #create ref_room_type
    create_references_wrapper(listings, "room_type", ['id', 'room_type'], '/airflow/data/DataVaultTables/references/ref_room_type.csv')
    #create ref_property_type
    create_references_wrapper(listings, "property_type", ['id', 'property_type'], '/airflow/data/DataVaultTables/references/ref_property_type.csv')
    #create ref_bathroom_text
    create_references_wrapper(listings, "bathrooms_text", ['id', 'bathrooms_text'], '/airflow/data/DataVaultTables/references/ref_bathroom_text.csv')
    pass

