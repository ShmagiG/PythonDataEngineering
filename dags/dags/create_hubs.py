import pandas as pd
import numpy as np
import hashlib

def create_hubs():
    hosts = pd.read_csv('/airflow/data/hosts_existing_7_Sep_2022.csv') # 5769
    listings = pd.read_csv('/airflow/data/listings_existing_7_Sep_2022.csv')
    def sha256hash(s: str): 
        return hashlib.sha256(s.encode('utf-8')).hexdigest()    

    def create_hub_wrapper(source_key, target_df, target_id_name, hub_columns, target_file_path):
        target_ids = target_df[target_id_name].to_list()
        target_source_keys = np.full(len(target_ids), source_key)
        hub_target_data = zip(target_source_keys, target_ids)
        hub_target = pd.DataFrame(hub_target_data, columns=hub_columns)
        hub_target = hub_target.assign(hash_key = hub_target[hub_columns[0]].astype(str) + hub_target[hub_columns[1]].astype(str))
        hub_target['hash_key'] = hub_target['hash_key'].apply(sha256hash)
        hub_target.to_csv(target_file_path)

    source_system = pd.read_csv("/airflow/data/DataVaultTables/references/ref_source_systems.csv")
    source_key = source_system.loc[source_system['source_system_name'] == 'airbnb']['id'][0]
    #create hub_hosts
    create_hub_wrapper(source_key, hosts, 'host_id', ['source_id', 'host_id'], '/airflow/data/DataVaultTables/hubs/hub_hosts.csv')
    #create hub_listing
    create_hub_wrapper(source_key, listings, 'id', ['source_id', 'listing_id'], '/airflow/data/DataVaultTables/hubs/hub_listing.csv')
    pass