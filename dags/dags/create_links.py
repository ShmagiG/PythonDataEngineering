import pandas as pd
import numpy as np
import hashlib


def create_links():
    hub_hosts = pd.read_csv('/airflow/data/DataVaultTables/hubs/hub_hosts.csv')
    hub_listing = pd.read_csv('/airflow/data/DataVaultTables/hubs/hub_listing.csv')
    #create link_hosts_listings
    links = pd.read_csv('/airflow/data/listing_to_host_mapping_existing_7_Sep_2022.csv')
    links = links.merge(hub_hosts, on='host_id')
    links.rename(columns = {'hash_key' : 'host_hash_key'}, inplace=True)
    links = links.merge(hub_listing, on='listing_id')
    links.rename(columns = {'hash_key' : 'listing_hash_key'}, inplace=True)
    links = links[['host_hash_key', 'listing_hash_key', 'valid_as_of']]
    links.to_csv('/airflow/data/DataVaultTables/links/link_hosts_listings.csv')
    pass