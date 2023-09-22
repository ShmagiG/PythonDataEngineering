import pandas as pd
import numpy as np
import hashlib





def create_satelites():
    ref_response_time = pd.read_csv('/airflow/data/DataVaultTables/references/ref_response_time.csv')
    ref_verification = pd.read_csv('/airflow/data/DataVaultTables/references/ref_verification.csv')
    ref_locations = pd.read_csv('/airflow/data/DataVaultTables/references/ref_locations.csv')

    ref_neighborhood = pd.read_csv('/airflow/data/DataVaultTables/references/ref_neighborhood.csv')
    ref_neighborhood_cleansed = pd.read_csv('/airflow/data/DataVaultTables/references/ref_neighborhood_cleansed.csv')
    ref_room_type = pd.read_csv('/airflow/data/DataVaultTables/references/ref_room_type.csv')
    ref_property_type = pd.read_csv('/airflow/data/DataVaultTables/references/ref_property_type.csv')
    ref_bathroom_text = pd.read_csv('/airflow/data/DataVaultTables/references/ref_bathroom_text.csv')

    hub_hosts = pd.read_csv('/airflow/data/DataVaultTables/hubs/hub_hosts.csv')
    hub_listing = pd.read_csv('/airflow/data/DataVaultTables/hubs/hub_listing.csv')

    hosts = pd.read_csv('/airflow/data/hosts_existing_7_Sep_2022.csv') # 5769
    listings = pd.read_csv('/airflow/data/listings_existing_7_Sep_2022.csv')

    hosts = hosts.merge(hub_hosts, on='host_id')
    hosts.rename(columns = {'hash_key' : 'host_hash_key', 'host_verifications' : 'verification', 'host_response_time' : 'response_time', 'host_location': 'location'}, inplace=True)
    hosts = hosts.merge(ref_response_time, on='response_time')
    hosts.rename(columns = {'id' : 'response_time_id'}, inplace=True)
    hosts = hosts.merge(ref_verification, on='verification')
    hosts.rename(columns = {'id' : 'verification_id'}, inplace=True)
    hosts = hosts.merge(ref_locations, on='location')
    hosts.rename(columns = {'id' : 'location_id'}, inplace=True)

    listings.rename(columns = {'id' : 'listing_id'}, inplace=True)
    listings = listings.merge(hub_listing, on='listing_id')
    listings.rename(columns = {'hash_key' : 'listing_hash_key'}, inplace=True)
    listings = listings.merge(ref_neighborhood, on='neighbourhood')
    listings.rename(columns = {'id' : 'neighbourhood_id'}, inplace=True)
    listings = listings.merge(ref_neighborhood_cleansed, on='neighbourhood_cleansed')
    listings.rename(columns = {'id' : 'neighbourhood_cleansed_id'}, inplace=True)
    listings = listings.merge(ref_room_type, on='room_type')
    listings.rename(columns = {'id' : 'room_type_id'}, inplace=True)
    listings = listings.merge(ref_property_type, on='property_type')
    listings.rename(columns = {'id' : 'property_type_id'}, inplace=True)
    listings = listings.merge(ref_bathroom_text, on='bathrooms_text')
    listings.rename(columns = {'id' : 'bathrooms_text_id'}, inplace=True)
    #create satelite_host_gen_info
    satelite_host_gen_info = hosts[['host_hash_key', 'host_url', 'host_name', 'host_since']]
    satelite_host_gen_info.to_csv('/airflow/data/DataVaultTables/satelites/satelite_host_gen_info.csv')
    #create satelite_host_description
    satelite_host_description = hosts[['host_hash_key', 'host_about', 'host_picture_url', 'host_is_superhost', 'host_identity_verified']]
    satelite_host_description['effective_start_date'] = ''
    satelite_host_description['effective_end_date'] = ''
    satelite_host_description.to_csv('/airflow/data/DataVaultTables/satelites/satelite_host_description.csv')
    #create satelite_host_statuses
    satelite_host_statuses = hosts[['host_hash_key', 'response_time_id', 'host_response_rate', 'host_total_listings_count', 'verification_id', 'location_id']]
    satelite_host_statuses['transaction_timestamp'] = ''
    satelite_host_statuses.to_csv('/airflow/data/DataVaultTables/satelites/satelite_host_statuses.csv')
    #create satelite_listing_gen_info
    satelite_listing_gen_info = listings[['listing_hash_key', 'listing_url', 'name']]
    satelite_listing_gen_info.to_csv('/airflow/data/DataVaultTables/satelites/satelite_listing_gen_info.csv')
    #create satelite_listing_availability
    satelite_listing_availability = listings[['listing_hash_key', 'minimum_nights', 'maximum_nights', 'has_availability']]
    satelite_listing_availability['transaction_timestamp'] = ''
    satelite_listing_availability.to_csv('/airflow/data/DataVaultTables/satelites/satelite_listing_availability.csv')
    #create satelite_listing_description
    satelite_listing_description = listings[['listing_hash_key', 'description', 'neighborhood_overview', 'picture_url']]
    satelite_listing_description['transaction_timestamp'] = ''
    satelite_listing_description.to_csv('/airflow/data/DataVaultTables/satelites/satelite_listing_description.csv')
    #create satelite_listing_statuses
    satelite_listing_statuses = listings[['listing_hash_key', 'neighbourhood_id', 'neighbourhood_cleansed_id', 'longitude', 'latitude']]
    satelite_listing_statuses['transaction_timestamp'] = ''
    satelite_listing_statuses.to_csv('/airflow/data/DataVaultTables/satelites/satelite_listing_statuses.csv')
    #create satelite_listing_accomodation
    satelite_listing_accomodation = listings[['listing_hash_key', 'property_type_id', 'room_type_id', 'accommodates', 'bathrooms_text_id',
                                                'bedrooms', 'beds', 'amenities', 'price']]
    satelite_listing_accomodation['effective_start_date'] = ''
    satelite_listing_accomodation['effective_end_date'] = ''
    satelite_listing_accomodation.to_csv('/airflow/data/DataVaultTables/satelites/satelite_listing_accomodation.csv')
    #create satelite_listing_reviews
    satelite_listing_reviews = listings[['listing_hash_key', 'number_of_reviews', 'number_of_reviews_ltm',
                                        'number_of_reviews_l30d', 'first_review', 'last_review',
                                        'review_scores_rating', 'review_scores_accuracy',
                                        'review_scores_cleanliness', 'review_scores_checkin',
                                        'review_scores_communication', 'review_scores_location',
                                        'review_scores_value', 'reviews_per_month']]
    satelite_listing_reviews['effective_start_date'] = ''
    satelite_listing_reviews['effective_end_date'] = ''
    satelite_listing_reviews.to_csv('/airflow/data/DataVaultTables/satelites/satelite_listing_reviews.csv')
    pass