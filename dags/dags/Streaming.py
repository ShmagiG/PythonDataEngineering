import functools
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import change_detection

from airflow import DAG
from airflow.operators.python import PythonOperator


from airflow_provider_kafka.operators.await_message import AwaitKafkaMessageOperator
from airflow_provider_kafka.operators.consume_from_topic import ConsumeFromTopicOperator
from airflow_provider_kafka.operators.produce_to_topic import ProduceToTopicOperator

default_args = {
    "owner": "airflow",
    "depend_on_past": False,
    "start_date": datetime(2023, 2, 8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def host_tr_producer_function():
    host_tr = pd.read_csv('/airflow/data/hosts_transactions_8_10_Sep_2022.csv')
    host_tr.sort_values(by='transaction_datetime')
    for i in range(len(host_tr)):
        yield host_tr.iloc[i:i+1].to_json()

def listing_tr_producer_function():
    listing_tr = pd.read_csv('/airflow/data/hosts_transactions_8_10_Sep_2022.csv')
    listing_tr.sort_values(by='transaction_datetime')
    for i in range(len(listing_tr)):
        yield listing_tr.iloc[i:i+1].to_json()

def link_tr_producer_function():
    link_tr = pd.read_csv('/airflow/data/hosts_transactions_8_10_Sep_2022.csv')
    link_tr.sort_values(by='transaction_datetime')
    for i in range(len(link_tr)):
        yield link_tr.iloc[i:i+1].to_json()


consumer_logger = logging.getLogger("airflow")

def host_consumer_function_batch(messages, prefix=None):
    for message in messages:
        tr_row = pd.DataFrame(message)
        change_detection.hosts_change_detection(tr_row)
    return

def listing_consumer_function_batch(messages, prefix=None):
    for message in messages:
        tr_row = pd.DataFrame(message)
        change_detection.listing_change_detection(tr_row)
    return

def link_consumer_function_batch(messages, prefix=None):
    for message in messages:
        tr_row = pd.DataFrame(message)
        change_detection.linking_change_detection(tr_row)
    return

with DAG(
    "kafka-example",
    default_args=default_args,
    description="Examples of Kafka Operators",
    schedule_interval=timedelta(minutes=1),
    start_date=datetime(2023, 2, 8),
    catchup=False,
    tags=["example"],
) as dag:

    host_producer = ProduceToTopicOperator(
        task_id="produce_host_transaction_change",
        topic="host_transaction",
        producer_function=host_tr_producer_function,
        kafka_config={"bootstrap.servers": "server:9092"},
    )

    host_consumer = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["host_transaction"],
        apply_function="hello_kafka.host_consumer_function_batch",
        apply_function_kwargs={"prefix": "consumed:::"},
        consumer_config={
            "bootstrap.servers": "server:9092",
            "group.id": "foo",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )
    listing_producer = ProduceToTopicOperator(
        task_id="produce_host_transaction_change",
        topic="host_transaction",
        producer_function=listing_tr_producer_function,
        kafka_config={"bootstrap.servers": "server:9092"},
    )

    listing_consumer = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["host_transaction"],
        apply_function="hello_kafka.listing_consumer_function_batch",
        apply_function_kwargs={"prefix": "consumed:::"},
        consumer_config={
            "bootstrap.servers": "server:9092",
            "group.id": "foo",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )
    link_producer = ProduceToTopicOperator(
        task_id="produce_host_transaction_change",
        topic="host_transaction",
        producer_function=link_tr_producer_function,
        kafka_config={"bootstrap.servers": "server:9092"},
    )

    link_consumer = ConsumeFromTopicOperator(
        task_id="consume_from_topic",
        topics=["host_transaction"],
        apply_function="hello_kafka.link_consumer_function_batch",
        apply_function_kwargs={"prefix": "consumed:::"},
        consumer_config={
            "bootstrap.servers": "server:9092",
            "group.id": "foo",
            "enable.auto.commit": False,
            "auto.offset.reset": "beginning",
        },
        commit_cadence="end_of_batch",
        max_messages=10,
        max_batch_size=2,
    )

    host_producer >> host_consumer
    listing_producer >> listing_consumer
    link_producer >> link_consumer