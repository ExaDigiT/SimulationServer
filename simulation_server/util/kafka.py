import os
from kafka import KafkaProducer, KafkaConsumer
import functools


@functools.cache
def get_kafka_producer(**configs):
    configs = {
        # Pick-up credentials from the context
       'bootstrap_servers': [os.environ['KAFKA_BOOTSTRAP']],
       'sasl_mechanism': os.environ['KAFKA_SASL_MECHANISM'],
       'sasl_plain_username': os.environ['KAFKA_SASL_USERNAME'],
       'sasl_plain_password': os.environ['KAFKA_SASL_PASSWORD'],
       'security_protocol': os.environ['KAFKA_SECURITY_PROTOCOL'],
       **configs,
    }
    return KafkaProducer(**configs)


@functools.cache
def get_kafka_consumer(*topics, **configs):
    configs = {
        # Pick-up credentials from the context
       'bootstrap_servers': [os.environ['KAFKA_BOOTSTRAP']],
       'sasl_mechanism': os.environ['KAFKA_SASL_MECHANISM'],
       'sasl_plain_username': os.environ['KAFKA_SASL_USERNAME'],
       'sasl_plain_password': os.environ['KAFKA_SASL_PASSWORD'],
       'security_protocol': os.environ['KAFKA_SECURITY_PROTOCOL'],
       **configs,
    }
    return KafkaConsumer(*topics, **configs)
