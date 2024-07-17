import os
from kafka import KafkaProducer, KafkaConsumer
import functools


@functools.cache
def get_kafka_producer(**configs):
    env_configs = {
        # Pick-up credentials from the context
       'bootstrap_servers': [os.environ['KAFKA_BOOTSTRAP']],
       'sasl_mechanism': os.environ.get('KAFKA_SASL_MECHANISM'),
       'sasl_plain_username': os.environ.get('KAFKA_SASL_USERNAME'),
       'sasl_plain_password': os.environ.get('KAFKA_SASL_PASSWORD'),
       'security_protocol': os.environ.get('KAFKA_SECURITY_PROTOCOL'),
    }
    env_configs = {k: v for k, v in env_configs.items() if v is not None}
    print(env_configs)
    return KafkaProducer(**{**env_configs, **configs})


@functools.cache
def get_kafka_consumer(*topics, **configs):
    env_configs = {
        # Pick-up credentials from the context
       'bootstrap_servers': [os.environ['KAFKA_BOOTSTRAP']],
       'sasl_mechanism': os.environ.get('KAFKA_SASL_MECHANISM'),
       'sasl_plain_username': os.environ.get('KAFKA_SASL_USERNAME'),
       'sasl_plain_password': os.environ.get('KAFKA_SASL_PASSWORD'),
       'security_protocol': os.environ.get('KAFKA_SECURITY_PROTOCOL'),
    }
    env_configs = {k: v for k, v in env_configs.items() if v is not None}
    return KafkaConsumer(*topics, **{**env_configs, **configs})
