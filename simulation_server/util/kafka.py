import os
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient


def _get_kafka_config():
    env_config = {
        # Pick-up credentials from the context
       'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP'],
       'sasl.mechanism': os.environ.get('KAFKA_SASL_MECHANISM'),
       'security.protocol': os.environ.get('KAFKA_SECURITY_PROTOCOL'),
       'sasl.plain.username': os.environ.get('KAFKA_SASL_USERNAME'),
       'sasl.plain.password': os.environ.get('KAFKA_SASL_PASSWORD'),
    }
    return {k: v for k, v in env_config.items() if v is not None}


def get_kafka_producer(config = {}):
    # Use confluent_kafka as it has significantly better producer performance
    # I think that kafka.KafkaProducer sends messages in a background thread so it still blocks the
    # GIL, while confluent_kafka is using some kind c bindings internally which avoid that.
    return Producer({**_get_kafka_config(), **config})


def get_kafka_consumer(*topics, config = {}):
    consumer = Consumer({**_get_kafka_config(), **config})
    consumer.subscribe(list(topics))
    return consumer


def get_kafka_admin(config = {}):
    return AdminClient({**_get_kafka_config(), **config})
