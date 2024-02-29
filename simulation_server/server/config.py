from typing import Annotated as A, NamedTuple
import os, functools
from pydantic import StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict
from fastapi import Depends
import sqlalchemy as sqla
from kafka import KafkaProducer
from ..util.kafka import get_kafka_producer as _get_kafka_producer
from ..util.druid import get_druid_engine as _get_druid_engine


class AppSettings(BaseSettings):
    log_level: A[str, StringConstraints(to_upper=True)] = "INFO"
    debug_mode: bool = True
    
    root_path: str = ""
    """ The root path of the application if you are behind a proxy """

    http_port: int = 8080

    allow_origins: list[str] = []

    job_image: str = ""

    model_config = SettingsConfigDict(
        frozen = True,
        env_prefix = 'EXADIGIT_',
    )


@functools.cache
def get_app_settings():
    return AppSettings()
AppSettingsDep = A[AppSettings, Depends(get_app_settings)]


@functools.cache
def get_druid_engine(): return _get_druid_engine()
DruidDep = A[sqla.Engine, Depends(get_druid_engine)]


@functools.cache
def get_kafka_producer(): return _get_kafka_producer()
KafkaProducerDep = A[KafkaProducer, Depends(get_kafka_producer)]


class AppDeps_(NamedTuple):
    """ Convenience wrapper around several global dependencies for use as a FastAPI dep. """
    # I'm making these lazy so I can test the queries locally without having to connect to kafka
    @property
    def settings(self): return get_app_settings()
    @property
    def druid_engine(self): return get_druid_engine()
    @property
    def kafka_producer(self): return get_kafka_producer()
AppDeps = A[AppDeps_, Depends(AppDeps_)]
