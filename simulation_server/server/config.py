from typing import Annotated as A

from pydantic import StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

class AppSettings(BaseSettings):
    log_level: A[str, StringConstraints(to_upper=True)] = "INFO"
    debug_mode: bool = True
    
    root_path: str = ""
    """ The root path of the application if you are behind a proxy """

    http_port: int = 8080

    model_config = SettingsConfigDict(
        frozen = True,
        env_prefix = 'EXADIGIT_',
    )
