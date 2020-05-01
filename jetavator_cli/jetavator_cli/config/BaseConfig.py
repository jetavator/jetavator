from jetavator.config import Config
from jetavator import json_schema_objects as jso


class BaseConfig(Config):

    wheel_path: str = jso.Property(jso.String)
    jetavator_source_path: str = jso.Property(jso.String)