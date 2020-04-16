from jetavator.config import Config
from jetavator.config import json_schema_objects as jso


class BaseConfig(Config):
    properties = {
        'wheel_path': jso.String,
        'jetavator_source_path': jso.String
    }
