from jetavator.config import (
    Config, SecretSubstitutingConfig, ConfigProperty, DBServiceConfig
)
from jetavator.config import json_schema_objects as jso


class DatabricksLibraryConfig(
    SecretSubstitutingConfig,
    register_as='remote_databricks_library'
):
    pass


class DatabricksConfig(DBServiceConfig, register_as='remote_databricks'):
    properties = {
        'type': jso.Const['remote_databricks'],
        'name': jso.String,
        'schema': jso.String,
        'host': jso.String,
        'cluster_id': jso.String,
        'org_id': jso.String,
        'token': jso.String,
        'libraries': jso.List[DatabricksLibraryConfig]
    }

    @property
    def http_path(self):
        return f'sql/protocolv1/o/{self.org_id}/{self.cluster_id}'


class BaseConfig(Config):
    properties = {
        'wheel_path': jso.String,
        'jetavator_source_path': jso.String
    }
