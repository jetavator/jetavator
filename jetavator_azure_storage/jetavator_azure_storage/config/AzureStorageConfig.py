from jetavator.config import ServiceConfig
from jetavator import json_schema_objects as jso


class AzureStorageConfig(ServiceConfig, register_as='azure_storage'):

    type = jso.Property(jso.Const('azure_storage'))
    name = jso.Property(str)
    account_name = jso.Property(str)
    account_key = jso.Property(str)
    blob_container_name = jso.Property(str)