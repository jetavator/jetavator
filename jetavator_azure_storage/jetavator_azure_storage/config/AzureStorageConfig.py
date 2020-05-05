from jetavator.config import ServiceConfig
from jetavator import json_schema_objects as jso


class AzureStorageConfig(ServiceConfig, register_as='azure_storage'):
    properties = {
        'type': jso.Const['azure_storage'],
        'name': jso.String,
        'account_name': jso.String,
        'account_key': jso.String,
        'blob_container_name': jso.String
    }
