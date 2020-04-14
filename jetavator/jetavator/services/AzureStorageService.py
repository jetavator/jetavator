from lazy_property import LazyProperty
import base64

from .Service import Service

from jetavator.config import ServiceConfig
from jetavator.config import json_schema_objects as jso

from azure.storage.queue import QueueClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError


class AzureStorageConfig(ServiceConfig, register_as='azure_storage'):
    properties = {
        'type': jso.Const['azure_storage'],
        'name': jso.String,
        'account_name': jso.String,
        'account_key': jso.String,
        'blob_container_name': jso.String
    }


class AzureStorageService(Service, register_as='azure_storage'):

    def __init__(self, engine, config):
        super().__init__(engine, config)
        try:
            base64.b64decode(self.config.account_key)
        except Exception:
            raise ValueError(
                'Malformed Azure storage account key. '
                'Please check credentials.'
            )

    @property
    def connection_string(self):
        return (
            'DefaultEndpointsProtocol=https;'
            f'AccountName={self.config.account_name};'
            f'AccountKey={self.config.account_key};'
            'EndpointSuffix=core.windows.net'
        )

    def queue_client(self, name):
        return QueueClient.from_connection_string(
            self.connection_string,
            name
        )

    @LazyProperty
    def blob_service_client(self):
        return BlobServiceClient.from_connection_string(
            self.connection_string
        )

    @LazyProperty
    def blob_container_client(self):
        return self.create_container_if_not_exists()

    def create_container_if_not_exists(self):
        client = self.blob_service_client.get_container_client(
            self.config.blob_container_name
        )
        try:
            client.create_container()
        except ResourceExistsError:
            pass
        return client

    def upload_blob(self, filename, data):
        self.blob_container_client.upload_blob(filename, data)
