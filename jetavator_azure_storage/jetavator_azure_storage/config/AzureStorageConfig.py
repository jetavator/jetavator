from jetavator.config import ServiceConfig
import jsdom


class AzureStorageConfig(ServiceConfig, register_as='azure_storage'):

    type = jsdom.Property(jsdom.Const('azure_storage'))
    name = jsdom.Property(str)
    account_name = jsdom.Property(str)
    account_key = jsdom.Property(str)
    blob_container_name = jsdom.Property(str)