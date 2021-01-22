import wysdom
from jetavator.config import StorageServiceConfig


class MSSQLConfig(StorageServiceConfig):

    database: str = wysdom.UserProperty(str)
    server: str = wysdom.UserProperty(str)
    username: str = wysdom.UserProperty(str)
    password: str = wysdom.UserProperty(str)
    trusted_connection: bool = wysdom.UserProperty(bool, optional=True)
