import jsdom
from jetavator.config import DBServiceConfig


class MSSQLConfig(DBServiceConfig, register_as='mssql'):

    type: str = jsdom.Property(jsdom.Const('mssql'))
    database: str = jsdom.Property(str)
    server: str = jsdom.Property(str)
    username: str = jsdom.Property(str)
    password: str = jsdom.Property(str)
    trusted_connection: bool = jsdom.Property(bool)
