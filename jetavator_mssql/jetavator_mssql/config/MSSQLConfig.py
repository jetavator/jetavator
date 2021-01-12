import wysdom
from jetavator.config import DBServiceConfig


class MSSQLConfig(DBServiceConfig):

    type: str = wysdom.UserProperty(wysdom.SchemaConst('mssql'))
    database: str = wysdom.UserProperty(str)
    server: str = wysdom.UserProperty(str)
    username: str = wysdom.UserProperty(str)
    password: str = wysdom.UserProperty(str)
    trusted_connection: bool = wysdom.UserProperty(bool)
