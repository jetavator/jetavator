from jetavator import json_schema_objects as jso
from jetavator.config import DBServiceConfig


class MSSQLConfig(DBServiceConfig, register_as='mssql'):

    type: str = jso.Property(jso.Const('mssql'))
    database: str = jso.Property(str)
    server: str = jso.Property(str)
    username: str = jso.Property(str)
    password: str = jso.Property(str)
    trusted_connection: bool = jso.Property(bool)
