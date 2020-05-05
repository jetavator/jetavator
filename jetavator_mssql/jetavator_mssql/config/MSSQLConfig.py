from jetavator import json_schema_objects as jso
from jetavator.config import DBServiceConfig


class MSSQLConfig(DBServiceConfig, register_as='mssql'):

    type: str = jso.Property(jso.Const['mssql'])
    database: str = jso.Property(jso.String)
    server: str = jso.Property(jso.String)
    username: str = jso.Property(jso.String)
    password: str = jso.Property(jso.String)
    trusted_connection: bool = jso.Property(jso.Bool)