from typing import List

import sqlalchemy
from sqlalchemy import func, cast, literal_column


PANDAS_DTYPE_MAPPINGS = {
    "bigint": "Int64",
    "bit": "object",
    "boolean": "object",
    "decimal": "Int64",
    "int": "Int64",
    "integer": "Int64",
    "money": "float",
    "numeric": "float",
    "smallint": "Int64",
    "smallmoney": "float",
    "tinyint": "Int64",
    "float": "float",
    "real": "float",
    "date": "datetime64[ns]",
    "datetime": "datetime64[ns]",
    "datetime2": "datetime64[ns]",
    "smalldatetime": "datetime64[ns]",
    "time": "timedelta64[ns]",
    "char": "object",
    "string": "object",
    "text": "object",
    "varchar": "object",
    "nchar": "object",
    "ntext": "object",
    "nvarchar": "object",
    "binary": "object",
    "varbinary": "object",
    "image": "object"
}


class ColumnType(str):

    @property
    def name(self) -> str:
        return self.split("(")[0]

    @property
    def parameters(self) -> List[int]:
        if "(" in self:
            remainder = self.split("(")[1]
            if ")" not in remainder:
                raise ValueError(f"{self} is not a valid type definition.")
            return [int(x.strip()) for x in remainder.split(")")[0].split(",")]
        else:
            return []
        
    @property
    def serialized_length(self) -> int:
        # TODO: Take a more comprehensive look at SQL types and how to serialize them
        if isinstance(self.sqlalchemy_type, sqlalchemy.types.Integer):
            return 11
        elif isinstance(self.sqlalchemy_type, sqlalchemy.types.Date):
            return 10
        elif isinstance(self.sqlalchemy_type, sqlalchemy.types.DateTime):
            return 25
        elif isinstance(self.sqlalchemy_type, sqlalchemy.types.Boolean):
            return 1
        elif isinstance(self.sqlalchemy_type, sqlalchemy.types.String):
            if len(self.parameters) == 0:
                ValueError(f"Must specify length of string variables")
            return self.parameters[0]
        else:
            raise ValueError(f"Cannot determine serialization length for type {self.name}")

    @property
    def sqlalchemy_type(self) -> sqlalchemy.types.TypeEngine:
        if self.name not in sqlalchemy.types.__all__:
            raise KeyError(f"{self.name} is not a valid SQLAlchemy type. "
                           "Note that types are case sensitive.")
        return getattr(sqlalchemy.types, self.name)(*self.parameters)

    @property
    def pandas_dtype(self):
        return PANDAS_DTYPE_MAPPINGS[self.name.lower()]

    def serialize_column_expression(
            self,
            column: sqlalchemy.sql.ColumnElement
    ) -> sqlalchemy.sql.ColumnElement:
        if isinstance(self.sqlalchemy_type, sqlalchemy.types.Integer):
            return cast(column, self.sqlalchemy_type)
        elif isinstance(self.sqlalchemy_type, sqlalchemy.types.Date):
            return func.date_format(column, literal_column("'yyyy-MM-dd'"))
        elif isinstance(self.sqlalchemy_type, sqlalchemy.types.DateTime):
            return func.date_format(column, literal_column("'yyyy-MM-dd HH:mm:ssX'"))
        elif isinstance(self.sqlalchemy_type, sqlalchemy.types.Boolean):
            return cast(column, self.sqlalchemy_type)
        elif isinstance(self.sqlalchemy_type, sqlalchemy.types.String):
            # TODO: Allow case-sensitive keys rather than forcing to uppercase?
            return func.upper(func.ltrim(func.rtrim(column)))
        else:
            raise ValueError(f"No serializer defined for type {self.name}")