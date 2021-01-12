from typing import Optional

import wysdom

PANDAS_DTYPE_MAPPINGS = {
    "bigint": "Int64",
    "bit": "Int64",
    "decimal": "Int64",
    "int": "Int64",
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
    "text": "object",
    "varchar": "object",
    "nchar": "object",
    "ntext": "object",
    "nvarchar": "object",
    "binary": "object",
    "varbinary": "object",
    "image": "object"
}


class SourceColumn(wysdom.UserObject):

    type: str = wysdom.UserProperty(str)
    nullable: bool = wysdom.UserProperty(bool)
    pk: Optional[bool] = wysdom.UserProperty(bool, default=False)

    # TODO: Modify type definitions so no text manipulation is needed
    @property
    def pandas_dtype(self):
        return PANDAS_DTYPE_MAPPINGS[self.type.split("(")[0].lower()]
