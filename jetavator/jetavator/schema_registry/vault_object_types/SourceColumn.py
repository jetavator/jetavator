from typing import Optional

from jetavator import json_schema_objects as jso


class SourceColumn(jso.Object):

    type: str = jso.Property(str)
    nullable: bool = jso.Property(bool)
    pk: Optional[bool] = jso.Property(bool, default=False)
