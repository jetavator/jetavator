from typing import Optional

from jetavator import json_schema_objects as jso


class SourceColumn(jso.Object):

    type: str = jso.Property(jso.String)
    nullable: bool = jso.Property(jso.Bool)
    pk: Optional[bool] = jso.Property(jso.Bool, default=False)
