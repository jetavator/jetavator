from jetavator import json_schema_objects as jso


class SatelliteColumn(jso.Object):

    type: str = jso.Property(jso.String)
    nullable: bool = jso.Property(jso.Bool)
    hub_reference: str = jso.Property(jso.String)
