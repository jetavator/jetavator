from jetavator import json_schema_objects as jso


class SatelliteColumn(jso.Object):

    type: str = jso.Property(jso.String)
    nullable: bool = jso.Property(jso.Boolean, default=True)
    # TODO: Update jso so Optional[str] is a valid type, and explicitly include non-optional
    #       properties in required_properties
    hub_reference: str = jso.Property(jso.String, default='')  # In future, default=None
    index: bool = jso.Property(jso.Boolean, default=False)
