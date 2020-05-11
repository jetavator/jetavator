from jetavator import json_schema_objects as jso


class SatelliteColumn(jso.Object):

    type: str = jso.Property(str)
    nullable: bool = jso.Property(bool, default=True)
    # TODO: Update jso so Optional[str] is a valid type, and explicitly include non-optional
    #       properties in required_properties
    hub_reference: str = jso.Property(str, default='')  # In future, default=None
    index: bool = jso.Property(bool, default=False)
