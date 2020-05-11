import jsdom


class SatelliteColumn(jsdom.Object):

    type: str = jsdom.Property(str)
    nullable: bool = jsdom.Property(bool, default=True)
    # TODO: Update jso so Optional[str] is a valid type, and explicitly include non-optional
    #       properties in required_properties
    hub_reference: str = jsdom.Property(str, default='')  # In future, default=None
    index: bool = jsdom.Property(bool, default=False)
