import wysdom


class SatelliteColumn(wysdom.UserObject):

    type: str = wysdom.UserProperty(str)
    nullable: bool = wysdom.UserProperty(bool, default=True)
    # TODO: Update jso so Optional[str] is a valid type, and explicitly include non-optional
    #       properties in required_properties
    hub_reference: str = wysdom.UserProperty(str, default='')  # In future, default=None
    index: bool = wysdom.UserProperty(bool, default=False)
