from typing import Optional

import wysdom


class SatelliteColumn(wysdom.UserObject):

    type: str = wysdom.UserProperty(str)
    nullable: bool = wysdom.UserProperty(bool, default=True)
    hub_reference: Optional[str] = wysdom.UserProperty(str, optional=True)
    index: bool = wysdom.UserProperty(bool, default=False)
