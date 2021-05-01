from typing import Optional

import wysdom

from .Column import Column


class SatelliteColumn(Column):

    nullable: bool = wysdom.UserProperty(bool, default=True)
    hub_reference: Optional[str] = wysdom.UserProperty(str, optional=True)
    index: bool = wysdom.UserProperty(bool, default=False)
