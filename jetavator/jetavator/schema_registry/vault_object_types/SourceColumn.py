from typing import Optional

import wysdom


class SourceColumn(wysdom.UserObject):

    type: str = wysdom.UserProperty(str)
    nullable: bool = wysdom.UserProperty(bool)
    pk: Optional[bool] = wysdom.UserProperty(bool, default=False)
