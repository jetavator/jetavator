from typing import Optional

import wysdom

from .Column import Column


class SourceColumn(Column):

    nullable: bool = wysdom.UserProperty(bool)
    pk: Optional[bool] = wysdom.UserProperty(bool, default=False)
