from typing import Optional

import jsdom


class SourceColumn(jsdom.Object):

    type: str = jsdom.Property(str)
    nullable: bool = jsdom.Property(bool)
    pk: Optional[bool] = jsdom.Property(bool, default=False)
