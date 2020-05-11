from typing import Any

from ..dom import JSONSchemaDOMInfo
from .JSONSchemaType import JSONSchemaType


class JSONSchemaNone(JSONSchemaType):

    type_name: str = 'null'

    def __call__(
            self,
            value: None,
            dom_info: JSONSchemaDOMInfo = None
    ) -> Any:
        if value is not None:
            raise ValueError('Value can only be None.')
        return None
