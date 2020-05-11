from typing import Any, Dict

from ..dom import JSONSchemaDOMInfo
from ..JSONSchema import JSONSchema


class JSONSchemaAnything(JSONSchema):

    def __call__(
            self,
            value: Any,
            dom_info: JSONSchemaDOMInfo = None
    ) -> Any:
        return value

    @property
    def schema(self) -> Dict[str, Any]:
        return {}
