from typing import Any, Dict, Tuple

from .JSONSchema import JSONSchema


class JSONSchemaAnything(JSONSchema):

    def __call__(
            self,
            value: Any,
            dom_info: Tuple = None
    ) -> Any:
        return value

    @property
    def schema(self) -> Dict[str, Any]:
        return {}
