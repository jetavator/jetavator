from typing import Any, Dict

from ..dom import JSONSchemaDOMInfo
from ..JSONSchema import JSONSchema


class JSONSchemaConst(JSONSchema):

    value: str = None

    def __init__(
            self,
            value: str
    ) -> None:
        self.value = value

    def __call__(
            self,
            value: str,
            dom_info: JSONSchemaDOMInfo = None
    ) -> Any:
        if value != self.value:
            raise ValueError(f"Value can only be '{self.value}'.")
        return self.value

    @property
    def schema(self) -> Dict[str, Any]:
        return {
            "const": self.value
        }
