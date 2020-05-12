from typing import Any, Union, Type, Dict, Iterable

from ..dom import JSONSchemaDOMInfo, JSONSchemaDOMList
from ..base_schema import JSONSchema

from .resolve_arg_to_type import resolve_arg_to_schema


class JSONSchemaArray(JSONSchema):

    items: JSONSchema = None

    def __init__(
            self,
            items: Union[Type, JSONSchema]
    ) -> None:
        self.items = resolve_arg_to_schema(items)

    def __call__(
            self,
            value: Iterable,
            dom_info: JSONSchemaDOMInfo = None
    ) -> Any:
        return JSONSchemaDOMList(
            value,
            dom_info,
            _item_type=self.items
        )

    @property
    def schema(self) -> Dict[str, Any]:
        return {
            "array": {
                "items": self.items
            }
        }
