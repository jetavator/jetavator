from typing import Any, Union, Type, Dict, Iterable

from ..dom import JSONSchemaDOMInfo, JSONSchemaDOMList
from ..JSONSchema import JSONSchema

from .get_schema_from_type import get_schema_from_type


class JSONSchemaArray(JSONSchema):

    items: JSONSchema = None

    def __init__(
            self,
            items: Union[Type, JSONSchema]
    ) -> None:
        self.items = get_schema_from_type(items)

    def __call__(
            self,
            value: Iterable,
            dom_info: JSONSchemaDOMInfo = None
    ) -> Any:
        return JSONSchemaDOMList(
            value,
            dom_info,
            _schema=self,
            _item_type=self.items
        )

    @property
    def schema(self) -> Dict[str, Any]:
        return {
            "array": {
                "items": self.items
            }
        }
