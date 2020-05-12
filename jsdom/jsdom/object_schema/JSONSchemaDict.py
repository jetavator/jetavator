from typing import Any, Type, Union

from ..dom import JSONSchemaDOMInfo, JSONSchemaDOMDict

from .JSONSchemaObject import JSONSchemaObject
from ..base_schema import JSONSchema
from .resolve_arg_to_type import resolve_arg_to_schema


class JSONSchemaDict(JSONSchemaObject):

    def __init__(
            self,
            items: Union[Type, JSONSchema]
    ) -> None:
        super().__init__(
            additional_properties=resolve_arg_to_schema(items)
        )

    def __call__(
            self,
            value: Any,
            dom_info: JSONSchemaDOMInfo = None
    ) -> Any:
        return JSONSchemaDOMDict(
            value,
            dom_info,
            _item_type=self.additional_properties
        )
