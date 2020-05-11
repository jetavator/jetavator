from typing import Any, Type, Union

from ..dom import JSONSchemaDOMInfo, JSONSchemaDOMDict

from .JSONSchemaObject import JSONSchemaObject
from ..JSONSchema import JSONSchema
from .get_schema_from_type import get_schema_from_type


class JSONSchemaDict(JSONSchemaObject):

    def __init__(
            self,
            items: Union[Type, JSONSchema]
    ) -> None:
        super().__init__(
            additional_properties=get_schema_from_type(items)
        )

    def __call__(
            self,
            value: Any,
            dom_info: JSONSchemaDOMInfo = None
    ) -> Any:
        return JSONSchemaDOMDict(
            value,
            dom_info,
            _schema=self,
            _item_type=self.additional_properties
        )
