from typing import Optional, Dict, Type, Any, Union

from ..dom import JSONSchemaDOMInfo
from .JSONSchemaType import JSONSchemaType
from ..JSONSchema import JSONSchema


class JSONSchemaObject(JSONSchemaType):

    type_name: str = "object"
    properties: Optional[Dict[str, JSONSchema]] = None
    additional_properties: Union[bool, JSONSchema] = False

    def __init__(
            self,
            properties: Optional[Dict[str, JSONSchema]] = None,
            additional_properties: Union[bool, JSONSchema] = False,
            object_type: Type = dict
    ) -> None:
        self.properties = properties or {}
        self.additional_properties = additional_properties
        self.object_type = object_type

    def __call__(
            self,
            value: Any,
            dom_info: JSONSchemaDOMInfo = None
    ) -> Any:
        return self.object_type(value, dom_info, _schema=self)

    @property
    def schema(self) -> Dict[str, Any]:
        return {
            **super().schema,
            'properties': {
                k: v.schema
                for k, v in self.properties.items()
            },
            "additionalProperties": (
                self.additional_properties.schema
                if isinstance(self.additional_properties, JSONSchema)
                else self.additional_properties
            )
        }


