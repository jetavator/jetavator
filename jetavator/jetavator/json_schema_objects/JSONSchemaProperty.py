from typing import Type, Any, Optional, Union

from .JSONSchemaElement import JSONSchemaElement
from .JSONSchemaGeneric import JSONSchemaGenericProxy


class JSONSchemaProperty(object):

    def __init__(
            self,
            schema_type: Union[Type[JSONSchemaElement], JSONSchemaGenericProxy],
            name: Optional[str] = None,
            default: Optional[Any] = None
    ) -> None:
        self.name = name
        self.schema_type = schema_type
        self.default = default

    def __get__(
            self,
            instance: JSONSchemaElement,
            owner: Type[JSONSchemaElement]
    ) -> Any:
        if callable(self.default) and self.name not in instance:
            instance[self.name] = self.default(instance)
            return instance[self.name]
        else:
            return instance.get(self.name, self.default)

    def __set__(
            self,
            instance: JSONSchemaElement,
            value: Any
    ) -> None:
        instance[self.name] = value
