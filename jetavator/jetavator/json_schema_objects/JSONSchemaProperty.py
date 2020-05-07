from typing import Type, Any, Optional, Union, Callable

from .JSONSchemaElement import JSONSchemaElement
from .JSONSchemaGeneric import JSONSchemaGenericProxy


class JSONSchemaProperty(object):

    def __init__(
            self,
            schema_type: Union[Type[JSONSchemaElement], JSONSchemaGenericProxy],
            name: Optional[str] = None,
            default: Optional[Any] = None,
            default_function: Optional[Callable] = None
    ) -> None:
        if default and default_function:
            raise ValueError("Cannot use both default and default_function.")
        self.name = name
        self.schema_type = schema_type
        self.default = default
        self.default_function = default_function

    def __get__(
            self,
            instance: JSONSchemaElement,
            owner: Type[JSONSchemaElement]
    ) -> Any:
        if instance is None:
            raise AttributeError(
                "JSONSchemaProperty is not valid as a class descriptor")
        if self.name not in instance:
            if self.default_function:
                instance[self.name] = self.default_function(instance)
            else:
                instance[self.name] = self.default
        return instance[self.name]

    def __set__(
            self,
            instance: JSONSchemaElement,
            value: Any
    ) -> None:
        instance[self.name] = value
