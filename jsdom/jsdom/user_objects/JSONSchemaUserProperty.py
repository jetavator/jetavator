from typing import Type, Any, Union, Optional, Callable

from ..JSONSchema import JSONSchema
from ..schema import get_schema_from_type
from ..dom import JSONSchemaDOMElement


class JSONSchemaUserProperty(object):

    def __init__(
            self,
            property_type: Union[Type, JSONSchema],
            name: Optional[str] = None,
            default: Optional[Any] = None,
            default_function: Optional[Callable] = None
    ) -> None:
        if default and default_function:
            raise ValueError("Cannot use both default and default_function.")
        self.schema_type = get_schema_from_type(property_type)
        self.name = name
        self.default = default
        self.default_function = default_function

    def __get__(
            self,
            instance: JSONSchemaDOMElement,
            owner: Type[JSONSchemaDOMElement]
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
            instance: JSONSchemaDOMElement,
            value: Any
    ) -> None:
        instance[self.name] = value
