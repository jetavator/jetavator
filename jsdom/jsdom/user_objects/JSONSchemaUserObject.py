from __future__ import annotations

from typing import Any, Type, Iterator, Union, Mapping

import inspect

from ..base_schema import JSONSchema
from ..object_schema import JSONSchemaObject, JSONSchemaAnyOf
from ..dom import (
    JSONSchemaDOMObject,
    JSONSchemaDOMProperties,
    JSONSchemaDOMInfo
)

from .JSONSchemaUserProperty import JSONSchemaUserProperty


class JSONSchemaUserProperties(JSONSchemaDOMProperties):

    def __init__(
            self,
            user_class: Type[JSONSchemaUserObject],
            additional_properties: Union[bool, JSONSchema] = False
    ):
        self._user_class = user_class
        properties = {}
        for superclass in reversed(list(self._schema_superclasses())):
            for k, v in superclass.__dict__.items():
                if isinstance(v, JSONSchemaUserProperty):
                    if not v.name:
                        v.name = k
                    properties[v.name] = v.schema_type
        super().__init__(properties, additional_properties)

    def _schema_superclasses(self) -> Iterator[Type[JSONSchemaUserObject]]:
        for superclass in inspect.getmro(self._user_class):
            if (
                    issubclass(superclass, JSONSchemaUserObject)
                    and superclass is not JSONSchemaUserObject
            ):
                yield superclass


class JSONSchemaUserObject(JSONSchemaDOMObject):
    """
    Base class for user-defined DOM objects.

    Example usage:

    class Address(JSONSchemaUserObject):
        first_line = JSONSchemaUserProperty(str)
        second_line = JSONSchemaUserProperty(str)
        city = JSONSchemaUserProperty(str)
        postal_code = JSONSchemaUserProperty(int)

    class Person(JSONSchemaUserObject):
        first_name = JSONSchemaUserProperty(str)
        last_name = JSONSchemaUserProperty(str)
        current_address = JSONSchemaUserProperty(Address)
        previous_addresses = JSONSchemaUserProperty(JSONSchemaList(Address))

    :param value:         A dict-like object to populate the underlying data
                          object's keys. May be provided alone or in conjunction
                          with keyword arguments.

    :param json_dom_info: A named tuple with parameters (element, document, parent,
                          element_key) which specify this object's position in a
                          larger document object model. This is only required if you
                          need to set these values when creating an object manually:
                          when objects are created as part of a DOM specification,
                          these values are populated automatically.

    :param kwargs:        Keyword arguments to be used to populate the underlying
                          data object's keys. May be provided alone or in
                          conjunction with `value`.
    """

    __json_schema_properties__: JSONSchemaUserProperties = None

    def __init_subclass__(
            cls,
            *args: Any,
            additional_properties: Union[bool, JSONSchema] = False,
            **kwargs: Any
    ) -> None:
        cls.__json_schema_properties__ = JSONSchemaUserProperties(cls)
        super().__init_subclass__(*args, **kwargs)

    def __init__(
            self,
            value: Mapping[str, Any] = None,
            json_dom_info: JSONSchemaDOMInfo = None,
            **kwargs: Any
    ) -> None:
        if json_dom_info:
            if not isinstance(json_dom_info, JSONSchemaDOMInfo):
                raise TypeError(
                    "The name json_dom_info is a reserved parameter of "
                    "JSONSchemaUserObject and must be of type "
                    f"JSONSchemaDOMInfo, not {type(json_dom_info)}"
                )
        super().__init__({**(value or {}), **kwargs}, json_dom_info)

    @classmethod
    def __json_schema__(cls) -> JSONSchema:
        if cls.registered_subclasses():
            return JSONSchemaAnyOf(
                subclass.__json_schema__()
                for subclass in cls.registered_subclasses().values()
                if issubclass(subclass, JSONSchemaUserObject)
            )
        else:
            return JSONSchemaObject(
                properties=cls.__json_schema_properties__.properties,
                additional_properties=cls.__json_schema_properties__.additional_properties,
                object_type=cls
            )
