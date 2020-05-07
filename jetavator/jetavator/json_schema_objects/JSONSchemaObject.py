from __future__ import annotations

from typing import Any, Iterator, Dict

from collections.abc import Mapping, MutableMapping

import inspect
import json

from jsonschema.validators import validator_for

from copy import deepcopy

from jetavator.mixins import RegistersSubclasses

from .JSONSchemaElement import JSONSchemaElement, JSONSchemaDOMInfo
from . import document
from .JSONSchemaProperty import JSONSchemaProperty
from .JSONSchemaValidationError import JSONSchemaValidationError


class JSONSchemaObject(JSONSchemaElement, MutableMapping, RegistersSubclasses):
    _element_data: Dict[str, JSONSchemaElement] = None
    _properties = {}
    _additional_properties: Optional[Type[JSONSchemaElement]] = None

    def __init_subclass__(cls, *args: Any, **kwargs: Any) -> None:
        cls._populate_properties()
        cls._update_inherited_properties()
        super().__init_subclass__(*args, **kwargs)

    def __init__(
            self,
            value: Mapping[str, Any] = None,
            dom_info: JSONSchemaDOMInfo = None,
            **kwargs: Any
    ) -> None:
        if value and not isinstance(value, Mapping):
            raise JSONSchemaValidationError(
                f"Cannot validate input. Object is not a mapping: {value}"
            )
        super().__init__(value, dom_info, **kwargs)
        self._element_data = {}
        self._populate_properties()
        for key, value in {**(value or {}), **kwargs}.items():
            self[key] = value

    def __getitem__(self, key: str) -> JSONSchemaElement:
        return self._element_data[key]

    def __setitem__(self, key: str, value: JSONSchemaElement) -> None:
        item_class = self._properties.get(key, self._additional_properties)
        if item_class:
            self._element_data[key] = item_class._instance_for_item(
                value,
                JSONSchemaDOMInfo(
                    document=document(self),
                    parent=self,
                    element_key=key
                )
            )
        else:
            self._element_data[key] = value

    def __delitem__(self, key: str) -> None:
        del self._element_data[key]

    def __len__(self) -> int:
        return len(self._element_data)

    def __iter__(self) -> Iterator[str]:
        return iter(self._element_data)

    def __repr__(self):
        return repr(self._element_data)

    def __str__(self):
        return str(self._element_data)

    @classmethod
    def _populate_properties(cls) -> None:
        class_properties = {}
        class_properties.update(cls._properties)
        for k, v in cls.__dict__.items():
            if isinstance(v, JSONSchemaProperty):
                if not v.name:
                    v.name = k
                class_properties[v.name] = v.schema_type
        cls._properties = class_properties

    @classmethod
    def _update_inherited_properties(cls) -> None:
        inherited_properties = {}
        for superclass in reversed(list(cls._schema_superclasses())):
            inherited_properties.update(superclass._properties)
        cls._properties = inherited_properties

    @classmethod
    def _schema_superclasses(cls) -> Iterator[Type[JSONSchemaElement]]:
        for superclass in inspect.getmro(cls):
            if (
                    issubclass(superclass, JSONSchemaElement)
                    and superclass is not JSONSchemaElement
            ):
                yield superclass

    @classmethod
    def _class_for_item(cls, item: Any) -> Dict[str, Any]:
        if cls.registered_subclasses():
            for subclass in cls.registered_subclasses().values():
                schema = subclass._schema()
                if validator_for(schema)(schema).is_valid(item):
                    return subclass
        else:
            return super()._class_for_item(item)

    # TODO: If a property type in a class refers to that class or
    #       its parent class, it will create an infinite recursive
    #       loop when building the schema. Avoid this by using:
    #       https://json-schema.org/understanding-json-schema/structuring.html#recursion

    @classmethod
    def _schema(
            cls,
            item_type: Optional[Type[JSONSchemaElement]] = None
    ) -> Dict[str, Any]:
        additional_properties = item_type or cls._additional_properties
        if cls.registered_subclasses():
            return {
                'anyOf': [
                    subclass._schema()
                    for subclass in cls.registered_subclasses().values()
                ]
            }
        else:
            return {
                'type': 'object',
                'properties': {
                    k: v._schema()
                    for k, v in cls._properties.items()
                },
                "additionalProperties": (
                    additional_properties._schema()
                    if additional_properties
                    else False
                )
            }

    def _to_json(self) -> str:
        return json.JSONEncoder().encode(self._value)

    @staticmethod
    def _decode_json(json_string: str) -> Dict[str, Any]:
        return json.JSONDecoder().decode(json_string)

    @classmethod
    def _from_json(
            cls,
            json_string: str,
            validate: bool = True
    ) -> JSONSchemaObject:
        new_object = cls(cls._decode_json(json_string))
        if validate:
            new_object._validate()
        return new_object

    @classmethod
    def _from_json_file(cls, filename: str) -> JSONSchemaObject:
        with open(filename) as json_file:
            return cls(json.load(json_file))

    def _walk(self) -> Iterator[Tuple[JSONSchemaElement, Optional[str], JSONSchemaElement]]:
        for key, value in self.items():
            if isinstance(value, JSONSchemaElement):
                yield from value._walk()
            else:
                yield self, key, value

    @property
    def _value(self) -> Dict[str, Any]:
        return {
            k: getattr(v, "_value", v)
            for k, v in self.items()
        }

    def __copy__(self) -> JSONSchemaObject:
        cls = self.__class__
        return cls(dict(self))

    def __deepcopy__(self, memo: Dict[int, JSONSchemaElement]) -> JSONSchemaObject:
        cls = self.__class__
        result = cls(deepcopy(dict(self), memo))
        memo[id(self)] = result
        return result
