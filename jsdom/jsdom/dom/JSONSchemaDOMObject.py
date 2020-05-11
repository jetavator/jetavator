from __future__ import annotations

from typing import Any, Iterator, Dict

from collections.abc import Mapping, MutableMapping

import inspect
import json

from copy import deepcopy

from jsdom.mixins import RegistersSubclasses

from ..exceptions import JSONSchemaValidationError
from ..JSONSchema import JSONSchema

from .JSONSchemaDOMElement import JSONSchemaDOMElement
from .JSONSchemaDOMInfo import JSONSchemaDOMInfo
from .functions import document


class JSONSchemaDOMObject(JSONSchemaDOMElement, MutableMapping, RegistersSubclasses):
    _element_data: Dict[str, JSONSchemaDOMElement] = None
    _properties: Dict[str, JSONSchema] = {}
    _additional_properties: Union[bool, JSONSchema] = False

    # TODO: Either remove dom_info and schema, or remove kwargs,
    #       because "schema" is a common dictionary key

    def __init__(
            self,
            value: Mapping[str, Any] = None,
            _dom_info: JSONSchemaDOMInfo = None,
            _schema: JSONSchema = None,
            **kwargs: Any
    ) -> None:
        if value and not isinstance(value, Mapping):
            raise JSONSchemaValidationError(
                f"Cannot validate input. Object is not a mapping: {value}"
            )
        super().__init__(value, _dom_info, _schema, **kwargs)
        self._element_data = {}
        # self._populate_properties()
        for key, value in {**(value or {}), **kwargs}.items():
            self[key] = value

    def __getitem__(self, key: str) -> JSONSchemaDOMElement:
        return self._element_data[key]

    def __setitem__(self, key: str, value: JSONSchemaDOMElement) -> None:
        item_class = self._properties.get(key, self._additional_properties)
        # TODO: refactor so item_class is not True
        #       in case of self._additional_properties = True
        if item_class:
            self._element_data[key] = item_class(
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
        return f"{self.__class__.__name__}({repr(self._element_data)})"

    def __str__(self):
        return str(self._element_data)

    @classmethod
    def _schema_superclasses(cls) -> Iterator[Type[JSONSchemaDOMElement]]:
        for superclass in inspect.getmro(cls):
            if (
                    issubclass(superclass, JSONSchemaDOMElement)
                    and superclass is not JSONSchemaDOMElement
            ):
                yield superclass

    # TODO: If a property type in a class refers to that class or
    #       its parent class, it will create an infinite recursive
    #       loop when building the schema. Avoid this by using:
    #       https://json-schema.org/understanding-json-schema/structuring.html#recursion

    def _to_json(self) -> str:
        return json.JSONEncoder().encode(self._value)

    @staticmethod
    def _decode_json(json_string: str) -> Dict[str, Any]:
        return json.JSONDecoder().decode(json_string)

    @classmethod
    def from_json(
            cls,
            json_string: str,
            validate: bool = True
    ) -> JSONSchemaDOMObject:
        new_object = cls(cls._decode_json(json_string))
        if validate:
            new_object._validate()
        return new_object

    @classmethod
    def from_json_file(cls, filename: str) -> JSONSchemaDOMObject:
        with open(filename) as json_file:
            return cls(json.load(json_file))

    def _walk(self) -> Iterator[Tuple[JSONSchemaDOMElement, Optional[str], JSONSchemaDOMElement]]:
        for key, value in self.items():
            if isinstance(value, JSONSchemaDOMElement):
                yield from value._walk()
            else:
                yield self, key, value

    @property
    def _value(self) -> Dict[str, Any]:
        return {
            k: getattr(v, "_value", v)
            for k, v in self.items()
        }

    def __copy__(self) -> JSONSchemaDOMObject:
        cls = self.__class__
        return cls(dict(self))

    def __deepcopy__(self, memo: Dict[int, JSONSchemaDOMElement]) -> JSONSchemaDOMObject:
        cls = self.__class__
        result = cls(deepcopy(dict(self), memo))
        memo[id(self)] = result
        return result
