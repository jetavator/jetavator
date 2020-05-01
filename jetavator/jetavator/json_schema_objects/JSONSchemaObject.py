from __future__ import annotations

from typing import Any, Iterator

import inspect
import json

from jsonschema.validators import validator_for

from copy import deepcopy

from .JSONSchemaElement import JSONSchemaElement
from .JSONSchemaProperty import JSONSchemaProperty

from jetavator.mixins import RegistersSubclasses


class JSONSchemaObject(dict, JSONSchemaElement, RegistersSubclasses):

    properties = {}
    additional_properties: Optional[Type[JSONSchemaElement]] = None
    index = None

    def __init_subclass__(cls, *args: Any, **kwargs: Any) -> None:
        cls._populate_properties()
        cls._update_inherited_properties()
        super().__init_subclass__(*args, **kwargs)

    def __init__(
            self,
            *args: Any,
            _document: JSONSchemaElement = None,
            **kwargs: Any
    ) -> None:
        if _document is None:
            self._document = self
        else:
            self._document = _document
        super().__init__({
            k: (
                self.properties[k]._instance_for_item(v, _document=self._document)
                if k in self.properties
                else (
                    self.additional_properties._instance_for_item(v)
                    if self.additional_properties
                    else v
                )
            )
            for k, v in dict(*args, **kwargs).items()
        })
        self._populate_properties()

    @classmethod
    def _populate_properties(cls) -> None:
        class_properties = {}
        for k, v in cls.__dict__.items():
            if isinstance(v, JSONSchemaProperty):
                if not v.name:
                    v.name = k
                class_properties[v.name] = v.schema_type
        class_properties.update(cls.properties)
        cls.properties = class_properties

    @classmethod
    def _update_inherited_properties(cls) -> None:
        inherited_properties = {}
        for superclass in reversed(list(cls._schema_superclasses())):
            inherited_properties.update(superclass.properties)
        cls.properties = inherited_properties

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
        additional_properties = item_type or cls.additional_properties
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
                    for k, v in cls.properties.items()
                },
                "additionalProperties": (
                    additional_properties._schema()
                    if additional_properties
                    else False
                )
            }

    def _to_json(self) -> str:
        return json.JSONEncoder().encode(self)

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

    def __copy__(self) -> JSONSchemaObject:
        cls = self.__class__
        return cls(dict(self))

    def __deepcopy__(self, memo: Dict[int, JSONSchemaElement]) -> JSONSchemaObject:
        cls = self.__class__
        result = cls(deepcopy(dict(self), memo))
        memo[id(self)] = result
        return result
