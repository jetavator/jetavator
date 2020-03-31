import inspect
import json
import re
import os

from copy import deepcopy

from .JSONSchemaElement import JSONSchemaElement
from jetavator.mixins import RegistersSubclasses


class JSONSchemaObject(dict, JSONSchemaElement, RegistersSubclasses):

    properties = {}
    additional_properties = False
    index = None

    def __init_subclass__(cls, *args, **kwargs):
        cls._update_inherited_properties()
        super().__init_subclass__(*args, **kwargs)

    def __init__(self, *args, _document=None, **kwargs):
        if _document is None:
            self._document = self
        else:
            self._document = _document
        super().__init__({
            k: (
                self.properties[k](v, _document=self._document)
                if k in self.properties
                else v
            )
            for k, v in dict(*args, **kwargs).items()
        })

    def __getattr__(self, key):
        if key not in self.properties.keys():
            raise AttributeError(f'Attribute not found: {key}')
        # TODO: Add default values here as well as just None
        return self.get(key, None)

    def __setattr__(self, key, value):
        if key in self.properties.keys():
            self[key] = value
        else:
            self.__dict__[key] = value

    @classmethod
    def _update_inherited_properties(cls):
        inherited_properties = {}
        for superclass in reversed(list(cls._schema_superclasses())):
            inherited_properties.update(superclass.properties)
        cls.properties = inherited_properties

    @classmethod
    def _schema_superclasses(cls):
        for superclass in inspect.getmro(cls):
            if (
                issubclass(superclass, JSONSchemaElement)
                and superclass is not JSONSchemaElement
            ):
                yield superclass

    @classmethod
    def _schema(cls):
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
                "additionalProperties": cls.additional_properties
            }

    def _to_json(self):
        return json.JSONEncoder().encode(self)

    @staticmethod
    def _decode_json(json_string):
        return json.JSONDecoder().decode(json_string)

    @classmethod
    def _from_json(cls, json_string, validate=True):
        new_object = cls(cls._decode_json(json_string))
        if validate:
            new_object._validate()
        return new_object

    @classmethod
    def _from_json_file(cls, filename):
        with open(filename) as json_file:
            return cls(json.load(json_file))

    def _walk(self):
        for key, value in self.items():
            if isinstance(value, JSONSchemaElement):
                yield from value._walk()
            else:
                yield (self, key, value)

    def __copy__(self):
        cls = self.__class__
        return cls(dict(self))

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls(deepcopy(dict(self), memo))
        memo[id(self)] = result
        return result
