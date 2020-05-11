from typing import Any

import jsonschema

from ..JSONSchema import JSONSchema
from ..schema import JSONSchemaObject, JSONSchemaAnyOf
from ..dom import JSONSchemaDOMObject

from .JSONSchemaUserProperty import JSONSchemaUserProperty


# TODO: Rather than inheriting from JSONSchemaDOMObject, have the
#       JSONSchemaDOMObject as a private instance to reduce the
#       number of private members that user subclasses will have
#       to inherit?

class JSONSchemaUserObject(JSONSchemaDOMObject):

    def __init_subclass__(cls, *args: Any, **kwargs: Any) -> None:
        cls._populate_properties()
        cls._update_inherited_properties()
        super().__init_subclass__(*args, **kwargs)

    @classmethod
    def _populate_properties(cls) -> None:
        class_properties = {}
        class_properties.update(cls._properties)
        for k, v in cls.__dict__.items():
            if isinstance(v, JSONSchemaUserProperty):
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
    def __json_schema__(cls) -> JSONSchema:
        if cls.registered_subclasses():
            return JSONSchemaAnyOf(
                subclass.__json_schema__()
                for subclass in cls.registered_subclasses().values()
            )
        else:
            return JSONSchemaObject(
                properties=cls._properties,
                additional_properties=cls._additional_properties,
                object_type=cls
            )

    @classmethod
    def _validate_item(cls, value: Any) -> None:
        if value is None:
            raise Exception("Value cannot be None.")
        jsonschema.validate(instance=value, schema=cls.__json_schema__().schema)

    def _validate(self) -> None:
        self._validate_item(self._value)