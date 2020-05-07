from typing import Type, Any

from jetavator import json_schema_objects as jso


class ConfigProperty(jso.Property):

    def __get__(
            self,
            instance: jso.Element,
            owner: Type[jso.Element]
    ) -> Any:
        return jso.document(instance).secret_lookup(
            super().__get__(instance, owner)
        )
