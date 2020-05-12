from __future__ import annotations

from typing import Generic, TypeVar, Optional, Any

from collections.abc import Mapping

from ..base_schema import JSONSchema, JSONSchemaAnything
from .JSONSchemaDOMObject import JSONSchemaDOMObject
from . import JSONSchemaDOMInfo
from .JSONSchemaDOMProperties import JSONSchemaDOMProperties

T_co = TypeVar('T_co')


class JSONSchemaDOMDict(JSONSchemaDOMObject, Generic[T_co]):

    _additional_properties: JSONSchema = None

    def __init__(
            self,
            value: Optional[Mapping[str, Any]] = None,
            json_dom_info: Optional[JSONSchemaDOMInfo] = None,
            _item_type: Optional[JSONSchema] = None
    ) -> None:
        self.__json_schema_properties__ = JSONSchemaDOMProperties(
            additional_properties=(_item_type or JSONSchemaAnything())
        )
        super().__init__(
            value or {},
            json_dom_info
        )

    def __getitem__(self, key: str) -> T_co:
        return super().__getitem__(key)
