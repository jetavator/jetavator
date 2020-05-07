from __future__ import annotations

from typing import Generic, Type, TypeVar, Optional, Any

from collections.abc import Mapping

from .JSONSchemaObject import JSONSchemaObject
from .JSONSchemaElement import JSONSchemaElement, JSONSchemaDOMInfo
from .JSONSchemaGeneric import JSONSchemaGeneric


T_co = TypeVar('T_co', covariant=True, bound=JSONSchemaElement)


class JSONSchemaDict(JSONSchemaObject, JSONSchemaGeneric, Generic[T_co]):

    _additional_properties: Type[JSONSchemaElement] = JSONSchemaElement

    def __init__(
            self,
            value: Optional[Mapping[str, Any]] = None,
            dom_info: JSONSchemaDOMInfo = None,
            _item_type: Optional[Type[T_co]] = None,
            **kwargs: Any
    ) -> None:
        if _item_type is not None:
            self._additional_properties = _item_type
        super().__init__(
            value or {},
            dom_info,
            **kwargs
        )

    def __getitem__(self, key: str) -> T_co:
        return super().__getitem__(key)
