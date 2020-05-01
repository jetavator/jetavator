from __future__ import annotations

from typing import Generic, TypeVar, Optional, Any

from .JSONSchemaObject import JSONSchemaObject
from .JSONSchemaElement import JSONSchemaElement
from .JSONSchemaGeneric import JSONSchemaGeneric


T_co = TypeVar('T_co', covariant=True, bound=JSONSchemaElement)


class JSONSchemaDict(JSONSchemaObject, JSONSchemaGeneric, Generic[T_co]):

    additional_properties: Type[JSONSchemaElement] = JSONSchemaElement

    def __init__(
            self,
            *args: Any,
            _document: JSONSchemaElement = None,
            _item_type: Optional[T_co] = None,
            **kwargs: Any
    ) -> None:
        if _item_type is not None:
            self.additional_properties = _item_type
        super().__init__(*args, _document=_document, **kwargs)

    def __getitem__(self, key: str) -> T_co:
        return super().__getitem__(key)
