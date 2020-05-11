from __future__ import annotations

from typing import Generic, TypeVar, Optional, Any

from collections.abc import Mapping

from ..JSONSchema import JSONSchema
from .JSONSchemaDOMObject import JSONSchemaDOMObject
from .JSONSchemaDOMInfo import JSONSchemaDOMInfo

T_co = TypeVar('T_co')


class JSONSchemaDOMDict(JSONSchemaDOMObject, Generic[T_co]):

    _additional_properties: JSONSchema = None

    # TODO: _item_type cannot be None.
    #       Allow this class to reference JSONSchemaAnything,
    #       or just make JSONSchemaAnything the default JSONSchema?

    # TODO: Check if _schema is actually used?

    def __init__(
            self,
            value: Optional[Mapping[str, Any]] = None,
            _dom_info: Optional[JSONSchemaDOMInfo] = None,
            _schema: Optional[JSONSchema] = None,
            _item_type: Optional[JSONSchema] = None,
            **kwargs: Any
    ) -> None:
        if _item_type is not None:
            self._additional_properties = _item_type
        super().__init__(
            value or {},
            _dom_info,
            _schema,
            **kwargs
        )

    def __getitem__(self, key: str) -> T_co:
        return super().__getitem__(key)
