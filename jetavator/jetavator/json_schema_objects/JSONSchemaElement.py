from __future__ import annotations

from abc import ABC, abstractmethod

import jsonschema


class JSONSchemaElement(ABC):

    def __init__(
            self,
            *args: Any,
            _document: JSONSchemaElement = None,
            **kwargs: Any
    ) -> None:
        self._document = _document or self

    @classmethod
    @abstractmethod
    def _schema(cls) -> Dict[str, Any]:
        return {}

    @classmethod
    def _class_for_item(cls, item: Any) -> Dict[str, Any]:
        try:
            jsonschema.validate(instance=item, schema=cls._schema())
        except Exception:
            raise Exception("foo")
        return cls

    @classmethod
    def _instance_for_item(
            cls,
            item: Any,
            _document: Optional[T_co] = None,
            **kwargs: Any
    ) -> Dict[str, Any]:
        return cls._class_for_item(item)(item, _document=_document, **kwargs)

    def _validate(self) -> None:
        jsonschema.validate(instance=self, schema=self._schema())

    def _walk(self) -> Iterator[Tuple[JSONSchemaElement, Optional[str], JSONSchemaElement]]:
        yield self, None, self
