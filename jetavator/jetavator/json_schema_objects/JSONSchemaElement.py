from __future__ import annotations

from typing import Optional

from abc import ABC, abstractmethod

import jsonschema


class JSONSchemaDOMInfo(object):

    def __init__(
            self,
            element: Optional[JSONSchemaElement] = None,
            document: Optional[JSONSchemaElement] = None,
            parent: Optional[JSONSchemaElement] = None,
            element_key: Optional[str] = None
    ) -> None:
        self._element = element
        self._document = document
        self._parent = parent
        self._element_key = element_key

    @property
    def element(self) -> JSONSchemaElement:
        return self._element

    @property
    def document(self) -> JSONSchemaElement:
        return self._element if self._document is None else self._document

    @property
    def parent(self) -> Optional[JSONSchemaElement]:
        return self._parent

    @property
    def element_key(self) -> Optional[str]:
        return self._element_key


class JSONSchemaElement(ABC):

    __json_dom_info__: JSONSchemaDOMInfo = None

    @abstractmethod
    def __init__(
            self,
            value: Any = None,
            dom_info: JSONSchemaDOMInfo = None,
            **kwargs: Any
    ) -> None:
        # TODO: refactor so this doesn't require creating a new
        #       JSONSchemaDOMInfo object
        if dom_info:
            self.__json_dom_info__ = JSONSchemaDOMInfo(
                self, dom_info.document, dom_info.parent, dom_info.element_key
            )
        else:
            self.__json_dom_info__ = JSONSchemaDOMInfo(self)

    @classmethod
    @abstractmethod
    def _schema(cls) -> Dict[str, Any]:
        return {}

    @property
    @abstractmethod
    def _value(self) -> Any:
        pass

    @classmethod
    def _class_for_item(cls, value: Any) -> Type[JSONSchemaElement]:
        cls._validate_item(value)
        return cls

    @classmethod
    def _instance_for_item(
            cls,
            value: Any,
            dom_info: JSONSchemaDOMInfo = None,
            **kwargs: Any
    ) -> Dict[str, Any]:
        return cls._class_for_item(value)(value, dom_info, **kwargs)

    @classmethod
    def _validate_item(cls, value: Any) -> None:
        if value is None:
            raise Exception("Value cannot be None.")
        jsonschema.validate(instance=value, schema=cls._schema())

    def _validate(self) -> None:
        self._validate_item(self._value)

    def _walk(self) -> Iterator[Tuple[JSONSchemaElement, Optional[str], JSONSchemaElement]]:
        yield self, None, self
