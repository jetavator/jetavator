from __future__ import annotations

from typing import Any, Iterator, NamedTuple, Optional

from abc import ABC, abstractmethod

from ..base_schema import JSONSchema, JSONSchemaAnything


class JSONSchemaDOMInfo(NamedTuple):

    element: Optional[JSONSchemaDOMElement] = None
    document: Optional[JSONSchemaDOMElement] = None
    parent: Optional[JSONSchemaDOMElement] = None
    element_key: Optional[str] = None


class JSONSchemaDOMElement(ABC):

    __json_dom_info__: JSONSchemaDOMInfo = None

    @abstractmethod
    def __init__(
            self,
            value: Any = None,
            json_dom_info: JSONSchemaDOMInfo = None,
            **kwargs: Any
    ) -> None:
        if json_dom_info:
            self.__json_dom_info__ = JSONSchemaDOMInfo(
                element=self,
                document=(
                    self if json_dom_info.document is None
                    else json_dom_info.document
                ),
                parent=json_dom_info.parent,
                element_key=json_dom_info.element_key
            )
        else:
            self.__json_dom_info__ = JSONSchemaDOMInfo(
                element=self, document=self)

    @classmethod
    def __json_schema__(cls) -> JSONSchema:
        return JSONSchemaAnything()

    @abstractmethod
    def to_builtin(self) -> Any:
        pass

    def walk_elements(self) -> Iterator[JSONSchemaDOMInfo]:
        yield self.__json_dom_info__
