from __future__ import annotations

from abc import abstractmethod

from collections.abc import MutableSequence

from typing import (
    Generic, TypeVar, Union, Optional, Any, Iterable,
    Dict, List, Iterator, overload
)

from copy import deepcopy

from ..base_schema import JSONSchema
from ..exceptions import JSONSchemaValidationError

from .JSONSchemaDOMElement import JSONSchemaDOMElement
from . import JSONSchemaDOMInfo
from .functions import document

T_co = TypeVar('T_co')


class JSONSchemaDOMList(JSONSchemaDOMElement, MutableSequence, Generic[T_co]):

    __json_element_data__: List[JSONSchemaDOMElement] = None

    def __init__(
            self,
            value: Iterable,
            json_dom_info: Optional[JSONSchemaDOMInfo] = None,
            _item_type: Optional[JSONSchema] = None
    ) -> None:
        if value and not isinstance(value, Iterable):
            raise JSONSchemaValidationError(
                f"Cannot validate input. Object is not iterable: {value}"
            )
        super().__init__(value, json_dom_info)
        self.__json_element_data__ = []
        if _item_type is not None:
            self.item_type = _item_type
        self[:] = value

    @overload
    @abstractmethod
    def __getitem__(self, i: int) -> T_co: ...

    @overload
    @abstractmethod
    def __getitem__(self, s: slice) -> MutableSequence[T_co]: ...

    def __getitem__(self, i: Union[int, slice]) -> Union[T_co, MutableSequence[T_co]]:
        return self.__json_element_data__[i]

    @overload
    @abstractmethod
    def __setitem__(self, i: int, o: T_co) -> None: ...

    @overload
    @abstractmethod
    def __setitem__(self, s: slice, o: Iterable[T_co]) -> None: ...

    def __setitem__(self, i: Union[int, slice], o: Union[T_co, MutableSequence[T_co]]) -> None:
        if type(i) is int:
            self.__json_element_data__[i] = self._new_child_item(o)
        else:
            self.__json_element_data__[i] = (self._new_child_item(x) for x in o)

    def _new_child_item(self, item: Any) -> JSONSchemaDOMElement:
        return self.item_type(
            item,
            JSONSchemaDOMInfo(
                document=document(self),
                parent=self
            )
        )

    @overload
    @abstractmethod
    def __delitem__(self, i: int) -> None: ...

    @overload
    @abstractmethod
    def __delitem__(self, i: slice) -> None: ...

    def __delitem__(self, i: int) -> None:
        del self.__json_element_data__[i]

    def __len__(self) -> int:
        return len(self.__json_element_data__)

    def __repr__(self):
        return repr(self.__json_element_data__)

    def __str__(self):
        return str(self.__json_element_data__)

    def insert(self, index: int, item: Any) -> None:
        self.__json_element_data__.insert(index, self._new_child_item(item))

    def to_builtin(self) -> List[Any]:
        return [
            (
                v.to_builtin()
                if isinstance(v, JSONSchemaDOMElement)
                else v
            )
            for v in self
        ]

    def walk_elements(self) -> Iterator[JSONSchemaDOMInfo]:
        for value in self:
            if isinstance(value, JSONSchemaDOMElement):
                yield from value.walk_elements()
            else:
                yield JSONSchemaDOMInfo(value, document(self), self, None)

    def __copy__(self) -> JSONSchemaDOMList:
        cls = self.__class__
        return cls(list(self))

    def __deepcopy__(self, memo: Dict[int, JSONSchemaDOMElement]) -> JSONSchemaDOMList:
        cls = self.__class__
        result = cls(deepcopy(list(self), memo))
        memo[id(self)] = result
        return result
