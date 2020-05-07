from __future__ import annotations

from abc import abstractmethod

from collections.abc import MutableSequence

from typing import Generic, TypeVar, Optional, Any, Iterable, Dict, List, Tuple, overload

from copy import deepcopy

from .JSONSchemaElement import JSONSchemaElement, JSONSchemaDOMInfo
from . import document
from .JSONSchemaGeneric import JSONSchemaGeneric
from .JSONSchemaValidationError import JSONSchemaValidationError

T_co = TypeVar('T_co', covariant=True, bound=JSONSchemaElement)


class JSONSchemaList(JSONSchemaElement, MutableSequence, JSONSchemaGeneric, Generic[T_co]):

    _element_data: List[JSONSchemaElement] = None

    def __init__(
            self,
            value: Iterable,
            dom_info: JSONSchemaDOMInfo = None,
            _item_type: Optional[Type[T_co]] = None,
            **kwargs: Any
    ) -> None:
        if value and not isinstance(value, Iterable):
            raise JSONSchemaValidationError(
                f"Cannot validate input. Object is not iterable: {value}"
            )
        super().__init__(value, dom_info, **kwargs)
        self._element_data = []
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
        return self._element_data[i]

    @overload
    @abstractmethod
    def __setitem__(self, i: int, o: T_co) -> None: ...

    @overload
    @abstractmethod
    def __setitem__(self, s: slice, o: Iterable[T_co]) -> None: ...

    def __setitem__(self, i: Union[int, slice], o: Union[T_co, MutableSequence[T_co]]) -> None:
        if type(i) is int:
            self._element_data[i] = self._new_child_item(o)
        else:
            self._element_data[i] = (self._new_child_item(x) for x in o)

    def _new_child_item(self, item: Any):
        return self.item_type._instance_for_item(
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
        del self._element_data[i]

    def __len__(self) -> int:
        return len(self._element_data)

    def __repr__(self):
        return repr(self._element_data)

    def __str__(self):
        return str(self._element_data)

    def insert(self, index: int, item: Any) -> None:
        self._element_data.insert(index, self._new_child_item(item))

    @classmethod
    def _schema(
            cls,
            item_type: Optional[Type[T_co]] = None
    ) -> Dict[str, Any]:
        return {
            'array': {
                'items': (item_type or cls.item_type)._schema()
            }
        }

    @property
    def _value(self) -> List[Any]:
        return [
            getattr(v, "_value", v)
            for v in self
        ]

    def _walk(self) -> Iterator[Tuple[JSONSchemaElement, Optional[str], JSONSchemaElement]]:
        for value in self:
            if isinstance(value, JSONSchemaElement):
                yield from value._walk()
            else:
                yield self, None, value

    def __copy__(self) -> JSONSchemaList:
        cls = self.__class__
        return cls(list(self))

    def __deepcopy__(self, memo: Dict[int, JSONSchemaElement]) -> JSONSchemaList:
        cls = self.__class__
        result = cls(deepcopy(list(self), memo))
        memo[id(self)] = result
        return result
