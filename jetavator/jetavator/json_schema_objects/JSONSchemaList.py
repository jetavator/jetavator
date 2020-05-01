from __future__ import annotations

from typing import Generic, TypeVar, Optional, Any, Iterable, Dict, Tuple

from copy import deepcopy

from .JSONSchemaElement import JSONSchemaElement
from .JSONSchemaGeneric import JSONSchemaGeneric

T_co = TypeVar('T_co', covariant=True, bound=JSONSchemaElement)


class JSONSchemaList(list, JSONSchemaGeneric, Generic[T_co], JSONSchemaElement):

    def __init__(
            self,
            iterable: Iterable,
            _document: Optional[T_co] = None,
            _item_type: Optional[Type[JSONSchemaElement]] = None
    ) -> None:
        self._document = self if _document is None else _document
        if _item_type is not None:
            self.item_type = _item_type
        super().__init__(
            self.item_type._instance_for_item(item, _document=self._document)
            for item in iterable
        )
        # TODO: Deprecate indexed lists in favour of JSONSchemaDict
        if self.item_type.index:
            if len(self) != len(set(
                item[self.item_type.index]
                for item in iterable
            )):
                raise ValueError(
                    'Cannot have more than one item with the same '
                    f'property "{self.item_type.index}".'
                )

    def __getitem__(self, key: Union[str, int, slice]) -> T_co:
        if self.item_type.index:
            for item in self:
                if item[self.item_type.index] == key:
                    return item
            raise KeyError(f'Item not found: {key}')
        else:
            return super().__getitem__(key)

    def __setitem__(self, key: str, value: T_co) -> None:
        if self.item_type.index:
            for count, item in enumerate(self):
                if item[self.item_type.index] == key:
                    self.pop(count)
            value[self.item_type.index] = key
            new_item = self.item_type(value)
            new_item._validate()
            self.append(new_item)
        else:
            raise AttributeError(
                'Cannot use __setitem__ for a list with no index.')

    def __delitem__(self, key: str) -> None:
        if self.item_type.index:
            for count, item in enumerate(self):
                if item[self.item_type.index] == key:
                    self.pop(count)
                    return
            raise KeyError(f'Item not found: {key}')
        else:
            raise AttributeError(
                'Cannot use __delitem__ for a list with no index.')

    @classmethod
    def _schema(
            cls,
            item_type: Optional[Type[JSONSchemaElement]] = None
    ) -> Dict[str, Any]:
        return {
            'array': {
                'items': (item_type or cls.item_type)._schema()
            }
        }

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
