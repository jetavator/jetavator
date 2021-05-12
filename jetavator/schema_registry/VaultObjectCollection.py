from __future__ import annotations

from abc import ABC
from typing import TypeVar, Generic, Iterable, Iterator, List, Dict, Tuple

from collections.abc import MutableSet

from .VaultObject import VaultObject, VaultObjectOwner


T_co = TypeVar('T_co', covariant=True, bound=VaultObject)


class VaultObjectCollection(Generic[T_co]):

    def __init__(self, items: Iterable[T_co] = ()) -> None:
        self._data = {
            x.key: x for x in items
        }

    def __len__(self) -> int:
        return len(self._data)


class VaultObjectSet(VaultObjectCollection, MutableSet, Generic[T_co]):

    def __contains__(self, x: T_co) -> bool:
        return x.key in self._data

    def __len__(self) -> int:
        return super().__len__()

    def __iter__(self) -> Iterator[T_co]:
        return iter(self._data.values())

    def add(self, x: T_co) -> None:
        self._data[x.key] = x

    def discard(self, x: T_co) -> None:
        del self._data[x.key]

    def __and__(self, other) -> VaultObjectCollection[T_co]:
        return type(self)(super().__and__(other))

    def __or__(self, other) -> VaultObjectCollection[T_co]:
        return type(self)(super().__or__(other))

    def __sub__(self, other) -> VaultObjectCollection[T_co]:
        return type(self)(super().__sub__(other))

    def __xor__(self, other) -> VaultObjectCollection[T_co]:
        return type(self)(super().__xor__(other))


# TODO: Is this redundant given the interface defined in VaultObjectOwner?

class VaultObjectMapping(VaultObjectCollection, VaultObjectOwner, Generic[T_co], ABC):

    def __getitem__(
            self,
            key: Tuple[str, str]
    ) -> VaultObject:
        if key in self._data:
            return self._data[key]
        else:
            raise KeyError(f'Could not find referenced object: {key}')

    def __len__(self) -> int:
        return super().__len__()

    def __iter__(self) -> Iterator[T_co]:
        return iter(self._data.keys())

    def items_by_type(
            self,
            keys: List[str]
    ) -> Dict[str, T_co]:
        return {
            name: self[object_type, name]
            for object_type, name in self.keys()
            if object_type in keys
        }

    @property
    def hubs(self) -> Dict[str, T_co]:
        return self.items_by_type(["hub"])

    @property
    def links(self) -> Dict[str, T_co]:
        return self.items_by_type(["link"])

    @property
    def satellite_owners(self) -> Dict[str, T_co]:
        return self.items_by_type(["hub", "link"])

    @property
    def satellites(self) -> Dict[str, T_co]:
        return self.items_by_type(["satellite"])

    @property
    def sources(self) -> Dict[str, T_co]:
        return self.items_by_type(["source"])
