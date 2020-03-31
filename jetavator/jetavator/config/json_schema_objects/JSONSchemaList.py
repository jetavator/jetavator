from copy import deepcopy

from .JSONSchemaElement import JSONSchemaElement


class JSONSchemaListMeta(type):

    def __getitem__(cls, items_type):
        return type(
            f'{cls.__name__}_{items_type.__name__}',
            (cls,),
            {'items_type': items_type}
        )


class JSONSchemaList(list, JSONSchemaElement, metaclass=JSONSchemaListMeta):

    def __init__(self, iterable, _document=None):
        if _document is None:
            self._document = self
        else:
            self._document = _document
        super().__init__(
            self.items_type(item, _document=self._document)
            for item in iterable
        )
        if self.items_type.index:
            if len(self) != len(set(
                item[self.items_type.index]
                for item in iterable
            )):
                raise ValueError(
                    'Cannot have more than one item with the same '
                    f'property "{self.items_type.index}".'
                )

    def __getitem__(self, key):
        if self.items_type.index:
            for item in self:
                if item[self.items_type.index] == key:
                    return item
            raise KeyError(f'Item not found: {key}')
        else:
            return super().__getitem__(key)

    def __setitem__(self, key, value):
        if self.items_type.index:
            for count, item in enumerate(self):
                if item[self.items_type.index] == key:
                    self.pop(count)
            value[self.items_type.index] = key
            new_item = self.items_type(value)
            new_item._validate()
            self.append(new_item)
        else:
            raise AttributeError(
                'Cannot use __setitem__ for a list with no index.')

    def __delitem__(self, key):
        if self.items_type.index:
            for count, item in enumerate(self):
                if item[self.items_type.index] == key:
                    self.pop(count)
                    return
            raise KeyError(f'Item not found: {key}')
        else:
            raise AttributeError(
                'Cannot use __delitem__ for a list with no index.')

    @classmethod
    def _schema(cls):
        return {
            'array': {
                'items': cls.items_type._schema()
            }
        }

    def _walk(self):
        for value in self:
            if isinstance(value, JSONSchemaElement):
                yield from value._walk()
            else:
                yield (self, None, value)

    def __copy__(self):
        cls = self.__class__
        return cls(list(self))

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls(deepcopy(list(self), memo))
        memo[id(self)] = result
        return result
