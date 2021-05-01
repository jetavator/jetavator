import wysdom

from jetavator.schema_registry.vault_object_types.ColumnType import ColumnType


class Column(wysdom.UserObject):

    _type: str = wysdom.UserProperty(str, name="type")

    @property
    def type(self) -> ColumnType:
        return ColumnType(self._type)

