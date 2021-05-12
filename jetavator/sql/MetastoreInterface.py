from typing import List
from abc import ABC, abstractmethod


class MetastoreInterface(ABC):

    @abstractmethod
    def drop_schema(self) -> None:
        pass

    @abstractmethod
    def create_schema(self) -> None:
        pass

    @property
    @abstractmethod
    def schema_empty(self) -> bool:
        pass

    @property
    @abstractmethod
    def schema_exists(self) -> bool:
        pass

    @abstractmethod
    def table_exists(self, table_name: str) -> bool:
        pass

    @abstractmethod
    def column_exists(self, table_name: str, column_name: str) -> bool:
        pass

    @property
    @abstractmethod
    def table_names(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def schema_names(self) -> List[str]:
        pass

    @abstractmethod
    def column_names_in_table(self, table_name: str) -> List[str]:
        pass
