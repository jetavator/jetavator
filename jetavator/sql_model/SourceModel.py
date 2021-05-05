from sqlalchemy.sql.ddl import DDLElement
from typing import List

from jetavator.schema_registry import Source

from .SQLModel import SQLModel


class SourceModel(SQLModel[Source], register_as="source"):

    @property
    def files(self) -> List[DDLElement]:
        return []
