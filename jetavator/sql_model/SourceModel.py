from sqlalchemy.sql.ddl import DDLElement
from typing import List

from jetavator.schema_registry import Source

from .BaseModel import BaseModel


class SourceModel(BaseModel[Source], register_as="source"):

    @property
    def files(self) -> List[DDLElement]:
        return []
