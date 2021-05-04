from typing import Union, Tuple, Iterator

from jetavator.schema_registry.Project import Project
from jetavator.schema_registry.sqlalchemy_tables import Deployment

from jetavator.schema_registry.RegistryService import RegistryService


class FileRegistryService(RegistryService, register_as="simple_file_registry"):

    loaded: Project = None

    def __getitem__(
            self,
            key: Union[str, Tuple[str, str]]
    ) -> Project:
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError

    def __iter__(self) -> Iterator[str]:
        raise NotImplementedError

    @property
    def deployed(self) -> Project:
        return Project.from_sqlalchemy_object(self.engine.compute_service, Deployment())

