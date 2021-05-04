from typing import Union, Tuple, Iterator, Any

from collections.abc import Mapping

from jetavator.schema_registry.Project import Project
from jetavator.schema_registry.sqlalchemy_tables import Deployment

from jetavator.schema_registry.RegistryService import RegistryService


class SQLAlchemyRegistryService(RegistryService, Mapping, register_as="sqlalchemy_registry"):

    loaded: Project = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if self.engine.config.model_path:
            self.load_from_disk()
        else:
            self.load_from_database()

    def __getitem__(
            self,
            key: Union[str, Tuple[str, str]]
    ) -> Project:
        session = self.session()
        deployment = session.query(Deployment).get(key)
        return Project.from_sqlalchemy_object(deployment)

    def __len__(self) -> int:
        return len(list(self.session().query(Deployment)))

    def __iter__(self) -> Iterator[str]:
        return iter(
            deployment.version
            for deployment in self.session().query(Deployment)
        )

    def session(self):
        raise NotImplementedError

    def load_from_disk(self) -> None:
        self.loaded = Project.from_directory(self.engine.config.model_path)

    def load_from_database(self) -> None:
        self.loaded = self.deployed

    # TODO: Implement storage/retrieval of deployed definitions on Spark/Hive
    @property
    def deployed(self) -> Project:
        raise NotImplementedError

    def write_definitions_to_sql(self) -> None:
        session = self.session()
        session.add(self.loaded.export_sqlalchemy_object())
        session.add_all([
            object_definition.export_sqlalchemy_object()
            for object_definition in self.loaded.values()
        ])
        session.commit()
        session.close()
        self.load_from_database()
