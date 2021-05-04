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
        session = self.engine.compute_service.session()
        deployment = session.query(Deployment).get(key)
        return Project.from_sqlalchemy_object(self.engine.compute_service, deployment)

    def __len__(self) -> int:
        return len(list(self.session().query(Deployment)))

    def __iter__(self) -> Iterator[str]:
        return iter(
            deployment.version
            for deployment in self.session().query(Deployment)
        )

    def session(self):
        return self.owner.compute_service.session()

    def load_from_disk(self) -> None:
        self.loaded = Project.from_directory(
            self.engine.config,
            self.engine.compute_service,
            self.engine.config.model_path)

    def load_from_database(self) -> None:
        self.loaded = self.deployed

    # TODO: Implement storage/retrieval of deployed definitions on Spark/Hive
    @property
    def deployed(self) -> Project:
        # self.engine.compute_service.test()
        # session = self.engine.compute_service.session()
        # try:
        #     deployment = session.query(Deployment).order_by(
        #         Deployment.deploy_dt.desc()).first()
        #     retry = False
        # except (ProgrammingError, OperationalError):
        #     deployment = Deployment()
        #     retry = False
        # # if there is no deployment on Spark/Hive above piece fails.
        # # for not loose fix is done by below if statement. needs to be
        # # fixed with more logical code in future
        # if deployment is None:
        #     deployment = Deployment()
        # return Project.from_sqlalchemy_object(self, deployment)
        return Project.from_sqlalchemy_object(self.engine.compute_service, Deployment())

    def write_definitions_to_sql(self) -> None:
        session = self.engine.compute_service.session()
        session.add(self.loaded.export_sqlalchemy_object())
        session.add_all([
            object_definition.export_sqlalchemy_object()
            for object_definition in self.loaded.values()
        ])
        session.commit()
        session.close()
        self.load_from_database()
