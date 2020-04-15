from jetavator.config import Config
from jetavator.services import DBService

from .Project import Project, ProjectChangeSet
from .sqlalchemy_tables import Deployment


class SchemaRegistry(object):

    def __init__(self, config: Config, compute_service: DBService):
        self.config = config
        self.compute_service = compute_service
        if self.config.model_path:
            self.load_from_disk()
        else:
            self.load_from_database()

    def load_from_disk(self):
        self.loaded = Project.from_directory(
            self,
            self.config.model_path)

    def load_from_database(self):
        self.loaded = self.deployed

    def __getitem__(self, key):
        session = self.compute_service.session()
        deployment = session.query(Deployment).get(key)
        return Project.from_sqlalchemy_object(self, deployment)

    def keys(self):
        session = self.compute_service.session()
        return [
            deployment.version
            for deployment in session.query(Deployment)
        ]

    def values(self):
        session = self.compute_service.session()
        return [
            Project.from_sqlalchemy_object(self, deployment)
            for deployment in session.query(Deployment)
        ]

    # TODO: Implement storage/retrieval of deployed definitions on Spark/Hive
    @property
    def deployed(self):
        # self.compute_service.test()
        # session = self.compute_service.session()
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
        return Project.from_sqlalchemy_object(self, Deployment())

    @property
    def changeset(self):
        return ProjectChangeSet(self.loaded, self.deployed)

    def write_definitions_to_sql(self):
        session = self.compute_service.session()
        session.add(self.loaded.export_sqlalchemy_object())
        session.add_all([
            object_definition.export_sqlalchemy_object()
            for object_definition in self.loaded.values()
        ])
        session.commit()
        session.close()
        self.load_from_database()
