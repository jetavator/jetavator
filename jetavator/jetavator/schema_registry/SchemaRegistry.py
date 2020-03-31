import time

from jetavator import utils

from sqlalchemy.exc import ProgrammingError, OperationalError

from .Project import Project, ProjectChangeSet
from .sqlalchemy_tables import Deployment


class SchemaRegistry(object):

    def __init__(self, config, connection):
        self.config = config
        self.connection = connection
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
        session = self.connection.session()
        deployment = session.query(Deployment).get(key)
        return Project.from_sqlalchemy_object(self, deployment)

    def keys(self):
        session = self.connection.session()
        return [
            deployment.version
            for deployment in session.query(Deployment)
        ]

    def values(self):
        session = self.connection.session()
        return [
            Project.from_sqlalchemy_object(self, deployment)
            for deployment in session.query(Deployment)
        ]

    @property
    def deployed(self):
        return Project.from_sqlalchemy_object(self, Deployment())
        # the above is for local Spark testing - remove it later

        self.connection.test()
        session = self.connection.session()
        try:
            deployment = session.query(Deployment).order_by(
                Deployment.deploy_dt.desc()).first()
            retry = False
        except (ProgrammingError, OperationalError):
            deployment = Deployment()
            retry = False
        #TODO:if there is no deployment on databricks above piece fails.
        # for not loose fix is done by below if statement. needs to be
        # fixed with more logical code in future
        if deployment is None:
            deployment = Deployment()
        return Project.from_sqlalchemy_object(self, deployment)

    @property
    def changeset(self):
        return ProjectChangeSet(self.loaded, self.deployed)

    def write_definitions_to_sql(self):
        session = self.connection.session()
        session.add(self.loaded.export_sqlalchemy_object())
        session.add_all([
            object_definition.export_sqlalchemy_object()
            for object_definition in self.loaded.values()
        ])
        session.commit()
        session.close()
        self.load_from_database()
