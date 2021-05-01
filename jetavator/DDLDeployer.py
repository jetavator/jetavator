from .sql_model import ProjectModel
from .services import StorageService


class DDLDeployer(object):

    def __init__(
            self,
            sql_model: ProjectModel,
            storage_service: StorageService
    ):
        self.sql_model = sql_model
        self.storage_service = storage_service

    def create_tables(self) -> None:
        self.storage_service.logger.info(f'Creating tables...')
        self.storage_service.create_tables(
            self.sql_model.create_tables_ddl())

    def create_history_views(self) -> None:
        self.storage_service.logger.info(f'Creating history views...')
        self.storage_service.create_views(
            self.sql_model.create_history_views())

    def create_current_views(self) -> None:
        self.storage_service.logger.info(f'Creating current views...')
        self.storage_service.create_views(
            self.sql_model.create_current_views())

    def deploy(self) -> None:
        self.create_tables()
        self.create_history_views()
        self.create_current_views()
