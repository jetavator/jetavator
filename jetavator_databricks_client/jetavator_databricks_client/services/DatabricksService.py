import sqlalchemy
import os
import time
import thrift

from lazy_property import LazyProperty

from jetavator.services.SparkService import SparkService

from TCLIService.ttypes import TOperationState

from functools import wraps

from .. import LogListener


CLUSTER_RETRY_INTERVAL_SECS = 15

# TODO: Move all the constants and filepath generation into the same place
DBFS_DATA_ROOT = 'dbfs:/jetavator/data'


def retry_if_cluster_not_ready(original_function):
    @wraps(original_function)
    def decorated_function(self, *args, **kwargs):
        retry = True
        while retry:
            retry = False
            try:
                return original_function(self, *args, **kwargs)
            except thrift.Thrift.TApplicationException as e:
                if "TEMPORARILY_UNAVAILABLE" in str(e):
                    self.logger.info(
                        "Cluster is starting: retrying in 15 seconds")
                    time.sleep(CLUSTER_RETRY_INTERVAL_SECS)
                    retry = True
    return decorated_function


class DatabricksService(SparkService, register_as='remote_databricks'):

    def __init__(self, engine, config):
        super().__init__(engine, config)
        self.sqlalchemy_master = self._create_sql_connection(
            schema="default"
        )
        self.sqlalchemy_connection = self._create_sql_connection()
        self.reset_metadata()

    # TODO: Reverse this relationship so the Runner instantiates the Service
    @LazyProperty
    def databricks_runner(self):
        raise NotImplementedError

    @LazyProperty
    def log_listener(self):
        return LogListener(self.engine.config, self.engine.logs_storage_service)

    def yaml_file_paths(self):
        for root, _, files in os.walk(self.engine.config.model_path):
            for yaml_file in filter(
                lambda x: os.path.splitext(x)[1] == '.yaml',
                files
            ):
                yield os.path.join(root, yaml_file)

    def _deploy_wheel(self):
        if not os.path.isfile(self.engine.config.wheel_path):
            raise Exception(
                f'No wheel file found at [{self.engine.config.wheel_path}]')
        self.databricks_runner.start_cluster()
        self.databricks_runner.load_wheel()


    def deploy(self):
        self._deploy_wheel()
        self.databricks_runner.create_jobs()
        self.databricks_runner.load_config()
        self.databricks_runner.create_secrets()
        self.databricks_runner.clear_yaml()
        for path in self.yaml_file_paths():
            self.databricks_runner.load_yaml(
                os.path.relpath(path, self.engine.config.model_path))
        self.databricks_runner.deploy_remote()

    def session(self):
        # noinspection PyUnresolvedReferences
        return sqlalchemy.orm.sessionmaker(bind=self.sqlalchemy_connection)()

    def reset_metadata(self):
        self._metadata = sqlalchemy.MetaData()
        self._metadata.bind = self.sqlalchemy_connection

    @property
    def metadata(self):
        return self._metadata

    def execute(self, sql):
        for sql_statement in sql.encode(
                "ascii", "ignore"
        ).decode("ascii").split("GO\n"):
            # noinspection PyUnresolvedReferences
            try:
                self.sqlalchemy_connection.execute(
                    sql_statement
                )
            except (
                    sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.DBAPIError
            ) as e:
                raise Exception(
                    f"""
                    Config dump:
                    {self.config}

                    Error while strying to run script:
                    {sql_statement}
                    """ + str(e)
                )

    @retry_if_cluster_not_ready
    def drop_schema(self):
        self.sqlalchemy_master.execute(
            f'DROP DATABASE {self.config.schema} CASCADE'
        )

    @retry_if_cluster_not_ready
    def create_schema(self):
        self.sqlalchemy_master.execute(
            f'CREATE DATABASE {self.config.schema}'
        )

    @property
    @retry_if_cluster_not_ready
    def schema_empty(self):
        return not any([
            row
            for row in self.sqlalchemy_master.execute(
                f'SHOW TABLES IN {self.config.schema}'
            )
        ])

    @property
    @retry_if_cluster_not_ready
    def schema_exists(self):
        return any([
            row['databaseName'] == self.config.schema
            for row in self.sqlalchemy_master.execute('SHOW DATABASES')
        ])

    def table_exists(self, table_name):
        # Workaround because has_table raises an OperationalError if
        # a table does not exist. Contribute a fix to
        # https://github.com/dropbox/PyHive for this in future!
        # noinspection PyUnresolvedReferences
        try:
            return self.sqlalchemy_connection.dialect.has_table(
                self.sqlalchemy_connection, table_name)
        except sqlalchemy.exc.OperationalError as e:
            if (
                "org.apache.spark.sql.catalyst.analysis.NoSuchTableException"
                in str(e)
            ):
                return False
            else:
                raise

    def column_exists(self, table_name, column_name):
        self.metadata.reflect(only=[table_name])
        return bool(self.metadata.tables.get(table_name).c.get(column_name))

    @retry_if_cluster_not_ready
    def sql_query_single_value(self, sql):
        try:
            return self.sqlalchemy_connection.execute(
                sql
            ).first()[0]
        except TypeError:
            return None

    @retry_if_cluster_not_ready
    def test(self, master=False):
        sqlalchemy_connection = (
            self.sqlalchemy_master if master
            else self.sqlalchemy_connection)
        assert sqlalchemy_connection.execute(
            "SELECT 1"
        ).first()[0] == 1
        return True

    def execute_sql_element(self, sql_element, async_cursor=False):
        try:
            cursor = self.sqlalchemy_connection.raw_connection().cursor()
            cursor.execute(
                self.compile_sqlalchemy(sql_element),
                async_=async_cursor
            )
            return cursor.fetchall()
        except Exception:
            print('Execution error')
            raise

    def execute_sql_elements_async(self, sql_elements):
        if type(sql_elements) is dict:
            jobs = sql_elements
        else:
            jobs = {
                self.sql_script_filename(sql_element): sql_element
                for sql_element in sql_elements
            }
        running_jobs = {
            k: self.execute_sql_element(v, async_cursor=True)
            for k, v in jobs.items()
        }
        while any(running_jobs.values()):
            for key, cursor in running_jobs.items():
                if cursor:
                    for message in cursor.fetch_logs():
                        self.logger.info(f'{key}: {message}')
                    state = cursor.poll().operationState
                    if state == TOperationState.FINISHED_STATE:
                        running_jobs[key] = None
                        self.logger.info(
                            f'Finished executing '
                            f'{list(running_jobs.values()).count(None)} of '
                            f'{len(running_jobs)}: {key}'
                        )
                    elif state not in (
                        TOperationState.INITIALIZED_STATE,
                        TOperationState.RUNNING_STATE
                    ):
                        raise Exception(
                            f'Error running job {key}: '
                            f'{cursor.poll().errorMessage}'
                        )

    def _create_sql_connection(
        self,
        schema=None
    ):
        database = (schema or self.config.schema)

        return sqlalchemy.create_engine(
            f'databricks+pyhive://token:{self.config.token}'
            f'@{self.config.host}:443'
            f'/{database}',
            connect_args={
                'http_path': self.config.http_path
            }
        )

    def csv_file_path(self, source):
        return (
            f'{DBFS_DATA_ROOT}/'
            f'{self.config.schema}/'
            f'{source.name}.csv'
        )

    # TODO: Either implement this - or refactor so this class
    #       doesn't inherit this from SparkService
    def source_csv_exists(self, source):
        raise NotImplementedError

    def load_csv(self, csv_file, source):
        self.databricks_runner.load_csv(csv_file, source)

    def run_remote(self):
        self.databricks_runner.start_cluster()
        self.databricks_runner.run_remote()
