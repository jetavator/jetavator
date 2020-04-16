import yaml

import jetavator

from . import utils


class Client(object):

    def __init__(
        self,
        config
    ):
        self.engine = jetavator.Engine(config)

    @property
    def config(self):
        return self.engine.config

    @property
    def connection(self):
        return self.engine.compute_service

    @property
    def schema_registry(self):
        return self.engine.schema_registry

    @property
    def sql_model(self):
        return self.schema_registry.changeset.sql_model

    @property
    def project(self):
        return self.schema_registry.deployed

    @property
    def project_history(self):
        return self.schema_registry

    def deploy(self):
        self.connection.deploy()

    # TODO: Deprecate environment_type.
    def run(self, load_type: jetavator.LoadType = jetavator.LoadType.DELTA):
        self.engine.run(load_type)

    def add(self, new_object, load_full_history=False, version=None):

        if isinstance(new_object, str):
            new_object_dict = yaml.load(new_object)
        elif isinstance(new_object, dict):
            new_object_dict = new_object
        else:
            raise Exception("Client.add: new_object must be str or dict.")

        self.schema_registry.loaded.increment_version(version)
        new_vault_object = self.schema_registry.loaded.add(new_object_dict)
        self.update_database_model()

        if new_vault_object.type == "satellite":
            self.connection.execute_sql_element(
                new_vault_object.sql_model.sp_load("full").execute())

    def drop(self, object_type, object_name, version=None):
        self.schema_registry.load_from_database()
        self.schema_registry.loaded.increment_version(version)
        self.schema_registry.loaded.delete(object_type, object_name)
        self.update_database_model()

    def update(self, model_dir=None, load_full_history=False):
        if model_dir:
            self.config.model_path = model_dir
        self.schema_registry.load_from_disk()
        assert (
                self.schema_registry.loaded.checksum
                != self.schema_registry.deployed.checksum
        ), (
            f"""
            Cannot upgrade a project if the definitions have not changed.
            Checksum: {self.schema_registry.loaded.checksum.hex()}
            """
        )
        assert (
                self.schema_registry.loaded.version
                > self.schema_registry.deployed.version
        ), (
            "Cannot upgrade - version number must be incremented "
            f"from {self.schema_registry.deployed.version}"
        )
        self.update_database_model(
            load_full_history=load_full_history
        )

    def table_dtypes(self, table_name, registry=None):
        return self.engine.table_dtypes(table_name, registry)

    def csv_to_dataframe(
        self,
        csv_file,
        table_name,
        registry=None
    ):
        return self.engine.csv_to_dataframe(
            csv_file,
            table_name,
            registry
        )

    def source_def_from_dataframe(self, df, table_name, use_as_pk):
        return self.engine.source_def_from_dataframe(df, table_name, use_as_pk)

    def add_dataframe_as_source(self, df, table_name, use_as_pk):
        self.engine.add_dataframe_as_source(df, table_name, use_as_pk)

    def call_stored_procedure(self, procedure_name):
        return self.connection.call_stored_procedure(procedure_name)

    def sql_query_single_value(self, sql):
        return self.connection.sql_query_single_value(sql)

    def get_performance_data(self):
        raise NotImplementedError

    def update_database_model(self, load_full_history=False):
        self._deploy_template(action="alter")
        if load_full_history:
            self.connection.execute_sql_element(
                self.sql_model.sp_jetavator_load_full.execute()
            )

    def update_model_from_dir(self, new_model_path=None):
        if new_model_path:
            self.config.model_path = new_model_path
        self.schema_registry.load_from_disk()

    def clear_database(self):
        self.engine.clear_database()

    def build_wheel(self):
        self.config.wheel_path = utils.build_wheel(
            self.config.jetavator_source_path
        )
        self.config.save()

    def _deploy_template(self, action):

        if not self.schema_registry.loaded.valid:
            raise Exception(self.schema_registry.loaded.validation_error)

        self.connection.reset_metadata()

        if not self.config.deploy_scripts_only:

            utils.print_to_console(f'Creating tables...')

            self.connection.execute_sql_elements_async(
                self.sql_model.create_tables(action)
            )

            utils.print_to_console(f'Creating history views...')

            self.connection.execute_sql_elements_async(
                self.sql_model.history_views(action)
            )

            utils.print_to_console(f'Creating current views...')

            self.connection.execute_sql_elements_async(
                self.sql_model.current_views(action)
            )

        else:

            utils.print_to_console(f'Creating tables... [SKIPPED]')
            utils.print_to_console(f'Creating history views... [SKIPPED]')
            utils.print_to_console(f'Creating current views... [SKIPPED]')

        if not self.config.deploy_scripts_only:
            utils.print_to_console(f'Updating schema registry...')
            self.schema_registry.write_definitions_to_sql()
        else:
            utils.print_to_console(f'Updating schema registry... [SKIPPED]')
