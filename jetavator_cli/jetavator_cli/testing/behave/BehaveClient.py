from ...Client import Client
from ..TestDataLoader import TestDataLoader
from ...utils import (
    run_bash,
    print_to_console
    )

from .Assertions import Assertions

import os

from behave.__main__ import main as behave_main


class BehaveClient(Client):

    @property
    def assert_that(self):
        return Assertions(self)

    @property
    def feature_path(self):
        return os.path.join(
            os.path.abspath(self.config.model_path),
            "features"
        )

    @property
    def behave_userdata(self):
        return {
            "schema": self.config.schema,
            "databricks_host": self.config.databricks_host,
            "databricks_token": self.config.databricks_token,
            "databricks_cluster_id": self.config.databricks_cluster_id,
            "databricks_http_path": self.config.databricks_http_path
        }

    @property
    def serialised_behave_userdata(self):
        return " ".join([
            "-D " + str(k) + "='" + str(v) + "'"
            for k, v in self.behave_userdata.items()
            if v
        ])

    # TODO: Review if this can be deprecated
    def patch_context(self, context):
        context.assert_that = self.assert_that
        context.load_csv = lambda x, y: self.load_csvs(y, [x])
        context.test_data_loader = self.test_data_loader

    def test_data_loader(self, dataframe):
        return TestDataLoader(
            self,
            dataframe,
            self.schema_registry.loaded["source"]
        )

    def test(self):
        if self.config.skip_deploy:
            print_to_console(
                "Skipping deploy step due to config option \n" +
                "----------------------------------------- \n\n"
            )
        else:
            self.deploy()

        behave_args = [
            f"'{self.feature_path}'",
            self.serialised_behave_userdata,
            self.config.behave_options
        ]

        assert behave_main(
            " ".join(behave_args)
            ) == 0, (
                "jetavator: Some behave tests exited with errors."
            )
