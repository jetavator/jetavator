from jetavator.Engine import Engine
from jetavator.print_to_console import print_to_console

from .Assertions import Assertions

import os

from behave.__main__ import main as behave_main


class BehaveEngine(Engine):

    @property
    def assert_that(self):
        return Assertions(self)

    @property
    def feature_path(self):
        return os.path.join(
            os.path.abspath(self.config.model_path),
            "features"
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
            self.config.behave_options
        ]

        assert behave_main(
            " ".join(behave_args)
            ) == 0, (
                "jetavator: Some behave tests exited with errors."
            )
