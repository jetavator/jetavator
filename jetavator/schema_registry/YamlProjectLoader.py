from typing import Any, Dict, List

import os
import yaml

from re import match
from contextlib import contextmanager


class YamlProjectLoader(object):

    def __init__(
            self,
            model_path: str,
            extension: str = "yaml"
    ) -> None:
        self._model_path = model_path
        self._extension = extension

    def load_files(self) -> List[Dict[str, Any]]:
        with self._cwd(self._model_path):
            return [
                self.load_yaml(
                    os.path.join(path, file)
                )
                for path, dirs, files in os.walk(".")
                for file in files
                if match(r"^.*\." + self._extension + "$", file)
            ]

    @staticmethod
    def load_yaml(file: str) -> Dict[str, Any]:
        with open(file) as stream:
            return yaml.safe_load(stream)

    @staticmethod
    @contextmanager
    def _cwd(path):
        old_pwd = os.getcwd()
        os.chdir(path)
        try:
            yield
        finally:
            os.chdir(old_pwd)
