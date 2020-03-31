import os

from re import match

from .load_yaml import load_yaml
from .cwd import cwd

EXTENSION = "yaml"


def load_yamls_in_dir(model_path):
    with cwd(model_path):
        return [
            load_yaml(
                os.path.join(path, file)
            )
            for path, dirs, files in os.walk(".")
            for file in files
            if match(r"^.*\." + EXTENSION + "$", file)
        ]
