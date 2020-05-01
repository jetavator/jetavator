import yaml


def load_yaml(file):
    with open(file, "r") as stream:
        return yaml.safe_load(stream)
