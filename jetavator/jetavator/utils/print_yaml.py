import yaml


def print_yaml(yaml_object):
    noalias_dumper = yaml.dumper.SafeDumper
    noalias_dumper.ignore_aliases = lambda self, data: True
    return yaml.dump(
        yaml_object, Dumper=noalias_dumper, default_flow_style=False
    )
