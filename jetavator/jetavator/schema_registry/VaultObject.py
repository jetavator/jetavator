from datetime import datetime
from collections import namedtuple

from jetavator.utils import print_yaml
from jetavator.mixins import RegistersSubclasses, ValidatesYaml
from jetavator.sql_model import HasSQLModel


VaultObjectKey = namedtuple('VaultObjectKey', ['type', 'name'])
HubKeyColumn = namedtuple('HubKeyColumn', ['name', 'source'])


class VaultObject(RegistersSubclasses, HasSQLModel, ValidatesYaml):

    required_yaml_properties = ["name", "type"]

    optional_yaml_properties = []

    def __init__(
        self,
        project,
        new_object=None,
        old_object=None
    ):
        super().__init__()
        self.project = project
        self.new_object = new_object
        self.old_object = old_object

        if self.new_object:
            self._validate_yaml(self.new_object.definition)

        if self.old_object:
            self._validate_yaml(self.old_object.definition)

        if self.old_object and self.new_object:
            if self.old_object.name != self.new_object.name:
                raise RuntimeError("Object names must match.")
            if self.old_object.type != self.new_object.type:
                raise RuntimeError("Object types must match.")

    def __repr__(self) -> str:
        class_name = type(self).__name__
        return f'{class_name}({self.name})'

    @classmethod
    def subclass_instance(
        cls,
        project,
        new_object=None,
        old_object=None
    ):

        if not (new_object or old_object):
            raise RuntimeError(
                "Must specify either new_object "
                "or old_object")

        if new_object:
            registered_subclass_name = new_object.type
        else:
            registered_subclass_name = old_object.type

        return cls.registered_subclass_instance(
            registered_subclass_name,
            project,
            new_object,
            old_object
        )

    @property
    def key(self):
        return VaultObjectKey(self.type, self.name)

    @property
    def definition(self):
        if self.new_object:
            return self.new_object.definition
        else:
            return self.old_object.definition

    def export_sqlalchemy_object(self):
        # self.new_object.version = str(self.project.version)
        # version should be immutable
        self.new_object.deploy_dt = str(datetime.now())
        return self.new_object

    @property
    def yaml(self):
        return print_yaml(self.definition)

    @property
    def compute_service(self):
        return self.project.compute_service

    @property
    def name(self):
        if self.new_object:
            return self.new_object.name
        else:
            return self.old_object.name

    @property
    def type(self):
        if self.new_object:
            return self.new_object.type
        else:
            return self.old_object.type

    @property
    def full_name(self):
        return f'{self.type}_{self.name}'

    @property
    def checksum(self):
        return self.new_object.checksum

    @property
    def dependent_satellites(self):
        return [
            satellite
            for satellite in self.project["satellite"].values()
            if any(
                dependency.type == self.type
                and dependency.name == self.name
                for dependency in satellite.pipeline.dependencies
            )
        ]

    @property
    def action(self):
        if self.new_object is None:
            return "drop"
        elif self.old_object is None:
            return "create"
        elif self.old_object.checksum != self.new_object.checksum:
            return "alter"
        else:
            return "none"

    def _resolve_reference(self, ref_element, ignore_errors=False):
        try:
            return self.project[
                ref_element["type"],
                ref_element["name"],
            ]
        except KeyError:
            if ignore_errors:
                return ref_element
            else:
                raise Exception(
                    "Could not locate linked definition "
                    f"{str(ref_element)}"
                )

    def _replace_reference(self, ref_parent, ref_key):
        ref_parent[ref_key] = self._resolve_reference(ref_parent[ref_key])