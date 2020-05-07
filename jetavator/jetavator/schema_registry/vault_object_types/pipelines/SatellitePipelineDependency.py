from typing import Optional

from jetavator import json_schema_objects as jso


from ... import VaultObject, VaultObjectKey, Project


class SatellitePipelineDependency(jso.Object):

    name: str = jso.Property(jso.String)
    type: str = jso.Property(jso.String)
    # TODO: Allow to be None
    view: Optional[str] = jso.Property(jso.String, default="")

    @property
    def object_reference_key(self) -> VaultObjectKey:
        return VaultObjectKey(self.type, self.name)

    @property
    def project(self) -> Project:
        return jso.document(self).project

    @property
    def object_reference(self) -> VaultObject:
        return self.project[self.type, self.name]

    def validate(self) -> None:
        if self.object_reference_key not in self.project:
            raise KeyError(f"Cannot find {self.object_reference_key} in project.")
