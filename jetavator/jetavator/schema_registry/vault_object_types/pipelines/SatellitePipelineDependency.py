from typing import Optional

import jsdom


from ... import VaultObject, VaultObjectKey, Project


class SatellitePipelineDependency(jsdom.Object):

    name: str = jsdom.Property(str)
    type: str = jsdom.Property(str)
    # TODO: Allow to be None
    view: Optional[str] = jsdom.Property(str, default="")

    @property
    def object_reference_key(self) -> VaultObjectKey:
        return VaultObjectKey(self.type, self.name)

    @property
    def project(self) -> Project:
        return jsdom.document(self).project

    @property
    def object_reference(self) -> VaultObject:
        return self.project[self.type, self.name]

    def validate(self) -> None:
        if self.object_reference_key not in self.project:
            raise KeyError(f"Cannot find {self.object_reference_key} in project.")
