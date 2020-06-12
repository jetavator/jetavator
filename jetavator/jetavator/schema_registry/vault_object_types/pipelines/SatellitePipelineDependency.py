from typing import Optional

import wysdom


from ... import VaultObject, VaultObjectKey, Project


class SatellitePipelineDependency(wysdom.UserObject):

    name: str = wysdom.UserProperty(str)
    type: str = wysdom.UserProperty(str)
    view: Optional[str] = wysdom.UserProperty(str, optional=True)

    @property
    def object_reference_key(self) -> VaultObjectKey:
        return VaultObjectKey(self.type, self.name)

    @property
    def project(self) -> Project:
        return wysdom.document(self).project

    @property
    def object_reference(self) -> VaultObject:
        return self.project[self.type, self.name]

    def validate(self) -> None:
        if self.object_reference_key not in self.project:
            raise KeyError(f"Cannot find {self.object_reference_key} in project.")
