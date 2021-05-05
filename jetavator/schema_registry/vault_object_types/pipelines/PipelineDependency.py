from typing import Optional, TypeVar, Generic

import wysdom


from ... import VaultObject, VaultObjectKey, VaultObjectOwner


DependencyType = TypeVar("DependencyType", bound=VaultObject)


class PipelineDependency(wysdom.UserObject, Generic[DependencyType]):

    name: str = wysdom.UserProperty(str)
    type: str = wysdom.UserProperty(str)
    view: Optional[str] = wysdom.UserProperty(str, optional=True)

    @property
    def object_reference_key(self) -> VaultObjectKey:
        return VaultObjectKey(self.type, self.name)

    @property
    def _vault_objects(self) -> VaultObjectOwner:
        return wysdom.document(self).owner

    @property
    def object_reference(self) -> DependencyType:
        return self._vault_objects[self.type, self.name]

    def validate(self) -> None:
        if self.object_reference_key not in self._vault_objects:
            raise KeyError(f"Cannot find {self.object_reference_key} in project.")
