from typing import Dict, Any
from abc import ABC, abstractmethod

import semver

from jetavator.services.ComputeServiceABC import ComputeServiceABC


class ProjectABC(ABC):

    @property
    @abstractmethod
    def version(self) -> semver.VersionInfo:
        pass

    @property
    @abstractmethod
    def compute_service(self) -> ComputeServiceABC:
        pass

    @property
    @abstractmethod
    def hubs(self) -> Dict[str, Any]:
        pass

    @property
    @abstractmethod
    def links(self) -> Dict[str, Any]:
        pass

    @property
    @abstractmethod
    def satellite_owners(self) -> Dict[str, Any]:
        pass

    @property
    @abstractmethod
    def satellites(self) -> Dict[str, Any]:
        pass

    @property
    @abstractmethod
    def sources(self) -> Dict[str, Any]:
        pass
