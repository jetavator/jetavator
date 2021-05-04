from typing import TypeVar, Generic
from abc import ABC, abstractmethod

import wysdom


# TODO: Pull up generic config-owning functionality from Service

ConfigType = TypeVar('ConfigType', bound=wysdom.UserObject, covariant=True)


class HasConfig(Generic[ConfigType], ABC):

    @property
    @abstractmethod
    def config(self) -> ConfigType:
        pass

