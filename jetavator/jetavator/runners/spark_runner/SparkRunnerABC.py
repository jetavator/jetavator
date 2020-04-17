from abc import ABC, abstractmethod
from logging import Logger
from typing import Dict, List

from jetavator import KeyType
from jetavator.schema_registry import VaultObject

from .SparkJobABC import SparkJobABC
from ..Runner import Runner


class SparkRunnerABC(Runner, ABC):

    @property
    @abstractmethod
    def logger(self) -> Logger:
        pass

    @property
    @abstractmethod
    def jobs(self) -> Dict[str, SparkJobABC]:
        pass

    @abstractmethod
    def get_job(self, class_name, *args):
        pass

    @abstractmethod
    def satellite_owner_output_keys(
            self,
            satellite: VaultObject,
            satellite_owner: VaultObject,
            key_type: KeyType
    ) -> SparkJobABC:
        pass

    @abstractmethod
    def input_keys(
        self,
        satellite: VaultObject,
        key_type: KeyType
    ) -> List[SparkJobABC]:
        pass
