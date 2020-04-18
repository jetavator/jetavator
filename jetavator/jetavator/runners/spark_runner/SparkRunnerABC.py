from abc import ABC, abstractmethod
from logging import Logger
from typing import Dict

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
