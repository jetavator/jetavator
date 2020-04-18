from abc import ABC, abstractmethod

from .SparkJobState import SparkJobState
from .. import Runner


class SparkJobABC(ABC):

    @abstractmethod
    def __init__(self, runner: Runner, *args, **kwargs) -> None:
        self.runner = runner

    @property
    @abstractmethod
    def key(self) -> str:
        pass

    @property
    @abstractmethod
    def state(self) -> SparkJobState:
        pass
