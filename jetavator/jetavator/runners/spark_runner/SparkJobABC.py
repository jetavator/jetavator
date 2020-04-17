from abc import ABC, abstractmethod

from .. import Runner


class SparkJobABC(ABC):

    @abstractmethod
    def __init__(self, runner: Runner, *args, **kwargs) -> None:
        self.runner = runner
