from abc import ABC, abstractmethod

from logging import Logger


class RunnerABC(ABC):
    """
    The generic interface that a Job expects a Runner to provide
    """

    @property
    @abstractmethod
    def logger(self) -> Logger:
        """
        Python `Logger` instance for raising log messages.
        """
        pass
