from abc import ABC, abstractmethod

import logging


class HasLogger(ABC):

    @property
    @abstractmethod
    def logger(self) -> logging.Logger:
        pass
