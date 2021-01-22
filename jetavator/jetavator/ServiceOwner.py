from abc import ABC, abstractmethod

import logging


class ServiceOwner(ABC):

    @property
    @abstractmethod
    def logger(self) -> logging.Logger:
        pass
