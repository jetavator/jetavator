from __future__ import annotations  # Remove in Python 3.8

from abc import ABC, abstractmethod

from logging import Logger
from pandas import DataFrame

from jetavator import Engine
from jetavator.schema_registry import Project
from jsdom.mixins import RegistersSubclasses
from jetavator.services import DBService


class Runner(RegistersSubclasses, ABC):

    def __init__(
        self,
        engine: Engine,
        compute_service: DBService,
        project: Project
    ):
        """
        Default constructor to be inherited by subclasses.
        Not intended for direct use: use
        :py:meth:`from_compute_service` instead.
        """
        super().__init__()
        self.engine = engine
        self.compute_service = compute_service
        self.project = project

    @classmethod
    def from_compute_service(
        cls,
        engine: Engine,
        compute_service: DBService,
        project: Project
    ) -> Runner:
        """
        Constructor that takes an :py:class:`~jetavator.Engine`,
        a :py:class:`~jetavator.services.DBService` and
        a :py:class:`~jetavator.schema_registry.Project`
        and returns a registered subclass
        of `Runner` as specified in `compute_service.config.type`
        """
        return cls.registered_subclass_instance(
            compute_service.config.type,
            engine,
            compute_service,
            project
        )

    @property
    @abstractmethod
    def logger(self) -> Logger:
        """
        Python `Logger` instance for raising log messages.
        """
        pass

    @abstractmethod
    def run(self) -> None:
        """
        Run all pipelines in the project
        """
        pass

    @abstractmethod
    def performance_data(self) -> DataFrame:
        """
        Return a `pandas.DataFrame` containing performance data
        """
        pass
