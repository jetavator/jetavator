from abc import ABC, abstractmethod

import pandas


class ExecutesSQL(ABC):

    @abstractmethod
    def execute(self, sql) -> pandas.DataFrame:
        pass
