from typing import Any
from abc import ABC, abstractmethod

import pandas
import sqlalchemy


class ExecutesSQL(ABC):

    @abstractmethod
    def execute(self, sql) -> pandas.DataFrame:
        pass

    def execute_sql_element(self, sql_element, async_cursor=False):
        return self.execute(
            self.compile_sqlalchemy(sql_element)
        )

    @property
    @abstractmethod
    def sqlalchemy_dialect(self) -> sqlalchemy.engine.interfaces.Dialect:
        pass

    @staticmethod
    def compile_sqlalchemy_with_dialect(
            sqlalchemy_element: sqlalchemy.sql.expression.ClauseElement,
            dialect: sqlalchemy.engine.interfaces.Dialect
    ) -> str:
        def compile_with_kwargs(**compile_kwargs: Any) -> str:
            return str(sqlalchemy_element.compile(
                dialect=dialect,
                compile_kwargs=compile_kwargs))
        try:
            return compile_with_kwargs(literal_binds=True)
        except TypeError:
            return compile_with_kwargs()

    def compile_sqlalchemy(
            self,
            sqlalchemy_element: sqlalchemy.sql.expression.ClauseElement
    ) -> str:
        return self.compile_sqlalchemy_with_dialect(
            sqlalchemy_element,
            self.sqlalchemy_dialect)
