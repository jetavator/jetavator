from __future__ import annotations

from typing import Iterable, List

from sqlalchemy import Table, MetaData, Column
from sqlalchemy.schema import CreateTable, DropTable, CreateIndex, DDLElement
from sqlalchemy_views import CreateView, DropView

from jsdom.mixins import RegistersSubclasses

from ..VaultAction import VaultAction

from jetavator.schema_registry import VaultObject, VaultObjectKey, Project


class BaseModel(RegistersSubclasses):

    def __init__(
            self,
            project: Project,
            new_object: VaultObject,
            old_object: VaultObject
    ) -> None:
        super().__init__()
        self.project = project
        self.new_object = new_object
        self.old_object = old_object

    @classmethod
    def subclass_instance(
            cls,
            project: Project,
            new_object: VaultObject,
            old_object: VaultObject
    ) -> BaseModel:
        key: VaultObjectKey = (
            new_object.key if new_object else old_object.key
        )
        return cls.registered_subclass_instance(
            key.type,
            project,
            new_object,
            old_object
        )

    @property
    def definition(self) -> VaultObject:
        if self.new_object:
            return self.new_object
        else:
            return self.old_object

    @property
    def action(self) -> VaultAction:
        if self.new_object is None:
            return VaultAction.DROP
        elif self.old_object is None:
            return VaultAction.CREATE
        elif self.old_object.checksum != self.new_object.checksum:
            return VaultAction.ALTER
        else:
            return VaultAction.NONE

    @property
    def schema(self) -> str:
        return self.project.config.schema

    @property
    def metadata(self) -> MetaData:
        return self.project.compute_service.metadata

    def define_table(
            self,
            name: str,
            *args: Any,
            **kwargs: Any
    ) -> Table:
        table_name = (
            f"{kwargs['schema']}.{name}"
            if 'schema' in kwargs
            else name
        )
        if table_name in self.metadata.tables:
            return self.metadata.tables[table_name]
        else:
            return Table(
                name, self.metadata, *args, **kwargs
            )

    @staticmethod
    def columns_in_table(
            table: Table,
            columns: Iterable[Column]
    ) -> List[Column]:
        return [
            table.columns[column.name]
            for column in columns
        ]

    def create_or_drop_view(
            self,
            view: Table,
            view_query: Any
    ) -> DDLElement:
        if self.action == VaultAction.CREATE:
            return CreateView(view, view_query)
        elif self.action == VaultAction.DROP:
            return DropView(view)

    def create_or_alter_table(
            self,
            table: Table,
            with_index: bool = False
    ) -> List[DDLElement]:
        files = []
        # we need "none" because "action" doesn't yet pick up if satellites
        # have changed, but if they have some tables need to be recreated
        if self.action in (
                VaultAction.ALTER,
                VaultAction.DROP,
                VaultAction.NONE
        ):
            files += [DropTable(table)]
        if self.action in (
                VaultAction.ALTER,
                VaultAction.CREATE,
                VaultAction.NONE
        ):
            files += BaseModel.create_table(table, with_index)
        return files

    def create_or_alter_tables(
            self,
            tables: Iterable[Table],
            with_index: bool = False
    ) -> List[DDLElement]:
        return [
            statement
            for table in tables
            for statement in self.create_or_alter_table(table, with_index)
        ]

    @staticmethod
    def create_table(
            table: Table,
            with_index: bool = False
    ) -> List[DDLElement]:
        statements = [CreateTable(table)]
        if with_index:
            statements += [
                CreateIndex(index)
                for index in table.indexes
            ]
        return statements

    @staticmethod
    def create_tables(
            tables: Iterable[Table],
            with_index: bool = False
    ) -> List[DDLElement]:
        return [
            statement
            for table in tables
            for statement in BaseModel.create_table(table, with_index)
        ]

    @staticmethod
    def drop_tables(tables: Iterable[Table]) -> List[DDLElement]:
        return [
            DropTable(table)
            for table in tables
        ]
