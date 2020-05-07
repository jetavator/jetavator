from __future__ import annotations

from typing import Optional, Any

from .JSONSchemaElement import JSONSchemaElement


class JSONSchemaPrimitive(JSONSchemaElement):

    def __new__(
            cls,
            value: Any,
            dom_info: JSONSchemaDOMInfo = None,
            **kwargs: Any
    ) -> JSONSchemaPrimitive:
        return cls.python_primitive(value)

    def __init_subclass__(
            cls,
            python_primitive: Optional[type] = None,
            type_name: Optional[str] = None
    ) -> None:
        cls.python_primitive = python_primitive
        cls.type_name = type_name

    @classmethod
    def _schema(cls) -> Dict[str, Any]:
        return {'type': cls.type_name}


class JSONSchemaString(
    JSONSchemaPrimitive,
    python_primitive=str,
    type_name='string'
):
    pass


class JSONSchemaBoolean(
    JSONSchemaPrimitive,
    python_primitive=bool,
    type_name='boolean'
):
    pass


class JSONSchemaInteger(
    JSONSchemaPrimitive,
    python_primitive=int,
    type_name='integer'
):
    pass


class JSONSchemaFloat(
    JSONSchemaPrimitive,
    python_primitive=float,
    type_name='number'
):
    pass


class JSONSchemaNone(JSONSchemaElement):

    def __new__(
            cls,
            value: None,
            dom_info: JSONSchemaDOMInfo = None,
            **kwargs: Any
    ) -> None:
        if value is not None:
            raise ValueError('Value can only be None.')
        return None

    @classmethod
    def _schema(cls):
        return {'type': 'null'}
