from __future__ import annotations

from typing import Type, TypeVar

import inspect

from ..JSONSchema import JSONSchema

from .JSONSchemaPrimitive import JSONSchemaPrimitive

T_co = TypeVar('T_co', covariant=True)


# TODO: Review the operation of this and use more explicit typings

def get_schema_from_type(
        some_type: Type
) -> JSONSchema:
    if inspect.isclass(some_type):
        if issubclass(some_type, JSONSchema):
            return some_type
        elif hasattr(some_type, "__json_schema__"):
            return some_type.__json_schema__()
        else:
            return JSONSchemaPrimitive(some_type)
    else:
        return some_type
