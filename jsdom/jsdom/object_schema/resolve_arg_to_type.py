from __future__ import annotations

from typing import Type, Union

import inspect

from ..base_schema import JSONSchema
from ..dom import JSONSchemaDOMElement

from ..base_schema import JSONSchemaPrimitive


def resolve_arg_to_schema(
        arg: Union[Type, JSONSchema]
) -> JSONSchema:
    if inspect.isclass(arg):
        if issubclass(arg, JSONSchemaDOMElement):
            return arg.__json_schema__()
        else:
            return JSONSchemaPrimitive(arg)
    elif isinstance(arg, JSONSchema):
        return arg
    else:
        raise TypeError(f"Unexpected object type: {type(arg)}")
