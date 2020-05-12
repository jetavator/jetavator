from typing import Any, Dict, Tuple, Iterable

from ..dom import JSONSchemaDOMInfo
from ..exceptions import JSONSchemaValidationError
from ..base_schema import JSONSchema


class JSONSchemaAnyOf(JSONSchema):
    allowed_schemas: Tuple[JSONSchema] = None

    def __init__(
            self,
            allowed_schemas: Iterable[JSONSchema]
    ) -> None:
        self.allowed_schemas = tuple(allowed_schemas)

    def __call__(
            self,
            value: Any,
            dom_info: JSONSchemaDOMInfo = None
    ) -> Any:
        valid_schemas = [
            allowed_schema
            for allowed_schema in self.allowed_schemas
            if allowed_schema.is_valid(value)
        ]
        if len(valid_schemas) > 1:
            raise JSONSchemaValidationError(
                "Ambiguous validation, more than one schema "
                f"is valid: {valid_schemas}"
            )
        if len(valid_schemas) == 0:
            raise JSONSchemaValidationError(
                "No valid schema was found for the supplied value"
            )
        return valid_schemas[0](value, dom_info)

    @property
    def schema(self) -> Dict[str, Any]:
        return {
            'anyOf': [
                allowed_schema.schema
                for allowed_schema in self.allowed_schemas
            ]
        }
