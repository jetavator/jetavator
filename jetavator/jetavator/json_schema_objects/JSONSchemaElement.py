from __future__ import annotations

from typing import Optional, Any

from abc import ABC, abstractmethod

import jsonschema

# from .types import JSONSchema, JSONSchemaTypeAnything


class JSONSchemaElement(ABC):

    @property
    @abstractmethod
    def _value(self) -> Any:
        pass

    def _walk(self) -> Iterator[Tuple[JSONSchemaElement, Optional[str], JSONSchemaElement]]:
        yield self, None, self


