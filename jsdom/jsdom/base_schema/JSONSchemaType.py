from __future__ import annotations

from abc import ABC, abstractmethod

from typing import Any, Dict

from .JSONSchema import JSONSchema


class JSONSchemaType(JSONSchema, ABC):

    @property
    @abstractmethod
    def type_name(self) -> str:
        pass

    @property
    def schema(self) -> Dict[str, Any]:
        return {"type": self.type_name}


