from typing import Any

from abc import ABC, abstractmethod

from ..JSONSchemaElement import JSONSchemaElement
from ..JSONSchema import JSONSchema

from .JSONSchemaDOMInfo import JSONSchemaDOMInfo


class JSONSchemaDOMElement(JSONSchemaElement, ABC):

    __json_dom_info__: JSONSchemaDOMInfo = None
    __json_schema__: JSONSchema = None

    @abstractmethod
    def __init__(
            self,
            value: Any = None,
            _dom_info: JSONSchemaDOMInfo = None,
            _schema: JSONSchema = None,
            **kwargs: Any
    ) -> None:
        # TODO: refactor so this doesn't require creating a new
        #       JSONSchemaDOMInfo object
        if _dom_info:
            self.__json_dom_info__ = JSONSchemaDOMInfo(
                self, _dom_info.document, _dom_info.parent, _dom_info.element_key
            )
        else:
            self.__json_dom_info__ = JSONSchemaDOMInfo(self)
        self.__json_schema__ = _schema
