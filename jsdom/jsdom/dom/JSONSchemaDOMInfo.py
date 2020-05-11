from typing import Optional

from ..JSONSchemaElement import JSONSchemaElement


class JSONSchemaDOMInfo(object):

    def __init__(
            self,
            element: Optional[JSONSchemaElement] = None,
            document: Optional[JSONSchemaElement] = None,
            parent: Optional[JSONSchemaElement] = None,
            element_key: Optional[str] = None
    ) -> None:
        self._element = element
        self._document = document
        self._parent = parent
        self._element_key = element_key

    @property
    def element(self) -> JSONSchemaElement:
        return self._element

    @property
    def document(self) -> JSONSchemaElement:
        return self._element if self._document is None else self._document

    @property
    def parent(self) -> Optional[JSONSchemaElement]:
        return self._parent

    @property
    def element_key(self) -> Optional[str]:
        return self._element_key
