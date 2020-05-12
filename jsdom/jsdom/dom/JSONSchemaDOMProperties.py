from typing import Dict, Union

from ..base_schema import JSONSchema


class JSONSchemaDOMProperties(object):

    properties: Dict[str, JSONSchema] = None
    additional_properties: Union[bool, JSONSchema] = False

    def __init__(
            self,
            properties: Dict[str, JSONSchema] = None,
            additional_properties: Union[bool, JSONSchema] = False
    ) -> None:
        self.properties = properties or {}
        self.additional_properties = additional_properties
