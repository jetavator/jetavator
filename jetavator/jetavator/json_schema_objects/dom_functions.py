from typing import Optional

from jetavator.json_schema_objects.JSONSchemaElement import JSONSchemaElement, JSONSchemaDOMInfo


def dom(element: JSONSchemaElement) -> JSONSchemaDOMInfo:
    return element.__json_dom_info__


def document(element: JSONSchemaElement) -> Optional[JSONSchemaElement]:
    return dom(element).document


def parent(element: JSONSchemaElement) -> Optional[JSONSchemaElement]:
    return dom(element).parent


def key(element: JSONSchemaElement) -> Optional[str]:
    return dom(element).element_key