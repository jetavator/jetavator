from typing import Optional

from .JSONSchemaDOMElement import JSONSchemaDOMElement
from . import JSONSchemaDOMInfo


def dom(element: JSONSchemaDOMElement) -> JSONSchemaDOMInfo:
    return element.__json_dom_info__


def document(element: JSONSchemaDOMElement) -> Optional[JSONSchemaDOMElement]:
    return dom(element).document


def parent(element: JSONSchemaDOMElement) -> Optional[JSONSchemaDOMElement]:
    return dom(element).parent


def key(element: JSONSchemaDOMElement) -> Optional[str]:
    return dom(element).element_key
