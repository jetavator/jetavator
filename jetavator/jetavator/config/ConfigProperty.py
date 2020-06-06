from typing import Type, Any

import wysdom


class ConfigProperty(wysdom.UserProperty):

    def __get__(
            self,
            instance: wysdom.dom.DOMElement,
            owner: Type[wysdom.dom.DOMElement]
    ) -> Any:
        return wysdom.document(instance).secret_lookup(
            super().__get__(instance, owner)
        )
