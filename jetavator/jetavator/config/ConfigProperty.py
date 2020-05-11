from typing import Type, Any

import jsdom


class ConfigProperty(jsdom.Property):

    def __get__(
            self,
            instance: jsdom.Element,
            owner: Type[jsdom.Element]
    ) -> Any:
        return jsdom.document(instance).secret_lookup(
            super().__get__(instance, owner)
        )
