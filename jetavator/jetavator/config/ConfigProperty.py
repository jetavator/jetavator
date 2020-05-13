from typing import Type, Any

import wysdom


class ConfigProperty(wysdom.UserProperty):

    def __get__(
            self,
            instance: wysdom.Element,
            owner: Type[wysdom.Element]
    ) -> Any:
        return wysdom.document(instance).secret_lookup(
            super().__get__(instance, owner)
        )
