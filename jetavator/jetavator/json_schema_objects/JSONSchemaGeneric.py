from typing import Type, TypeVar, Generic, Any, Optional, Dict

from .JSONSchemaElement import JSONSchemaElement, JSONSchemaDOMInfo

T_co = TypeVar('T_co', covariant=True)


class JSONSchemaGenericProxy(Generic[T_co]):

    def __init__(
            self,
            origin: Type[T_co],
            item_type: Type[JSONSchemaElement]
    ) -> None:
        self.origin = origin
        self.item_type = item_type

    def __call__(self, *args: Any, **kwargs: Any) -> T_co:
        return self.origin(*args, _item_type=self.item_type, **kwargs)

    def _schema(self) -> Dict[str, Any]:
        return self.origin._schema(item_type=self.item_type)

    def _instance_for_item(
            self,
            value: Any,
            dom_info: JSONSchemaDOMInfo = None,
            **kwargs: Any
    ) -> Dict[str, Any]:
        return self.origin._instance_for_item(
            value, dom_info, _item_type=self.item_type, **kwargs)


class JSONSchemaGeneric(object):
    item_type: Type[JSONSchemaElement] = JSONSchemaElement

    def __class_getitem__(
            cls,
            item_type: Type[JSONSchemaElement]
    ) -> JSONSchemaGenericProxy:
        return JSONSchemaGenericProxy(cls, item_type)
