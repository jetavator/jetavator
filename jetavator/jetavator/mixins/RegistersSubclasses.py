import inspect


def get_registration_namespace(cls):
    return [
        get_class_namespace(ancestor)
        for ancestor in inspect.getmro(cls)
        if RegistersSubclasses in ancestor.__bases__
    ][0]


def get_class_namespace(cls):
    return f"{cls.__module__}.{cls.__name__}"


class RegistersSubclasses(object):

    registered_name = None
    _registration_namespace = None
    _registered_subclasses = {}

    @classmethod
    def registration_namespace(cls):
        try:
            return cls._registration_namespace
        except AttributeError:
            raise AttributeError(
                "Cannot find attribute '_registration_namespace'. "
                "Has another class overridden __init_subclass__ "
                "without calling super().__init_subclass__?"
            )

    def __init_subclass__(cls, register_as=None, **kwargs):
        super().__init_subclass__(**kwargs)

        cls._registration_namespace = get_registration_namespace(cls)

        if register_as:
            key = (cls.registration_namespace(), str(register_as))
            if (
                key in cls._registered_subclasses
                and cls._registered_subclasses[key] is not cls
            ):
                raise Exception(
                    f"""
                    Cannot register {cls} as {register_as}:
                    Already used by {cls._registered_subclasses[key]}.
                    """)
            cls._registered_subclasses[key] = cls
            cls.registered_name = str(register_as)

    @classmethod
    def registered_subclasses(cls):
        return {
            name: subclass
            for namespace, name, subclass in [
                (*key, subclass)
                for key, subclass in cls._registered_subclasses.items()
            ]
            if namespace == cls.registration_namespace()
            and issubclass(subclass, cls)
            and subclass is not cls
        }

    @classmethod
    def is_base_class(cls):
        return (get_class_namespace(cls) == cls.registration_namespace())

    @classmethod
    def registered_subclass(cls, name):
        key = (cls.registration_namespace(), str(name))
        if key not in cls._registered_subclasses:
            raise KeyError(
                f"Unknown registered subclass key: {name}")
        return cls._registered_subclasses[key]

    @classmethod
    def registered_subclass_instance(cls, name, *args, **kwargs):
        return cls.registered_subclass(name)(*args, **kwargs)
