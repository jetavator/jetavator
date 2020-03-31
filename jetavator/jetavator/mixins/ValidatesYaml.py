import inspect


class ValidatesYaml(object):

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.required_yaml_properties = [
            prop
            for superclass in inspect.getmro(cls)
            for prop in getattr(superclass, "required_yaml_properties", [])
        ]
        cls.optional_yaml_properties = [
            prop
            for superclass in inspect.getmro(cls)
            for prop in getattr(superclass, "optional_yaml_properties", [])
        ]

    @property
    def allowed_yaml_properties(self):
        return self.required_yaml_properties + self.optional_yaml_properties

    def _validate_yaml(self, yaml_dict):
        for key in yaml_dict.keys():
            if key not in self.allowed_yaml_properties:
                raise Exception(
                    'Invalid object definition:'
                    f' Unknown property "{key}"'
                    f' in object [{yaml_dict.get("name")}]'
                    f' with type [{yaml_dict.get("type")}]'
                )
        for key in self.required_yaml_properties:
            if key not in yaml_dict:
                raise Exception(
                    'Invalid object definition:'
                    f' Required property is missing "{key}"'
                    f' in object [{yaml_dict.get("name")}]'
                    f' with type [{yaml_dict.get("type")}]'
                )

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            if key not in self.allowed_yaml_properties:
                raise AttributeError(
                    f'Unknown attribute "{key}":'
                    ' Attribute is not defined'
                    f' in object [{self}].'
                    ' Allowed attributes: required_yaml_properties='
                    f'{self.required_yaml_properties}'
                    ' or optional_yaml_properties='
                    f'{self.optional_yaml_properties}'
                )
            return self.definition.get(key)


class HasYamlProperties(ValidatesYaml):

    def __init__(self, project, parent, definition):
        self.project = project
        self.parent = parent
        self.definition = definition
        self._validate_yaml(self.definition)
