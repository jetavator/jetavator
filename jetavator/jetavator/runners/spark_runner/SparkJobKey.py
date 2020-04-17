from jetavator.schema_registry import VaultObject


class SparkJobKey(object):

    def __init__(
        self,
        job_type: str,
        **kwargs: VaultObject
    ):
        self.job_type = job_type
        self.vault_objects = kwargs

    def __repr__(self) -> str:
        return '/'.join([
            self.job_type,
            *[
                '.'.join(vault_object.key)
                for vault_object in self.vault_objects.values()
            ]
        ])
