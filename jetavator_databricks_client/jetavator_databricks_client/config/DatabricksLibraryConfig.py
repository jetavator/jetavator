from jetavator.config import SecretSubstitutingConfig


class DatabricksLibraryConfig(
    SecretSubstitutingConfig,
    register_as='remote_databricks_library'
):
    pass