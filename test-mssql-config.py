from jetavator import Config, Engine

print(Config.__json_schema__().jsonschema_full_schema)

my_config = Config.from_yaml(
    """
      services:
        spark:
          storage_services:
            mssql:
              service_type: storage
              type: spark_mssql
              database: jetavator_dev
              server: localhost
              username: jetavator
              password: jetavator
          service_type: compute
          type: local_spark
          storage:
            vault: mssql
            star: mssql
        file_registry:
          service_type: registry
          type: simple_file_registry
          storage_path: ~/.jetavator/registry
      compute: spark
      registry: file_registry
      secret_lookup: environment
      schema: $RANDOM_TEST_SCHEMA
    """
)

print(my_config)

my_engine = Engine(my_config)

