jetavator config --config-file=./local_spark_config.yml
behave -D config=~/.jetavator/config.yml --no-capture --verbose $1
