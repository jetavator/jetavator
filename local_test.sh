#!/bin/bash
jetavator config --config-file=./local_spark_config.yml
behave -D config=~/.jetavator/config.yml --stop --no-capture --verbose $1
