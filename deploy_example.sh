#!/bin/bash
jetavator config --config-file=./local_spark_config.yml --set model_path=./example_project/yaml --set schema=example_project
jetavator deploy --drop-if-exists
jetavator run delta --folder=./example_project/csv
