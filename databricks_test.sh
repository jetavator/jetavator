#!/bin/bash
jetavator config --config-file=./ci_config.yml
jetavator build
# export HTTPS_PROXY=""
# export HTTP_PROXY=""
behave -D config=~/.jetavator/config.yml --no-capture --verbose $1
