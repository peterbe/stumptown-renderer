#!/bin/bash
set -e

export ENV_FILE=testing/.env

yarn build

yarn workspace testing run test $@
