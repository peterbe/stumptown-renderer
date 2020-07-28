#!/bin/bash
set -e

export ENV_FILE=testing/.env

yarn build

# Sanity check that certain things got built
ls client/build/en-us/search-index.json
ls client/build/en-us/docs/web/foo/index.html

yarn workspace testing run test $@
