#!/usr/bin/env bash

REDIS_IS=$(bin/is-running.sh tron-redis)
PG_IS=$(bin/is-running.sh tron-postgres)

if [[ $REDIS_IS != "2" ]]; then
  echo "ERROR: tron-redis is not running locally."
  exit 1
elif [[ $PG_IS != "2" ]]; then
  echo "ERROR: tron-postgres is not running locally."
  exit 1
fi

source .default.env && NODE_ENV=test node_modules/.bin/mocha 'test/**/*.test.js'