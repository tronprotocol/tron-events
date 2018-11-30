#!/usr/bin/env bash

REDIS_IS=$(bin/is-running.sh tron-redis)
PG_IS=$(bin/is-running.sh tron-postgres)

if [[ $REDIS_IS != "1" ]]; then
  docker rm -f tron-redis
fi

if [[ $PG_IS != "1" ]]; then
  docker rm -f tron-postgres
fi

bin/redis.sh
bin/postgres.sh

