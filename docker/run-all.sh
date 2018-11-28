#!/usr/bin/env bash

REDIS_IS=$(docker/is-running.sh tron-redis)
PG_IS=$(docker/is-running.sh tron-postgres)
DEV_IS=$(docker/is-running.sh tron-events-dev)

if [[ $REDIS_IS == "1" ]]; then
  echo "Starting redis"
  ./docker/redis.sh
elif [[ $REDIS_IS == "3" ]]; then
  echo "Restarting redis"
  docker restart tron-redis
fi

if [[ $PG_IS == "1" ]]; then
  echo "Starting postgres"
  ./docker/postgres.sh
elif [[ $PG_IS == "3" ]]; then
  echo "Restarting postgres"
  docker restart tron-postgres
fi

if [[ $DEV_IS != "1" ]]; then
  echo "Killing dev"
  docker rm -f tron-events-dev
fi

echo "Starting app"
./docker/dev.sh
