#!/usr/bin/env bash

docker run -it --rm \
  --link tron-redis:redis \
  redis redis-cli -h redis -p 6379
