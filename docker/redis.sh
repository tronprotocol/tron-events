#!/usr/bin/env bash

docker run \
  --name tron-redis \
  --restart unless-stopped \
  -v /vol/data/tron-redis:/data \
  -d redis redis-server --appendonly yes

