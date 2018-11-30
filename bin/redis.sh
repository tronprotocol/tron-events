#!/usr/bin/env bash

docker run \
  --name tron-redis \
  -p 63790:6379 \
  --restart unless-stopped \
  -v /vol/data/tron-redis:/data \
  -d redis redis-server --appendonly yes

