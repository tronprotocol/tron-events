#!/usr/bin/env bash

docker run -it --rm \
  -v $PWD/sql:/sql \
  --link tron-postgres:postgres \
  postgres psql -h postgres -U postgres