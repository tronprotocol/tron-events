#!/usr/bin/env bash

source .default.env && docker run -it --rm \
  --name tron-events-dev \
  --link tron-postgres:postgres \
  --link tron-redis:redis \
  -v $PWD:/usr/src/app \
  -e PGPASSWORD=$PGPASSWORD \
  -e PGDATABASE=$PGDATABASE \
  -e PGUSER=$PGUSER \
  -p 8060 \
  -e NODE_ENV=development \
  -e VIRTUAL_HOST=tron-events.localhost \
  -w /usr/src/app node:carbon npm run start
