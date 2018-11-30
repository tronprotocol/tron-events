#!/usr/bin/env bash

source .default.env && docker run -d \
  --name tron-events \
  --link tron-postgres:postgres \
  --link tron-redis:redis \
  -v $PWD:/usr/src/app \
  -e PGPASSWORD=$PGPASSWORD \
  -e PGDATABASE=$PGDATABASE \
  -e PGUSER=$PGUSER \
  -p 8060 \
  --restart unless-stopped \
  -w /usr/src/app node:carbon npm run start

