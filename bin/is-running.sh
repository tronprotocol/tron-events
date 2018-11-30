#!/usr/bin/env bash

PS=$(docker inspect --format="{{ .Name }}" $(docker ps -q --no-trunc) | sed "s,/,,g")

RESULT=1

for c in $PS
do
  if [[ "$c" == "$1" ]]; then
    RESULT=2
  fi
done

if [[ $RESULT == 1 ]]; then

  PSA=$(docker inspect --format="{{ .Name }}" $(docker ps -aq --no-trunc) | sed "s,/,,g")
  for c in $PSA
  do
    if [[ "$c" == "$1" ]]; then
      RESULT=3
    fi
  done
fi

echo $RESULT