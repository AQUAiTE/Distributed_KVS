#!/bin/bash
set -x

active=$(docker container ls -aq)
if [ -z "$active" ]; then
    exit 0
fi
docker container stop $active
docker container rm $active
docker network rm asg4net
docker image rm asg4img