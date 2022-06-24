#!/bin/bash

declare -r NETWORK="kafka-network"


docker stop zookeeper-server || true
docker stop kafka-server || true
docker stop producer || true
docker stop consumer || true


docker rm zookeeper-server || true
docker rm kafka-server || true

docker network rm ${NETWORK} || true

