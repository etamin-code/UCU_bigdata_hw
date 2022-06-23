#!/bin/bash

declare -r NETWORK="kafka-network"

docker build --tag kafka_producer .
docker run -d --rm --name producer --network ${NETWORK} kafka_producer
