#!/bin/bash

declare -r NETWORK="kafka-network"

docker build -f dockerfiles/producer.Dockerfile --tag kafka_producer .
docker run  -d --rm --name producer --network ${NETWORK} kafka_producer
