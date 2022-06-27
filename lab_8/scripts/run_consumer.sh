#!/bin/bash


declare -r NETWORK="kafka-network"

docker build -f dockerfiles/consumer.Dockerfile --tag kafka_consumer .

docker run  -d --rm --name consumer --network ${NETWORK} kafka_consumer

