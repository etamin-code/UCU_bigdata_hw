#!/bin/bash

declare -r NETWORK="kafka-network"

docker run -d --rm --name cassandra-node --network ${NETWORK} -p 9042:9042 -d cassandra:latest

