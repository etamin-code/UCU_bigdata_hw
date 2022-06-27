#!/bin/bash


declare -r NETWORK="kafka-network"

docker build -f dockerfiles/app.Dockerfile --tag app_image .

docker run -d --rm --name app --network ${NETWORK} app_image

