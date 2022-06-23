#!/bin/bash

sudo docker run -it --rm --network kafka-network -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper-server:2181 bitnami/kafka:latest kafka-console-consumer.sh --topic tweets --from-beginning --bootstrap-server kafka-server:9092


