#!/bin/bash

sudo docker network create my-cassandra-network
sudo docker run --name cassandra-node1 --network my-cassandra-network -d cassandra:latest
sudo docker run --name cassandra-node2 --network my-cassandra-network -d -e CASSANDRA_SEEDS=cassandra-node1 cassandra:latest
sudo docker run --name cassandra-node3 --network my-cassandra-network -d -e CASSANDRA_SEEDS=cassandra-node1 cassandra:latest
