#!/bin/bash

sudo docker rm $(sudo docker stop $(sudo docker ps -q))

sudo docker network rm my-cassandra-network


