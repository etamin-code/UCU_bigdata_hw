#!/bin/bash
docker build --tag kukhar_lab4 .
docker run --name rest-api --network kukhar-cassandra-network -p 8001:8001 kukhar_lab4
