#!/bin/bash

bash ./scripts/run-kafka-cluster.sh
bash ./scripts/run-cassandra-cluster.sh
bash ./scripts/create_tables.sh
bash ./scripts/run_producer.sh
bash ./scripts/run_consumer.sh
bash ./scripts/run_app.sh

