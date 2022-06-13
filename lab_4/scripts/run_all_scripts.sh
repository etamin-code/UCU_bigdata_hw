#!/bin/bash

sudo bash run_cassandra_network.sh
sudo bash create_tables.sh
sudo bash write_data.sh
sudo bash run_docker.sh
