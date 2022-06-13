#!/bin/bash


declare -r ddl="CREATE  KEYSPACE hw2_kukhar WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE hw2_kukhar;

CREATE TABLE favorite_songs(
  id int,
  author text,
  song_name text,
  release_year int,
  PRIMARY KEY (id)
);
CREATE TABLE favorite_movies(
  id int,
  name text,
  producer text,
  release_year int,
  PRIMARY KEY (id)
);

describe tables;
"

sudo docker exec -it cassandra-node1 cqlsh -e "${ddl}"
