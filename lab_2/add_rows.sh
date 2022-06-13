#!/bin/bash

declare -r ddl="
USE hw2_kukhar;

INSERT INTO favorite_songs (id, author, song_name, release_year) VALUES (1, 'author1', 'song1', 2021);
INSERT INTO favorite_songs (id, author, song_name, release_year) VALUES (2, 'author2', 'song2', 2022);
INSERT INTO favorite_movies (id, name, producer, release_year) VALUES (1, 'movie1', 'producer1', 2021);
INSERT INTO favorite_movies (id, name, producer, release_year) VALUES (2, 'movie2', 'producer2', 2022);

SELECT * FROM favorite_songs;
SELECT * FROM favorite_movies;
"
sudo docker exec -it cassandra-node1 cqlsh -e "${ddl}"

