#!/bin/bash
sudo docker build . -t my_example_image:1.0
docker run  my_example_image:1.0
