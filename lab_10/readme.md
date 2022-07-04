run: ``` sudo docker-compose up -d ```
to get permission write files from docker container: ``` chmod 777 $(pwd) ```
![docker_compose_up](https://user-images.githubusercontent.com/70692373/177099441-5bbb8b06-fcbd-4d2d-80b0-f6a812218b06.png)

to run requests inside docker container: 
``` sudo docker run --rm -it --network spark-network --name spark-submit -v [path to directory with code]:/opt/app bitnami/spark:3 /bin/bash ```

inside container:
``` cd /opt/app/ ```
``` spark-submit --master spark://spark-master:7077 --deploy-mode client process_data.py ```
![run_requests](https://user-images.githubusercontent.com/70692373/177099503-8f1458c5-0508-480b-bb07-d0524eaad528.png)
![well_done](https://user-images.githubusercontent.com/70692373/177099512-1e7cd2ff-0208-4e6e-81bf-5118e77b5047.png)

``` 
sudo docker-compose stop
sudo docker-compose rm
```
![shutdown](https://user-images.githubusercontent.com/70692373/177099588-fc1cf2b4-c94e-4447-9e94-6a99af512de7.png)
