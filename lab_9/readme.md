``` 
sudo docker-compose up -d 
sudo docker run --rm -it --network spark-network --name spark-submit -v [path_to_folder_with_files]:/opt/app bitnami/spark:3 /bin/bash
```
![run](https://user-images.githubusercontent.com/70692373/176235282-8c8f2aa0-41d0-4fb8-ba97-393ff0e30b4d.png)

```
cd /opt/app/
spark-submit --master spark://spark-experiments-spark-1:7077 --deploy-mode client SimpleProgram.py
```
![result](https://user-images.githubusercontent.com/70692373/176235424-158d7358-acf2-40bd-8cb4-532521333c19.png)
