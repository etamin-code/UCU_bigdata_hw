FROM python:3.8-slim-buster
	
WORKDIR /lab_8
COPY src/app_requirements.txt app_requirements.txt
RUN pip3 install -r app_requirements.txt
RUN apt-get -y update; apt-get -y install curl
COPY src/app.py src/constants.py /lab_8/
ENTRYPOINT [ "python3", "./app.py"]
