FROM python:3.8-slim-buster

WORKDIR /lab_6
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY main.py tweets.csv /lab_6/
ENTRYPOINT [ "python3", "./main.py"]
