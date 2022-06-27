FROM python:3.8-slim-buster

WORKDIR /lab_8
COPY src/producer_requirements.txt producer_requirements.txt
RUN pip3 install -r producer_requirements.txt
COPY src/producer.py src/data.csv /lab_8/
ENTRYPOINT [ "python3", "./producer.py"]
