FROM python:3.8-slim-buster
	
WORKDIR /lab_8
COPY src/consumer_requirements.txt consumer_requirements.txt
RUN pip3 install -r consumer_requirements.txt
COPY src/consumer.py src/constants.py /lab_8/
ENTRYPOINT [ "python3", "./consumer.py"]
