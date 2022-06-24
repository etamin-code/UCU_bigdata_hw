FROM python:3.8-slim-buster
	
WORKDIR /lab_7
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY consumer.py tweets.csv /lab_7/
ENTRYPOINT [ "python3", "./consumer.py"]
