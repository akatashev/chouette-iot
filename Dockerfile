FROM python:3.8-slim

WORKDIR /chouette
RUN apt-get update && apt-get install gcc -y && apt-get clean
RUN pip3 install pytest pytest-cov requests-mock
COPY ./requirements.txt /chouette/requirements.txt
RUN pip3 install -r requirements.txt

COPY . /chouette