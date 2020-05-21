FROM python:3.8

WORKDIR /chouette

COPY ./requirements.txt /chouette/requirements.txt
RUN pip3 install -r requirements.txt
COPY . /chouette