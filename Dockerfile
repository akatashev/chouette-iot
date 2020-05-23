FROM python:3.8

WORKDIR /chouette

RUN pip3 install pytest pytest-cov
COPY ./requirements.txt /chouette/requirements.txt
RUN pip3 install -r requirements.txt

COPY . /chouette