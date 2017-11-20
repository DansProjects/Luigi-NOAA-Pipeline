FROM ubuntu:16.04
MAINTAINER rawrzors
RUN apt-get update && apt-get install -y software-properties-common vim && add-apt-repository ppa:jonathonf/python-3.6
RUN apt-get update -y &&  apt-get install -y python3.6 python3.6-dev python3-pip python3.6-venv nano
WORKDIR /opt/noaa-pipeline
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .