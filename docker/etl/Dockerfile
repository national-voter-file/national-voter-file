FROM ubuntu:15.04
MAINTAINER Marek Obuchowicz <marek@korekontrol.eu>

# Setup python and java and base system
ENV DEBIAN_FRONTEND noninteractive
ENV LANG=en_US.UTF-8

RUN apt-get update && \
  apt-get upgrade -y && \
  apt-get install -q -y openjdk-8-jdk language-pack-en python3-pip 

RUN pip3 install --upgrade pip requests

RUN apt-get install -q -y python-psycopg2 libpq-dev python-pandas

RUN pip3 install usaddress psycopg2 

