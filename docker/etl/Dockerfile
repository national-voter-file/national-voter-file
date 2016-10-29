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

# Install Pentaho Data Integration
# Init ENV
ENV PENTAHO_VERSION 6.1
ENV PENTAHO_TAG 6.1.0.1-196
ENV PENTAHO_HOME /opt/pentaho

RUN apt-get install -q -y wget zip

RUN mkdir ${PENTAHO_HOME}; useradd -s /bin/bash -d ${PENTAHO_HOME} pentaho; chown pentaho:pentaho ${PENTAHO_HOME}

USER pentaho

# Download Pentaho BI Server
RUN /usr/bin/wget --progress=dot:giga http://downloads.sourceforge.net/project/pentaho/Data%20Integration/${PENTAHO_VERSION}/pdi-ce-${PENTAHO_TAG}.zip -O /tmp/pdi-ce-${PENTAHO_TAG}.zip; \
    /usr/bin/unzip -q /tmp/pdi-ce-${PENTAHO_TAG}.zip -d  $PENTAHO_HOME; \
     rm /tmp/pdi-ce-${PENTAHO_TAG}.zip

COPY ./simple-jndi/jdbc.properties /opt/pentaho/data-integration/simple-jndi

