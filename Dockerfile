FROM getmovement/national-voter-file-etl:beta2.1

USER root
RUN mkdir /national-voter-file

RUN chmod -R go+rw /opt/pentaho/data-integration/simple-jndi

RUN useradd -s /bin/bash -d /national-voter-file national-voter-file; chown national-voter-file:national-voter-file /national-voter-file
USER national-voter-file

COPY setup.py /national-voter-file
COPY load /national-voter-file/load
COPY src /national-voter-file/src


env	PYTHONPATH /national-voter-file/src/python
ENTRYPOINT ["python3", "/national-voter-file/load/loader.py"]
# COPY ./simple-jndi/jdbc.properties /opt/pentaho/data-integration/simple-jndi
