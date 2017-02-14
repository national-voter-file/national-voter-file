FROM python:3-onbuild

RUN apt-get update -y && apt-get install -y libgeos-dev
RUN pip install shapely==1.6b2 && pip install pyproj==1.9.5.1

CMD ["/bin/bash"]
