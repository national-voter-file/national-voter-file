FROM python:3-onbuild

RUN apt-get update -y && apt-get install -y libgeos-dev
RUN pip install shapely==1.6b2

CMD ["/bin/bash"]
