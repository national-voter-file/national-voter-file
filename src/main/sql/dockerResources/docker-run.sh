#! /bin/bash

# This first tries to attach to the container if it is already running,
# then tries to start the container and attach to it, and finally 
# will run the container from the image if it can't find the container at all.

echo "attaching to container. Press ctrl-C to stop the service."
docker attach nvf_postgis || \
docker start nvf_postgis && docker attach nvf_postgis || \
docker run --name nvf_postgis -p 54321:5432 voterfile/postgis
