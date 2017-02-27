# Geocoding

This directory contains modules to update the columns `geom` and `geocode_status`
in `public.household_dim`. The program will successively try different web APIs
until it finds the longitude and latitude, placing that in `household_dim.geom`.
The status code indicates the API that was eventually used (or the last API
that was tried and failed).

## 'Daemonizing' adapted for a Docker container

A docker container must have a foregrounded process in order to keep running,
so we are not daemonizing the process inside the docker container (which will
remain in the container's foreground), but rather daemonizing the Docker
process that is running the container. The command:

```
docker-compose run -d <container> <script>
```

will leave the entire container running in the background.
Either of these commands list running containers started with
`docker-compose`:

```
docker-compose ps
docker ps
```


## Quickstart

Copy _daemon.conf.example_ to _daemon.conf_, replace the
keys and passwords inside with real ones,
and then change to _national-voter-file/docker_.

If the containers are already built, you can do this:

```
docker-compose run -d etl national-voter-file/src/main/python/geocoder/daemon.py
```

This should daemonize the running container. You should see the following new files:

```
national-voter-file/src/main/python/geocoder/
|-- RUNNING
|-- logs/
    |-- debug.log
    |-- error.log
```

and should be able to gracefully kill the container (and delete the _RUNNING_ file) with:

```
docker exec `cat ../src/main/python/daemon/RUNNING` kill 1
```


## For debugging

Daemonizing the process will suppress everything that comes to standard out and standard
error (which is not very helpful). Run the container in the foreground by omitting the
`-d` in the `docker-compose` command and adding extra flags:

```
docker-compose run etl \
    /national-voter-file/src/main/python/geocoder/daemon.py \
    --skip-pidfile --log-stdout
```

With `--skip-pidfile`, the 'pidfile' (by default named _RUNNING_) will neither be checked
nor written to, and with `--log-stdout` the log messages will write to standard out.

## Elasticsearch Geocoder (WIP)

To use the full geocoding setup, you'll need to use the Docker Compose configuration
from the `docker-compose-full.yml` file. In general, any commands that you would
ordinarily run can be run the same by adding `-f docker-compose-full.yml`.

The Elasticsearch TIGER loading script reads directly from S3 into memory, but because
it uses `boto3`, you'll need valid AWS credentials in the format of the `.env.sample`
file in a `.env` file before creating the containers. The operations we're running
won't have a cost for anyone reading the data, but `boto3` requires valid credentials.

After you've copied the `.env.sample` file and substituted your own credentials,
build the containers with these commands:

```
docker-compose -f docker-compose-full.yml build
docker-compose -f docker-compose-full.yml up
```

### ES TIGER Data Loading

Because of issues loading TIGER data, both with `grasshopper-loader` and the Census
FTP site itself, we have an S3 Bucket ([viewer here](https://nvf-tiger-2016.s3.amazonaws.com/index.html))
with all of the `ADDRFEAT` files downloaded and broken up into directories by
STATE FIPS code for easier loading.

If you've successfully built and started the containers as shown above, you should
be able to start loading TIGER data with:

`docker-compose -f docker-compose-full.yml run geocoder python /geocoder/es_tiger_loader.py WA`

Where `WA` is the two letter state abbreviation for the state you want to load
TIGER data from.

The [`grasshopper-loader`](https://github.com/cfpb/grasshopper-loader) repository
will work for loading address point data (following their instructions for `index.js`),
and you can clone it into the `geocoder` directory and it will be ignored by git.

`docker-compose -f docker-compose-full.yml run loader ./index.js -h elasticsearch -f ./data.json`

### Running the Geocoder

To start the geocoder itself (which can run into issues if it's started at
the same time as the other containers), run
`docker-compose -f docker-compose-full.yml -d run geocoder python /geocoder/daemon.py`
