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

First, you'll need to clone the [`grasshopper-loader`](https://github.com/cfpb/grasshopper-loader)
repository into the `geocoder` directory where it'll be ignored by git. Add either
ADDRFEAT files from the Census TIGER dataset or statewide master address files (some
listed in the repo in the `data.json` file) in the `test/data` directories.

Most instructions are the same, but instead of using the standard `docker-compose.yml`
file you can use the version with multiple containers for geocoding by running:

```
docker-compose -f docker-compose-full.yml build
docker-compose -f docker-compose-full.yml up
```

Any commands that you would ordinarily run can be run the same by adding `-f docker-compose-full.yml`.
In order to use the Elasticsearch geocoding, you'll need to load in data with
[`grasshopper-loader`](https://github.com/cfpb/grasshopper-loader) first. You should
be able to follow the same instructions for loading data as are included in
that repository's README (as well as the parent repository for [`grasshopper`](https://github.com/cfpb/grasshopper)
by running commands similar to this:

`docker-compose -f docker-compose-full.yml run loader ./tiger.js -d test/data/tiger`
