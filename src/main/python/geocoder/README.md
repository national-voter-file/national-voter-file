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

