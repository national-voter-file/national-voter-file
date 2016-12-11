# Python tools

This directory collects random useful Python tools.


## CSVFileSampler.py

_CSVFileSampler.py_ requires no external libraries.
It provides functions to sample lines from a file.
Use it in a command line environment from within the
Docker *etl* container (you have to be in the
directory _national-voter-file/docker_) like this:

```
docker-compose run etl bash
/$  # logged in now
/$ cd national-voter-file/src/main/python/tools/
/$ python3
Python 3.4.3 (default, Mar 26 2015, 22:03:40) 
[GCC 4.9.2] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> 
>>> import CSVFileSampler as sampler
>>> fname = '/national-voter-file/testdata/Washington/Washington State Data.txt'
>>> sampler.fileLen(fname)
1001
>>> sampled_rows = sampler.filesample(fname, 3, 1001)  # get 3 random lines
>>> print('\n'.join(sampled_rows))
## Stuff that looks ugly on screen
>>> exit()
/$ exit
```


## make_erdplus_model.py

_make_erdplus_model.py_ is a command-line utility that creates
JSON-formatted files to be read by [ERDplus][erdplus], a free
online relational diagram / database design tool we're using
to visualize the database models.

It requires the Python library `psycopg2` (already on the
Docker *etl* instance) to be installed. Run it with the flag `--help`
to see all of the options. Example usage, from within
the directory _national-voter-file/docker_:

```
docker-compose run etl python \
national-voter-file/src/main/python/tools/make_erdplus_model.py --ip=localhost
```

It will prompt for the database password (on *etl* it's blank), and then
run the code and place it, by default, in a file named _<schemaname>.erdplus_
in the directory as below:

```
password:  # (it's blank on the container)
Writing to file: /national-voter-file/docs/relational-diagrams/public.erdplus
Done!
```

On older version of Docker for OS X, you may have to use the
IP address of your virtual machine, instead of `localhost`, to connect.
Get it by typing this on the command line:

```
docker-machine ip
```


[erdplus]: https://erdplus.com/
