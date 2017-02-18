"""
run
~~~

Empty main module; daemon.py will call `run.run(config)`, and
`config` will contain the entire dictionary object as written in
_daemon.conf_.
"""
import logging
import time
from es_geocoder import *

log = logging.getLogger()


def run(config, sleep_duration=0):
    # Managing loop on its own, don't need timings
    run_geocoder(config, log)
