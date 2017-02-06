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


def run(config, sleep_duration=0.5):
    while True:
        log.info('Running the main module.')

        db_config = config['databases']
        log.info('Database configurations:\n{}'.format(db_config))

        mapzen_config = config['mapzen']
        log.info('Mapzen configuration:\n{}'.format(mapzen_config))

        tamu_config = config['tamu']
        log.info('Tamu configuration:\n{}'.format(tamu_config))

        run_geocoder(config, log)
        time.sleep(sleep_duration)
