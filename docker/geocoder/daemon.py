#!/usr/bin/python
# The python3 path is intentionally hardcoded to set the daemon's process
# name as this script's name (instead of python3).
"""
Start the process that will query addresses in household_dim, get geolocations
from one of a number of web APIs, and place them back into the database.

"""
import argparse
import json
import logging
import logging.config
import os
import pwd
import signal
import sys


import run

def get_full_path(relative_path):
    this_dir = os.path.dirname(os.path.abspath(__file__))
    full_relative_path = os.path.abspath('/'.join((this_dir, relative_path)))
    return full_relative_path

#------------------------------------------
# Logging

def setup_logfile_directory(log_config):
    """True if successfully created/confirmed existence of the director(y|ies).
    """
    did_something = False
    if 'handlers' in log_config:
        for h in log_config['handlers'].values():
            if 'filename' in h:
                h['filename'] = get_full_path(h['filename'])
                os.makedirs(os.path.dirname(h['filename']), exist_ok=True)
                did_something=True
    return did_something


def setup_logging(config, stdout=False):
    log = logging.getLogger()
    if stdout:
        log.setLevel(logging.DEBUG)
        streamHandler = logging.StreamHandler(sys.stdout)
        streamHandler.setFormatter(logging.Formatter(
            "%(asctime)s [%(processName)s]:%(name)s %(levelname)s %(message)s"
        ))
        streamHandler.setLevel(logging.DEBUG)
        log.addHandler(streamHandler)
    else:
        try:
            if 'logging' in config:
                if setup_logfile_directory(config['logging']):
                    logging.config.dictConfig(config['logging'])
                else:
                    sys.stderr.write(
                            'Either no filenames or no handlers given in '
                            'config "logging" entry.\n')
            else:
                sys.stderr.write(
                        'Configuration has no "logging" entry.\n'
                        '...logs will not be written to a file.\n')

        except Exception as e:
            sys.stderr.write("Error when initializing logging: {}\n".format(e))


#------------------------------------------
# Configuration

def get_configuration(options):
    configfile = os.path.abspath(options.configfile)
    if os.path.exists(configfile):
        try:
            config = json.load(open(options.configfile))
            return config
        except ValueError as e:
            msg = 'ERROR: JSON formatting problem in {}.\n'
            sys.stderr.write(msg.format(options.configfile))
            sys.stderr.write(e)
            sys.exit(1)
    else:
        msg = 'ERROR: Config file: {} does not exist.\n'
        sys.stderr.write(msg.format(options.configfile))
        return None


#------------------------------------------
# Command-line interaction

def get_parser():
    this_directory = os.path.dirname(os.path.abspath(__file__))
    default = dict(
        configfile=os.path.join(this_directory, 'daemon.conf'),
        pidfile=None,
        skip_pidfile=False
    )
    usage = """
        Usage:
            docker-compose run -d \\
                etl /national-voter-file/src/main/python/geocoder/daemon.py

        To kill the daemonized container later:
            docker exec `cat ../src/main/python/geocoder/RUNNING` kill -9 1
    """
    description = """
         {}\nConfigure connection settings and logging options using
         the file daemon.conf in ({}).
    """.format(__doc__, this_directory)
    parser = argparse.ArgumentParser(usage=usage, description=description)

    parser.add_argument(
        '-c', '--configfile',
        default=default['configfile'],
        help='config file (default: "{configfile}")'.format(**default))

    parser.add_argument(
        '-l', '--log-stdout',
        default=False,
        action='store_true',
        help='log to stdout')

    parser.add_argument(
        '-p', '--pidfile',
        default=default['pidfile'],
        help='pid file (default: from the config file)')

    return parser



def main():
    try:
        # 1. Parse the command line options
        parser = get_parser()
        options = parser.parse_args()

        config = get_configuration(options)
        if config is None:
            parser.print_help(sys.stderr)
            sys.exit(1)

        # 2. Setup logging
        setup_logging(config, options.log_stdout)
        log = logging.getLogger()

    # Pass an exit upstream rather then handle it as an general exception
    except SystemExit as e:
        raise SystemExit

    except Exception as e:
        import traceback
        sys.stderr.write('\n'.join((
            'Unhandled exception: {}'.format(e),
            'traceback: {}'.format(traceback.format_exc()),
            '')))
        sys.exit(1)

    # Different try/except ... (now using the logging system)
    try:
        run.run(config)

    # Pass the exit up stream rather then handle it as an general exception
    except SystemExit as e:
        raise SystemExit

    except Exception as e:
        import traceback
        log.error('Unhandled exception: {}'.format(e))
        log.error('traceback: {}'.format(traceback.format_exc()))
        sys.exit(1)


if __name__ == '__main__':
    main()
