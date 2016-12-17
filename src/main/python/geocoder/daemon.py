#!/usr/bin/python3
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
# Container ID management

def setup_pid(options, config):
    """This puts the Docker container ID in the PID file,

    because the PID is kind of meaningless...containers
    must have a process in the foreground to keep running.
    But this way, you can spin up a container with this and
    leave it going, and easily kill it later, using

    docker exec `cat <path/to/pidfile>` kill -9 1
    """
    log = logging.getLogger()
    # Initialize container id file
    if not options.pidfile:
        options.pidfile = str(config['pid_file'])

    options.pidfile = get_full_path(options.pidfile)
    # Read existing pid file
    try:
        pf = open(options.pidfile, 'r')
        container_id = pf.read().strip()
        pf.close()
    except (IOError):
        container_id = None

    # Check existing pid file
    if container_id:
        msg = 'ERROR: pid file exists. Container already running?\nID: {}\n'
        sys.stderr.write(msg.format(container_id))
        sys.exit(1)

    # Write pid file
    # Get the current docker container id
    with open('/proc/self/cgroup') as groups:
        line_with_container_id = next(g for g in groups if 'docker' in g)
        container_id = line_with_container_id.rsplit('/', 1)[-1]

    try:
        os.makedirs(os.path.dirname(options.pidfile), exist_ok=True)
        pf = open(options.pidfile, 'w+')
    except IOError as e:
        sys.stderr.write('Failed to write PID file: {}\n'.format(e))
        sys.exit(1)

    pf.write('{}\n'.format(container_id))
    pf.close()
    # Log
    log.debug('Wrote First PID file: {}'.format(options.pidfile))


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

    parser.add_argument(
        '--skip-pidfile',
        default=default['skip_pidfile'],
        action='store_true',
        help='Skip creating PID file')

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
        if not options.skip_pidfile:
            setup_pid(options, config)

            # Handle the interrupts
            def sigint_handler(signum, frame):
                log.info('Signal Received: {}'.format(signum))
                # Delete Pidfile
                if not options.skip_pidfile and os.path.exists(options.pidfile):
                    os.remove(options.pidfile)
                    log.debug('Removed PID file: {}'.format(options.pidfile))
                sys.exit(0)
    
            # Set the signal handlers
            signal.signal(signal.SIGINT, sigint_handler)
            signal.signal(signal.SIGTERM, sigint_handler)

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
