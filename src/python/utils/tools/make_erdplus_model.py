"""
make_erdplus_model

usage:
    python3 make_erdplus_model.py --ip=127.0.0.1 --user=postgres
    python3 make_erdplus_model.py --help

Programmatically create a (JSON-formatted) file for viewing in the
free online database modeling tool, ERDplus (https://erdplus.com),
using Postgres catalog tables to obtain all user-created table names
and columns in the user's schema of choice:
(https://www.postgresql.org/docs/9.5/static/catalogs.html)
"""
import argparse
import getpass
import json
import logging
import os
import psycopg2

from collections import namedtuple
from contextlib import closing


def to_erd_type(pg_type, modifier):
    """Map from postgres-style datatypes to ERDplus-style."""
    if pg_type == 'int4':
        dataType, dataTypeSize = 'int', 4
    elif pg_type == 'int8':
        dataType, dataTypeSize = 'int', 8
    elif pg_type == 'date':
        dataType, dataTypeSize = 'date', None
    elif pg_type == 'varchar':
        dataType = 'varcharn'
        dataTypeSize = modifier
    elif pg_type == 'bpchar':
        dataType = 'charn'
        dataTypeSize = modifier
    else:
        dataType, dataTypeSize = 'custom', pg_type
    return dataType, dataTypeSize


def _get_columns(args):
    """Return the columns as an array of namedtuples.

    The rows are namedtuples with entries:
        'tablename', 'column', 'type', 'type_modifier',
        'nullable', 'constraint', 'fkey',
        'constraint_name'

    Example usage:
        result = _get_columns(args)
        print(result[0].tablename)

    Passes error:
        psycopg2.Error if there's a database connection problem.
    """
    connection_details = dict(
        host=args['ip'],
        port=args['port'],
        database=args['dbname'],
        user=args['username'],
        password=args['password']
    )
    # NOTE: remember to modify both the Row here and the query below.
    Column = namedtuple('Column', (
        'tablename'
        ,'column'
        ,'type'
        ,'type_modifier'
        ,'nullable'
        ,'constraint'
        ,'fkey_source'
    ))
    query = """
        WITH pkeys AS (  -- the primary key constraints
            SELECT
                pga.attrelid AS table_id
                ,pga.attnum
            FROM pg_catalog.pg_constraint AS pgc
            JOIN pg_catalog.pg_attribute AS pga
                ON pga.attnum = ANY(pgc.conkey)
                AND pga.attrelid = pgc.conrelid
            WHERE contype = 'p'
        ),
        conns AS (  -- the foreign key constraints
            SELECT
                %s || '.' || c.relname AS srcname
                ,dest.attrelid AS dest_oid
                ,dest.attnum  AS dest_colnum
            FROM pg_catalog.pg_constraint AS pgc
            JOIN pg_catalog.pg_attribute AS dest
                ON dest.attrelid = pgc.conrelid
                AND dest.attnum = ANY(pgc.conkey)
            JOIN pg_catalog.pg_attribute AS src
                ON src.attrelid = pgc.confrelid
                AND src.attnum = ANY(pgc.confkey)
            JOIN pg_catalog.pg_class AS c
                ON c.oid = src.attrelid
        )
        SELECT
            %s || '.' || c.relname as tablename
            ,a.attname
            ,t.typname
            ,a.atttypmod
            ,NOT a.attnotnull AS nullable
            ,CASE WHEN pkeys.attnum IS NOT NULL THEN 'pkey'
                  WHEN conns.dest_oid IS NOT NULL THEN 'unique'
             ELSE NULL END AS constraint
            ,conns.srcname AS fkey_source
        FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
          JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
          LEFT JOIN pkeys ON
            pkeys.table_id = a.attrelid AND pkeys.attnum = a.attnum
          LEFT JOIN conns ON
            conns.dest_oid = a.attrelid AND conns.dest_colnum = a.attnum
        WHERE n.nspname = %s
          AND c.relkind = 'r'
          AND t.typname not in ('cid', 'oid', 'tid', 'xid')
    ;
    """
    with closing(psycopg2.connect(**connection_details)) as conn:
        cursor = conn.cursor()
        cursor.execute(query, [args['schema']] * 3)
        result = [Column(*c) for c in cursor.fetchall()]
    return result


##------------------------------------- construct the geometry -----------##
# voter_model['shapes'][<id>]['type'] is in ('Fact', 'Dimension'), and
# voter_model['shapes'][<id>]['details'] has keys:
#  'id', 'y', 'x', 'sort', 'uniqueGroups', 'attributes', 'name'
def create_shape(name, y=10, x=20, uniqueGroups=None):
    """Return a table shape."""
    if uniqueGroups is None:
        uniqueGroups = []
    id = get_id(name)
    shape_type = 'Fact' if 'fact' in name.lower() else 'Dimension'
    details = dict(
        name=name,
        id=id,
        y=y,
        x=x + 10 * id,
        sort='automatic',
        uniqueGroups=uniqueGroups,
        attributes=[]
    )
    return dict(type=shape_type, details=details)


def create_column(id, entry):
    """Return a table shape."""
    datatype, size = to_erd_type(entry.type, entry.type_modifier)
    attribute = dict(
        names=[entry.column],
        pkMember=(entry.constraint == 'pkey'),
        dataType=datatype,
        dataTypeSize=size,
        order=id,
        soloUnique=(entry.constraint == 'unique'),
        optional=entry.nullable,
        id=id,
        fk=entry.fkey_source is not None)
    return attribute


def create_connection(source_table, dest_table, dest_col, tables):
    """Return an connection object."""
    connection_id = tuple([dest_table, dest_col])
    source_id = get_id(source_table)
    destination_id = get_id(dest_table)
    attribute_loc = next(
        attr for attr in tables[dest_table]['details']['attributes']
        if dest_col in attr['names']
    )
    attribute_id = attribute_loc['id']
    connection = dict (
        type='TableConnector',
        source=source_id,
        destination=destination_id,
        details=dict(id=get_id(connection_id), fkAttributeId=attribute_id)
    )
    return connection


def get_id(object_name, __entries={}):
    """Look up and cache new unique IDs for the tables and connections."""
    if object_name not in __entries:
        maxval = 0 if not __entries else max(__entries.values())
        __entries[object_name] = 1 + maxval
    return __entries[object_name]


def get_new_model(width=1400, height=1600):
    """Return an initialized ERDplus object."""
    return dict(
        www='erdplus.com',
        version=2,
        connectors=[],
        shapes=[],
        width=width,
        height=height
    )


def make_erdplus_model(args):
    """Query the database, make the ERDplus model, and write it to a file.

    The arguments are from the command line (see get_parser()).
    An ERDplus file contains one large JSON object that lists
    the tables and columns, and connections between them.
    """
    new_model = get_new_model()
    data = _get_columns(args)
    tables = {}
    connections = {}
    tablenames = set(d.tablename for d in data)
    groups = dict((t,[d for d in data if d.tablename==t]) for t in tablenames)
    for table_name, columns in groups.items():
        logging.info("working on table: {}".format(table_name))
        tables[table_name] = create_shape(table_name)
        new_model['shapes'].append(tables[table_name])
        tables[table_name]['details']['attributes'] = [
            create_column(i, col) for i, col in enumerate(columns)
        ]
        for fkey in (c for c in columns if c.fkey_source is not None):
            connections[(table_name, fkey.column)] = fkey.fkey_source
    for dest, source_table in connections.items():
        dest_table, dest_col = dest
        logging.info("working on: {} to {}".format(source_table, dest_table))
        new_model['connectors'].append(create_connection(source_table, dest_table, dest_col, tables))
    logging.info('Writing to file: {}'.format(args['outfile']))
    print('Writing to file: {}'.format(args['outfile']))
    with open(args['outfile'], 'w') as outfile:
        outfile.write(json.dumps(new_model, separators=(',',':')))


def get_parser():
    """Return an argparse.ArgumentParser with reasonable defaults."""
    outfile = os.path.join(
        os.path.abspath(__file__).split('national-voter-file')[0],
        'national-voter-file',
        'docs',
        'relational-diagrams',
        '{schema}.erdplus'
    )
    default = dict(
        ip='127.0.0.1',
        port=5432,
        dbname='VOTER',
        username='postgres',
        schema='public',
        outfile=outfile
    )
    parser = argparse.ArgumentParser(
        description='make an *.erdplus relational model from the chosen schema')
    parser.add_argument(
        '-i', '--ip',
        help='database server ip (default: "{ip}")'.format(**default),
        default=default['ip'])
    parser.add_argument(
        '-p', '--port',
        help='database server port (default: {port})'.format(**default),
        default=default['port'],
        type=int)
    parser.add_argument(
        '-d', '--dbname',
        help='database name to connect to (default: "{dbname}")'.format(**default),
        default=default['dbname'])
    parser.add_argument(
        '-U', '--username',
        help='database user name (default: "{username}")'.format(**default),
        default=default['username'])
    parser.add_argument(
        '-s', '--schema',
        help='desired schema to build (default: "{schema}")'.format(**default),
        default=default['schema'])
    parser.add_argument(
        '-o', '--outfile',
        help='output file path (default: "{outfile}")'.format(**default),
        default=default['outfile'])
    return parser


if __name__ == '__main__':
    parser = get_parser()
    args = vars(parser.parse_args())
    args['password'] = getpass.getpass('password:')
    args['outfile'] = args['outfile'].format(**args)
    try:
        make_erdplus_model(args)
        print("Done!")
    except psycopg2.Error as e:
        logging.critical(e)
        print("Database error:", e)
        parser.print_help()
        print(
            "If you can't connect to `localhost` on OSX, try "
            "the IP address in `docker-machine ip`."
        )
