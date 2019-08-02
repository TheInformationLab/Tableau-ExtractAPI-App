from __future__ import print_function

import argparse
import sys
import textwrap
import json
import csv
import os

from tqdm import tqdm
from datetime import datetime
from cloud import getS3File

from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, HyperException, print_exception

typeObj = { "BIG_INT" : SqlType.big_int, "BOOLEAN" : SqlType.bool , "BYTES" : SqlType.bytes , "CHAR" : SqlType.char , "DATE" : SqlType.date, "DOUBLE" : SqlType.double, "GEOGRAPHY" : SqlType.geography, "INT" : SqlType.int, "INTERVAL" : SqlType.interval, "JSON" : SqlType.json, "NUMERIC" : SqlType.numeric, "OID" : SqlType.oid, "SMALL_INT" : SqlType.small_int, "TEXT" : SqlType.text, "TIME" : SqlType.time, "TIMESTAMP" : SqlType.timestamp, "TIMESTAMP_TZ" : SqlType.timestamp_tz, "VARCHAR" : SqlType.varchar }

def parseArguments():
    parser = argparse.ArgumentParser( description='Tableau CSV to Hyper Parser by Craig Bloodworth, The Information Lab', formatter_class=argparse.RawTextHelpFormatter )
    parser.add_argument( '-s', '--schema', action='store', metavar='SCHEMA', required=True,
                help=textwrap.dedent('''\
                    JSON file containing the SCHEMA of the source CSV file.
                    (required)
                    ''' ) )
    parser.add_argument( '-a', '--accesskey', action='store', metavar='ACCESSKEY', #required=True, #default=,
				help=textwrap.dedent('''\
				   AWS Access Key with permission to read the S3 bucket.
				   (optional)
				   ''' ) )
    parser.add_argument( '-k', '--secretkey', action='store', metavar='SECRETKEY', #required=True, #default=,
				help=textwrap.dedent('''\
				   AWS Secret Key belonging to the access key.
				   (optional)
				   ''' ) )
    parser.add_argument( '-b', '--bucket', action='store', metavar='BUCKET', #required=True, #default=,
				help=textwrap.dedent('''\
				   AWS S3 Bucket containing the source CSV file.
				   (optional)
				   ''' ) )
    parser.add_argument( '-p', '--path', action='store', metavar='PATH', #default=,
				help=textwrap.dedent('''\
				   Path to the CSV file if using subfolders. No trailing slash!
				   (optional, e.g. /folder/subfolder)
				   ''' ) )
    parser.add_argument( '-o', '--output', action='store', metavar='OUTPUT', default='output.hyper',
				help=textwrap.dedent('''\
				   Filename of the extract to be created or extended.
				   (optional, default='%(default)s')
				   ''' ) )
    parser.add_argument( '-d', '--delimiter', action='store', metavar='DELIMITER', default=',',
				help=textwrap.dedent('''\
				   Specify the CSV delimiter character. Use \\\\t for tab delimited.
				   (optional, default='%(default)s')
				   ''' ) )
    parser.add_argument( '-i', '--ignoreheader', action='store_true',
				help=textwrap.dedent('''\
				   Skip first line of each CSV file as they contain a header line.
				   (optional, default='%(default)s')
				   ''' ) )
    parser.add_argument( '-w', '--overwrite', action='store_true',
				help=textwrap.dedent('''\
				   Overwrite existing hyper file.
				   (optional, default='%(default)s')
				   ''' ) )
    parser.add_argument( '-l', '--localread', action='store_true',
				help=textwrap.dedent('''\
				   Skip S3 download and read local cached file.
				   (optional, default='%(default)s')
				   ''' ) )
    parser.add_argument( '-t', '--temptable', action='store_true',
				help=textwrap.dedent('''\
				   Load first into a temp table.
				   (optional, default='%(default)s')
				   ''' ) )

    return vars( parser.parse_args() )

def importTextSchema(
    schemaJson
):
    try:
        if 'name' not in schemaJson:
            print('[ERROR] No table name defined in the schema json. Key \'name\' required of type STRING:\nExiting now\n.')
            exit( -1 )

        if 'columns' not in schemaJson:
            print('[ERROR] No columns defined in the schema json. Key \'columns\' required of type [OBJECT]:\nExiting now\n.')
            exit( -1 )

        print('[INFO] Creating temp landing table.')
        table_def = TableDefinition('temptable');

        for c in schemaJson['columns']:
            colFunc = typeObj['TEXT']
            colType = colFunc()
            table_def.add_column(c['name'], colType)

        if ( table_def == None ):
            print('[ERROR] A fatal error occurred while creating the temp table:\nExiting now\n.')
            exit( -1 )

    except HyperException as e:
        print('[ERROR] A fatal error occurred while reading the schema definition:\n', e, '\nExiting now.')
        exit( -1 )

    return table_def

def importSchema(
    schemaJson
):
    try:
        if 'name' not in schemaJson:
            print('[ERROR] No table name defined in the schema json. Key \'name\' required of type STRING:\nExiting now\n.')
            exit( -1 )

        if 'columns' not in schemaJson:
            print('[ERROR] No columns defined in the schema json. Key \'columns\' required of type [OBJECT]:\nExiting now\n.')
            exit( -1 )

        table_def = TableDefinition(schemaJson['name']);

        for c in schemaJson['columns']:
            if ( 'name' not in c ):
                print('No column name defined in the schema json. Key \'name\' required of type STRING:\nExiting now\n.')
                exit( -1 )
            colFunc = typeObj['TEXT']
            if( 'type' in c and c['type'] in typeObj):
                colFunc = typeObj[c['type']]
                if (c['type'] == 'CHAR'):
                    if ( 'length' not in c ):
                        print('No length defined for CHAR column', c['name'],'\nKey \'length\' required of type INTEGER:\nExiting now\n.')
                        exit( -1 )
                    colType = colFunc(c['length'])
                elif (c['type'] == 'VARCHAR'):
                    if ( 'length' not in c ):
                        print('No length defined for VARCHAR column', c['name'],'\nKey \'length\' required of type INTEGER:\nExiting now\n.')
                        exit( -1 )
                    colType = colFunc(c['length'])
                elif (c['type'] == 'NUMERIC'):
                    if ( 'precision' not in c or 'scale' not in c ):
                        print('No precision and/or scale defined for NUMERIC column', c['name'],'\nKeys \'precision\' and \'scale\' required of type INTEGER:\nExiting now\n.')
                        exit( -1 )
                    colType = colFunc(c['precision'], c['scale'])
                else:
                    colType = colFunc()
            else:
                colType = colFunc()
            if( 'collation' in c and c['collation'] in collationObj):
                colCollation = collationObj[c['collation']]
                table_def.add_column(c['name'], colType, colCollation)
            else:
                table_def.add_column(c['name'], colType)

        if ( table_def == None ):
            print('[ERROR] A fatal error occurred while creating the table:\nExiting now\n.')
            exit( -1 )

    except HyperException as e:
        print('[ERROR] A fatal error occurred while reading the schema definition:\n', e, '\nExiting now.')
        exit( -1 )

    return table_def

#------------------------------------------------------------------------------
#   Populate Extract
#------------------------------------------------------------------------------
def populateExtract(
    connection,
    schemaJson,
    filepath,
    ignoreheader,
    delimiterChar,
    temptable
):
    try:
        header = ''
        if (ignoreheader):
            header = ' HEADER'
            print('[INFO] Header flag specified. Ignoring first row of each file')
        delimiter = 'WITH (FORMAT CSV)'
        if (delimiterChar == '\\t'):
            delimiter = 'DELIMITER E\'\\t\' CSV'
        elif (delimiterChar != ','):
            delimiter = 'DELIMITER \'' + delimiterChar + '\' CSV'

        if (temptable) :
            SQLCMD = 'COPY temptable FROM \'' + filepath + '\' ' + delimiter + header + ';'
            print('[INFO] SQLCMD:', SQLCMD)
            connection.execute_command(SQLCMD)
            SQLCMD = 'INSERT INTO ' + schemaJson['name'] + ' SELECT * FROM temptable;'
            print('[INFO] SQLCMD:', SQLCMD)
            connection.execute_command(SQLCMD)
            SQLCMD = 'DROP TABLE temptable;'
            print('[INFO] SQLCMD:', SQLCMD)
            connection.execute_command(SQLCMD)
        else:
            SQLCMD = 'COPY ' + schemaJson['name'] + ' FROM \'' + filepath + '\' ' + delimiter + header + ';'
            print('[INFO] SQLCMD:', SQLCMD)
            connection.execute_command(SQLCMD)

    except HyperException as e:
        print('[ERROR] A fatal error occurred while populating the extract:\n', print_exception(e), '\nExiting now.')
        exit( -1 )

#------------------------------------------------------------------------------
#   Main
#------------------------------------------------------------------------------
def main():
    # Parse Arguments

    #Demo Key: AKIATK6I3PBQPNZKAKFL Secret: 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
    options = parseArguments()

    # Create the table schema
    print('[INFO] Importing schema file', options['schema'])
    with open(options['schema']) as schema_file:
        schemaJson = json.load(schema_file)
        if (options['temptable']):
            tempschema = importTextSchema ( schemaJson )
        schema = importSchema( schemaJson )
        print('[INFO] Schema Imported')

        overwrite = CreateMode.CREATE
        if (os.path.exists(options[ 'output' ]) and options[ 'overwrite' ]):
            print('[INFO] Overwriteing existing', options[ 'output' ], 'file')
            overwrite = CreateMode.CREATE_AND_REPLACE

        print('[INFO] Creating new local Hyper instance')
        with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, 'TheInformationLab-CloudBucket-CSV') as hyper:
            # Create the extract, replace it if it already exists
            print('[INFO] Instance created. Building connection to', options['output'])
            with Connection(hyper.endpoint, options['output'], overwrite) as connection:
                print('[INFO] Connection established. Adding table to Hyper database')
                connection.catalog.create_table(schema)
                if (options['temptable']):
                    connection.catalog.create_table(tempschema)
                print('[INFO] The databse is ready to go. Let\'s get it populated')
                if 'files' not in schemaJson:
                    print('[ERROR] No files listed in the schema json. Key \'files\' required of type [STRING]:\nExiting now\n.')
                    exit( -1 )
                files = schemaJson['files']

                for file in tqdm(files, ascii=True, desc='[INFO] Importing'):
                    localfilepath = file
                    if not options[ 'localread' ]:
                        s3get = getS3File(options[ 'accesskey' ], options[ 'secretkey' ], options[ 'bucket' ], file, folder = options[ 'path' ])
                        localfilepath = './tmp/' + file
                    populateExtract(connection, schemaJson, localfilepath, options['ignoreheader'], options['delimiter'], options['temptable'])
                connection.close()
                print('[INFO] The data is in, your Hyper file is ready. Viz on Data Rockstar!')

    return 0

if __name__ == "__main__":
    retval = main()
    sys.exit( retval )
