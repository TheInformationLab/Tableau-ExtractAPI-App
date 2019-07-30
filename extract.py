from __future__ import print_function

import argparse
import sys
import textwrap
import json
import csv
import os
from datetime import datetime
from downloads3 import getS3File

from tableausdk import *
from tableausdk.HyperExtract import *

collationObj = {"AR" : Collation.AR , "BINARY" : Collation.BINARY , "CS" : Collation.CS , "CS_CI" : Collation.CS_CI , "CS_CI_AI" : Collation.CS_CI_AI , "DA" : Collation.DA , "DE" : Collation.DE , "EL" : Collation.EL , "EN_GB" : Collation.EN_GB , "EN_US" : Collation.EN_US , "EN_US_CI" : Collation.EN_US_CI , "ES" : Collation.ES , "ES_CI_AI" : Collation.ES_CI_AI , "ET" : Collation.ET , "FI" : Collation.FI , "FR_CA" : Collation.FR_CA , "FR_FR" : Collation.FR_FR , "FR_FR_CI_AI" : Collation.FR_FR_CI_AI , "HE" : Collation.HE , "HU" : Collation.HU , "IS" : Collation.IS , "IT" : Collation.IT , "JA" : Collation.JA , "JA_JIS" : Collation.JA_JIS , "KO" : Collation.KO , "LT" : Collation.LT , "LV" : Collation.LV , "NL_NL" : Collation.NL_NL , "NN" : Collation.NN , "PL" : Collation.PL , "PT_BR" : Collation.PT_BR , "PT_BR_CI_AI" : Collation.PT_BR_CI_AI , "PT_PT" : Collation.PT_PT , "ROOT" : Collation.ROOT , "RU" : Collation.RU , "SL" : Collation.SL , "SV_FI" : Collation.SV_FI , "SV_SE" : Collation.SV_SE , "TR" : Collation.TR , "UK" : Collation.UK , "VI" : Collation.VI , "ZH_HANS_CN" : Collation.ZH_HANS_CN , "ZH_HANT_TW" : Collation.ZH_HANT_TW }

typeObj = { "BOOLEAN" : Type.BOOLEAN , "CHAR_STRING" : Type.CHAR_STRING , "DATE" : Type.DATE , "DATETIME" : Type.DATETIME , "DOUBLE" : Type.DOUBLE , "DURATION" : Type.DURATION , "INTEGER" : Type.INTEGER , "SPATIAL" : Type.SPATIAL , "UNICODE_STRING" : Type.UNICODE_STRING }

def parseArguments():
    parser = argparse.ArgumentParser( description='Tableau S3 CSV to Hyper ÇParser by Craig Bloodworth, The Information Lab', formatter_class=argparse.RawTextHelpFormatter )
    parser.add_argument( '-s', '--schema', action='store', metavar='SCHEMA', required=True,
                help=textwrap.dedent('''\
                    JSON file containing the SCHEMA of the source CSV file.
                    (required)
                    ''' ) )
    parser.add_argument( '-f', '--filename', action='store', metavar='FILENAME', required=True,
				help=textwrap.dedent('''\
				   Source CSV file containing data to add to the extract.
				   (required)
				   ''' ) )
    parser.add_argument( '-a', '--accesskey', action='store', metavar='ACCESSKEY', required=True, #default=,
				help=textwrap.dedent('''\
				   AWS Access Key with permission to read the S3 bucket.
				   (required)
				   ''' ) )
    parser.add_argument( '-k', '--secretkey', action='store', metavar='SECRETKEY', required=True, #default=,
				help=textwrap.dedent('''\
				   AWS Secret Key belonging to the access key.
				   (required)
				   ''' ) )
    parser.add_argument( '-b', '--bucket', action='store', metavar='BUCKET', required=True, #default=,
				help=textwrap.dedent('''\
				   AWS S3 Bucket containing the source CSV file.
				   (required)
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
    parser.add_argument( '-n', '--skip', action='store', metavar='SKIP', default=0,
				help=textwrap.dedent('''\
				   Skip first n rows. Defaults to zero.
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

    return vars( parser.parse_args() )

def importSchema(
    schemaFilename,
    extractFilename
):
    try:
        extract = Extract( extractFilename )
        with open(schemaFilename) as schema_file:
            data = json.load(schema_file)

            if 'name' not in data:
                print('[ERROR] No table name defined in the schema json. Key \'name\' required of type STRING:\nExiting now\n.')
                exit( -1 )

            if 'columns' not in data:
                print('[ERROR] No columns defined in the schema json. Key \'columns\' required of type [OBJECT]:\nExiting now\n.')
                exit( -1 )

            if ( not extract.hasTable( data['name'] ) ):
                schema = TableDefinition()

                if ( 'collation' in data and data['collation'] in collationObj):
                    schema.setDefaultCollation( collationObj[data['collation']] )
                else:
                    schema.setDefaultCollation( collationObj['BINARY'] )

                schema = TableDefinition()
                for c in data['columns']:
                    if ( 'name' not in c ):
                        print('No column name defined in the schema json. Key \'name\' required of type STRING:\nExiting now\n.')
                        exit( -1 )
                    colType = Type.UNICODE_STRING
                    if( 'type' in c and c['type'] in typeObj):
                        colType = typeObj[c['type']]
                    if( 'collation' in c and c['collation'] in collationObj):
                        schema.addColumnWithCollation( c['name'], colType, c['collation'] )
                    else:
                        schema.addColumn( c['name'], colType )

                table = extract.addTable( data['name'], schema )
                if ( table == None ):
                    print('[ERROR] A fatal error occurred while creating the table:\nExiting now\n.')
                    exit( -1 )

    except TableauException as e:
        print('[ERROR] A fatal error occurred while reading the schema definition:\n', e, '\nExiting now.')
        exit( -1 )

    return extract

#------------------------------------------------------------------------------
#   Populate Extract
#------------------------------------------------------------------------------
#   (NOTE: This function assumes that the Tableau SDK Extract API is initialized)
def populateExtract(
    extract,
    schemaFilename,
    csvFilename,
    skip
):
    try:
        with open(schemaFilename) as schema_file:
            schemaJSON = json.load( schema_file )
            table = extract.openTable( schemaJSON['name'] )

            schema = table.getTableDefinition()
            colCount = schema.getColumnCount();
            with open(csvFilename, mode='r') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                rowIdx = 0
                for csvrow in csv_reader:
                    rowIdx = rowIdx + 1
                    if (rowIdx > skip):
                        cellIdx = 0
                        row = Row( schema )
                        for cell in csvrow:
                            type = schema.getColumnType(cellIdx)
                            if (type == 15):
                                row.setCharString( cellIdx, cell)
                            elif (type == 16):
                                row.setString( cellIdx, cell)
                            elif (type == 11):
                                row.setBoolean( cellIdx, bool(cell))
                            elif (type == 7):
                                row.setInteger( cellIdx, int(cell))
                            elif (type == 12):
                                dateObj = datetime.strptime(cell)
                                row.setDate( cellIdx, dateObj.year, dateObj.month, dateObj.day)
                            else:
                                print('[WARN] Unknown column type', schema.getColumnName(cellIdx), type)

                            if (cellIdx >= colCount):
                                print('[WARN] More columns in CSV than defined in schema. Skipping to next row.')
                                break
                            cellIdx = cellIdx + 1
                        table.insert(row)

    except TableauException as e:
        print('[ERROR] A fatal error occurred while populating the extract:\n', e, '\nExiting now.')
        exit( -1 )

#------------------------------------------------------------------------------
#   Main
#------------------------------------------------------------------------------
def main():
    # Parse Arguments

    #Demo Key: AKIATK6I3PBQPNZKAKFL Secret: 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
    options = parseArguments()

    if not options[ 'localread' ]:
        s3get = getS3File(options[ 'accesskey' ], options[ 'secretkey' ], options[ 'bucket' ], options[ 'filename' ], folder = options[ 'path' ])

    # Initialize the Tableau Extract API
    ExtractAPI.initialize()

    if (os.path.exists(options[ 'output' ]) and options[ 'overwrite' ]):
        print('[INFO] Overwriteing existing', options[ 'output' ], 'file')
        os.remove(options[ 'output' ])

    # Create or Expand the Extract
    extract = importSchema( options[ 'schema' ], options[ 'output' ] )
    populateExtract( extract, options[ 'schema' ], options[ 'filename' ], options[ 'skip' ] )

    # Flush the Extract to Disk
    extract.close()

    # Close the Tableau Extract API
    ExtractAPI.cleanup()

    return 0

if __name__ == "__main__":
    retval = main()
    sys.exit( retval )
