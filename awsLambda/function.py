from __future__ import print_function

import argparse
import sys
import textwrap
import json
import csv
import os
from datetime import datetime
from cloud import getS3File

from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, HyperException, print_exception

collationObj = {"AR" : Collation.AR , "BINARY" : Collation.BINARY , "CS" : Collation.CS , "CS_CI" : Collation.CS_CI , "CS_CI_AI" : Collation.CS_CI_AI , "DA" : Collation.DA , "DE" : Collation.DE , "EL" : Collation.EL , "EN_GB" : Collation.EN_GB , "EN_US" : Collation.EN_US , "EN_US_CI" : Collation.EN_US_CI , "ES" : Collation.ES , "ES_CI_AI" : Collation.ES_CI_AI , "ET" : Collation.ET , "FI" : Collation.FI , "FR_CA" : Collation.FR_CA , "FR_FR" : Collation.FR_FR , "FR_FR_CI_AI" : Collation.FR_FR_CI_AI , "HE" : Collation.HE , "HU" : Collation.HU , "IS" : Collation.IS , "IT" : Collation.IT , "JA" : Collation.JA , "JA_JIS" : Collation.JA_JIS , "KO" : Collation.KO , "LT" : Collation.LT , "LV" : Collation.LV , "NL_NL" : Collation.NL_NL , "NN" : Collation.NN , "PL" : Collation.PL , "PT_BR" : Collation.PT_BR , "PT_BR_CI_AI" : Collation.PT_BR_CI_AI , "PT_PT" : Collation.PT_PT , "ROOT" : Collation.ROOT , "RU" : Collation.RU , "SL" : Collation.SL , "SV_FI" : Collation.SV_FI , "SV_SE" : Collation.SV_SE , "TR" : Collation.TR , "UK" : Collation.UK , "VI" : Collation.VI , "ZH_HANS_CN" : Collation.ZH_HANS_CN , "ZH_HANT_TW" : Collation.ZH_HANT_TW }

typeObj = { "BOOLEAN" : Type.BOOLEAN , "CHAR_STRING" : Type.CHAR_STRING , "DATE" : Type.DATE , "DATETIME" : Type.DATETIME , "DOUBLE" : Type.DOUBLE , "DURATION" : Type.DURATION , "INTEGER" : Type.INTEGER , "SPATIAL" : Type.SPATIAL , "UNICODE_STRING" : Type.UNICODE_STRING }

def importSchema(
    schema,
    extractFilename
):
    try:
        extract = Extract( extractFilename )

        if 'name' not in schema:
            errMsg = 'No table name defined in the schema json. Key \'name\' required of type STRING'
            print('[ERROR]', errMsg, ':\n', e, '\nExiting now.')
            return respond(True, errMsg)
            exit( -1 )

        if 'columns' not in schema:
            errMsg = 'No columns defined in the schema json. Key \'columns\' required of type [OBJECT]'
            print('[ERROR]', errMsg, ':\n', e, '\nExiting now.')
            return respond(True, errMsg)
            exit( -1 )

        if ( not extract.hasTable( schema['name'] ) ):
            schema = TableDefinition()

            if ( 'collation' in schema and schema['collation'] in collationObj):
                schema.setDefaultCollation( collationObj[schema['collation']] )
            else:
                schema.setDefaultCollation( collationObj['BINARY'] )

            schema = TableDefinition()
            for c in schema['columns']:
                if ( 'name' not in c ):
                    errMsg = 'No column name defined in the schema json. Key \'name\' required of type STRING'
                    print('[ERROR]', errMsg, ':\n', e, '\nExiting now.')
                    return respond(True, errMsg)
                    exit( -1 )
                colType = Type.UNICODE_STRING
                if( 'type' in c and c['type'] in typeObj):
                    colType = typeObj[c['type']]
                if( 'collation' in c and c['collation'] in collationObj):
                    schema.addColumnWithCollation( c['name'], colType, c['collation'] )
                else:
                    schema.addColumn( c['name'], colType )

            table = extract.addTable( schema['name'], schema )
            if ( table == None ):
                errMsg = 'A fatal error occurred while creating the table'
                print('[ERROR]', errMsg, ':\n', e, '\nExiting now.')
                return respond(True, errMsg)
                exit( -1 )


    except TableauException as e:
        errMsg = 'A fatal error occurred while reading the schema definition'
        print('[ERROR]', errMsg, ':\n', e, '\nExiting now.')
        return respond(True, errMsg)
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
                            if (type == 15 or type == 16):
                                row.setCharString( cellIdx, cell)
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
        errMsg = 'A fatal error occurred while populating the extract'
        print('[ERROR]', errMsg, ':\n', e, '\nExiting now.')
        return respond(True, errMsg)
        exit( -1 )


def lambda_handler(event, context):
    # Parse Arguments

    print(event)

    ACCESS_KEY = event.accesskey
    SECRET_KEY = event.secretkey
    BUCKET = event.bucket
    FOLDER = event.folder
    FILENAME = event.csvfilename
    HYPERFILE = '/tmp/' + event.csvfilename + '.hyper'
    SCHEMA = event.schema
    SKIP = event.skip


    s3get = getS3File(ACCESS_KEY, SECRET_KEY, BUCKET, FILENAME)

    # Initialize the Tableau Extract API
    ExtractAPI.initialize()

    if (os.path.exists(HYPERFILE)):
        print('[INFO] Overwriteing existing', HYPERFILE, 'file')
        os.remove(HYPERFILE)

    # Create or Expand the Extract
    extract = importSchema( SCHEMA, HYPERFILE )
    populateExtract( extract, SCHEMA, FILENAME, SKIP )

    # Flush the Extract to Disk
    extract.close()

    # Close the Tableau Extract API
    ExtractAPI.cleanup()

    extract_processed = open(HYPERFILE, 'rb')
    extract_processed_data = extract_processed.read()
    extract_processed.close()
    extract_64_encode = base64.encodestring(extract_processed_data)

    return respond(False, extract_64_encode)

def respond(err, res):
    return {
        'statusCode': '400' if err else '200',
        'body': res,
        'headers': {
            'Content-Type': 'application/octet-stream',
        },
        'isBase64Encoded': 'true'
    }
