# Adam Kippenhan ajk8sb 03/27/2022 ETLdataProcessor.py

import os
import pandas as pd
from sqlalchemy import create_engine
import sys

# args format is 'python dataProcessor.py <data source> <output format>'
if len(sys.argv) < 3:
    print('usage: python ETLdataProcessor.py <data source> <output format>\nwhere <data source> is the filename or url for the data source to read from\nwhere <output type> is the desired output format and is one of CSV, JSON, SQL, XLSX')
    sys.exit()
filename = sys.argv[1]
# check if the data source file exists
if not os.path.exists(filename):
    print('Requested data source file \'' + filename + '\' does not exist.')
    sys.exit()
requestedOutputFormat = sys.argv[2].upper()
if requestedOutputFormat not in ['CSV', 'JSON', 'SQL', 'XLSX']:
    print('Error: Output format \'' + sys.argv[1] + '\' is not supported. Supported output formats are: CSV, JSON, SQL, XLSX')
    sys.exit()
# read data into pandas dataframe based on filename extension
try:
    filenameExtension = os.path.splitext(filename)[1].lower()[1:] # determine proper reading function for the file type based on the file extension
    if filenameExtension == requestedOutputFormat.lower(): # check if the requested output format is the same as the source file
        print('\'' + filename + '\' is already a ' + requestedOutputFormat + ' file')
        sys.exit()
    match filenameExtension:
        case 'csv':
            data = pd.read_csv(filename)
        case 'json':
            data = pd.read_json(filename)
        case 'xls' | 'xlsx' | '.xlsm' | 'xlsb' | 'odf' | 'ods' | 'odt':
            data = pd.read_excel(filename)
        case _: # default case
            data = pd.read_table(filename)
except Exception as e:
    print('Unable to read data source file \'' + filename + '\': ' + str(e))
    sys.exit()
match requestedOutputFormat:
    case 'SQL':
        newDbName = os.path.splitext(filename)[0].split('/')[-1] + 'Db' # new database name is data source filename with 'Db' at end eg: 'Air_Traffic_Cargo_StatisticsDb'
        newTblName = os.path.splitext(filename)[0].split('/')[-1] + 'Tbl' # same as above but with 'Tbl' at end
        print('Inserting data from \'' + filename + '\' into SQL database \'' + newDbName + '\' in table \'' + newTblName + '\'')
        sqlEngine = create_engine(f'mysql+pymysql://adam:password@localhost', pool_recycle=3600)
        sqlEngine.execute(f'DROP DATABASE IF EXISTS {newDbName};')
        sqlEngine.execute(f'CREATE DATABASE {newDbName};')
        sqlEngine.execute(f'USE {newDbName};')
        sqlEngine = create_engine(f'mysql+pymysql://adam:password@localhost/{newDbName}', pool_recycle=3600)
        data.to_sql(newTblName, con=sqlEngine.connect(), index=False, if_exists='append')
    case _:
        print('Converting \'' + filename + '\' to ' + requestedOutputFormat + ' format...')
        if not filename.startswith('data/'):
            newFilenameRoot = 'data/' + os.path.splitext(filename)[0].split('/')[-1] # split() is to remove any directory paths before actual filename
        else:
            newFilenameRoot = os.path.splitext(filename)[0]
        match requestedOutputFormat:
            case 'CSV':
                data.to_csv(newFilenameRoot + '.csv', index=False)
                print('Converted data now available in \'' + newFilenameRoot + '.csv\'')
            case 'JSON':
                data.to_json(newFilenameRoot + '.json')
                print('Converted data now available in \'' + newFilenameRoot + '.json\'')
            case 'XLSX':
                data.to_excel(newFilenameRoot + '.xlsx', index=False)
                print('Converted data now available in \'' + newFilenameRoot + '.xlsx\'')
pd.options.display.show_dimensions = False # remove dimensions from head() call below
print('\nData Summary:\n\nrecords: ' + str(data.shape[0]) + '\ncolumns: ' + str(data.shape[1]) + '\n\nData Preview:\n' + str(data.head()))