'''
create a data dictionary that maps distinct categorical or 'code' features 
to unique integers for retain model training and interpretation
'''

import pandas as pd
import pickle
import json
import csv
import sys
import argparse
import sqlalchemy
from sqlalchemy import create_engine
import urllib
#from sql_engine import initialize

def make_engine():
    conn_str = (
        'Driver={SQL Server};'
        'Server=TIGRIS;'
        'Database=WorkDBTest;'
        'Trusted_Connection=yes;'
    )
    quoted_conn_str = urllib.parse.quote_plus(conn_str)
    engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted_conn_str))
    return engine

def read_data_dict(filepath=None):
	'''
	read in HICOR data dictionary from csv
	data elements have an original SAS format, a SQL format, and a Temporal aspect
	this ammended version of read_data_dict only captures codes and their data types (binary vs char)
	'''
	datadict = {}
	with open('./data_dictionary.csv') as fin:
		reader = csv.DictReader(fin)
		for row in reader:
			if (row['DataTreatment'] == 'Code'):
				datadict[row['Name']] = row['DataType']

	return datadict

def map_codes_to_ints(datadict, engine,config):
	'''
	query distinct values of categorical features from full dataset
	store in pickled dictionary where {integer:string_code_feature}
	'''
	code_d = {}
	int_map_count = 1
	for col, dtype in datadict.items():
		if (dtype != 'Binary'):
			results = [r[0] for r in engine.execute('select distinct {} from {} WHERE {} is not NULL group by {}'.format(col, config['TABLE_NAME'], col, col))]
			for r in results:
				code_d[int_map_count] = "{}_{}".format(col, r)				
				int_map_count += 1
		else:
			code_d[int_map_count] = col
			int_map_count += 1
	with open('./dictionary_{}.pkl'.format(int_map_count-1), 'wb') as code_map:
		pickle.dump(code_d, code_map)

def parse_arguments(parser):
    """single argument for whether or not to use the sql trusted connection or the user login info in config.json"""
    parser.add_argument('--trusted', action='store_true',
                        help="use trusted mssql connection (requires proper sql server drivers)")
    args = parser.parse_args()
    return args

if __name__ == '__main__':
	PARSER = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	ARGS = parse_arguments(PARSER)

	with open('./config.json') as fin:
		config = json.load(fin)
	engine = make_engine()
	datadict = read_data_dict()
	map_codes_to_ints(datadict, engine, config)