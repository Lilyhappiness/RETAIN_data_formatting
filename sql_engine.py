from sqlalchemy import create_engine
import urllib

def initialize(config, trusted=False):
	if not config or not config.get('ENGINE'):
		raise ValueError("Must include valid config dict")
	sql_config = config['ENGINE']
	if trusted:
		print("Connecting to SQL Server using trusted connection...")
		conn_str = 'Driver={{SQL Server}};Server={};Database={};Trusted_Connection=yes;'.format(sql_config['host'], sql_config['db'])
		quoted_conn_str = urllib.parse.quote_plus(conn_str)
		return create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted_conn_str))
	print("Connecting to SQL Server using credentials...")
	return create_engine('mssql+pymssql://{}:{}@{}/{}'.format(urllib.parse.quote_plus(sql_config['user']),
															  urllib.parse.quote_plus(sql_config['pwd']),
															  sql_config['host'], sql_config['db']),
						 echo=(sql_config['echo'].lower == 'true'))