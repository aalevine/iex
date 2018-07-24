"""
Fetch IEX data to populate tables: company, prices, orders
"""

from postgres import cursor, execute_from_text, execute_from_file
from psycopg2.extensions import AsIs
from datetime import datetime
import argparse
import requests
import logging
import json
import csv
import os


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

root_dir = os.path.dirname(os.path.realpath(__file__)) # Whatever the current directory is
root_url = 'https://api.iextrading.com/1.0'

# Read in flat file of stock codes
stock_codes = []
with open('{}/data/stock_codes.txt'.format(root_dir)) as f:
	for code in csv.reader(f):
		stock_codes.append(code[0])


def get_args():
	"""
	Parse command-line arguments
	backfill argument is optional (default is False)
	"""
	parser = argparse.ArgumentParser()

	parser.add_argument(
		'-b', 
		'--backfill',
		type=bool,
		nargs='?',
		const=True, # if no value specified after flag, set to True
		default=False, # if no flag, default is False
		help='Optional: Backfill the data 2 years? True/False. This will drop and re-create all IEX tables.'
	)
	args = parser.parse_args()

	return args.backfill


def fetch_data(endpoint, backfill):
	"""
	Make the IEX API call and return the response as JSON

	:param endpoint: the endpoint to call (e.g. 'company', 'chart')
	:param backfill: perform backfill? (boolean)
	"""

	try:
		# If backfill arg is True, set data range to 2y, otherwise, default (1m)
		range = '1m'
		if backfill:
			range = '2y'

		# Use batch endpoint to return data for all stock codes
		call = '{root_url}/stock/market/batch?symbols={symbols}&types={endpoint}&range={range}'.format(
			root_url=root_url, 
			symbols=','.join(stock_codes),
			endpoint=endpoint,
			range=range
		)

		response = requests.get(call)

		if response.status_code != 200:
			logging.error('Non-200 status code: {}'.format(response.status_code))

		# Load response as JSON
		json_data = json.loads(response.text)
		json_keys = json_data.keys()

		# Make sure returned JSON is not empty
		if len(json_keys) == 0:
			logging.error('Returned empty json')

	except Exception, e:
		logging.critical('IEX HTTP call failed: {}'.format(e))

	else:
		return json_data, json_keys


def parse_company_data(backfill):
	"""
	Returns parsed JSON data for company table

	:param backfill: perform backfill? (boolean)
	"""

	# Make the API call, using 'company' endpoint
	json_data, json_keys = fetch_data(endpoint='company', backfill=backfill)

	company_table = []

	# Parse out only the desired fields from the company endpoint
	for i in json_keys:
		company_table.append({
			'stock_code': i,
			'company_name': json_data[i]['company'].get('companyName'),
			'exchange': json_data[i]['company'].get('exchange'),
			'sector': json_data[i]['company'].get('sector'),
			'industry': json_data[i]['company'].get('industry')
		})

	return company_table


def parse_prices_data(backfill):
	"""
	Returns parsed JSON data for prices table

	:param backfill: perform backfill? (boolean)
	"""

	# Make the API call, using 'chart' endpoint
	json_data, json_keys = fetch_data(endpoint='chart', backfill=backfill)

	prices_table = []

	# Parse out only the desired fields from the chart endpoint
	for i in json_keys:
		for row in json_data[i]['chart']:
			prices_table.append({
				'stock_code': i,
				'date': row.get('date'),
				'close': row.get('close')
			})

	return prices_table	


def json_to_postgres(data, table):
	"""
	Write parsed JSON data to unlogged Postgres table 
	Entire JSON blob will be copied to one record in the table
	This format allows for subsequent parsing into final SQL table format (using json_populate_recordset)

	:param data: parsed JSON data from API call (e.g. for company, prices tables)
	:param table: table name (string)
	"""

	# Write parsed JSON data to a local file
	with open('{}/data/{}.json'.format(root_dir, table), 'w') as f:
		json.dump(data, f)

	# Create an unlogged Postgres table to copy the local JSON file to. 
	# Table has one column: doc
	logging.info('Creating unlogged table to store JSON {} data...'.format(table))
	staging_json_table = AsIs('staging.'+table+'_json')

	execute_from_text('''

		DROP TABLE IF EXISTS {unlogged};

		CREATE UNLOGGED TABLE {unlogged} (doc JSON);

		'''.format(unlogged=staging_json_table))

	# Copy from local JSON file to unlogged table
	with open('{}/data/{}.json'.format(root_dir, table), 'r') as f:  
		_, cur = cursor()

		logging.info('Copying JSON {} data into table...'.format(table))
		cur.copy_expert('COPY {} FROM STDIN'.format(staging_json_table), f)

	# Make sure new unlogged table has data
	query_check_length = 'SELECT LENGTH(doc::VARCHAR) FROM {}'.format(staging_json_table)
	json_char_length = execute_from_text(query_check_length, aggregate_output=True)

	# Log success if the doc field is populated
	if json_char_length != 0:
		logging.info('JSON data successfully copied to {}. Char length: {}'.format(staging_json_table, json_char_length))
	else:
		logging.error('No JSON data copied to {}. Char length: {}'.format(staging_json_table, json_char_length))		


def data_check_row_count(table):
	"""
	Make sure each staging and production table has data

	:param table: table name (string)
	"""

	for schema in ['staging', 'public']:
		query_row_count = 'SELECT COUNT(*) FROM {}.{}'.format(schema, table)
		row_count = execute_from_text(query_row_count, aggregate_output=True)

		if row_count != 0:
			logging.info('Row count for {}.{}: {}'.format(schema, table, row_count))
		else:
			logging.info('Table {}.{} is empty'.format(schema, table))	


def load_table(table, backfill, new_data_field, from_json_data=None):
	"""
	Loads each production table

	If backfilling, drops and recreates the table
	If not backfilling, logs the new values to be inserted
	
	:param table: table name (string)
	:param backfill: perform backfill? (boolean)
	:param new_data_filed: field to check for newly inserted records (e.g. 'date' for prices table, 
		'stock_code' for company table)
	:param from_json_data: parsed JSON data to load the table with (e.g. company, prices tables). 
		The orders table is derived FROM the prices table, so it requires no JSON data processing
	"""

	# If backfilling, run the DDL script (to drop and recreate prod table)
	if backfill:
		execute_from_file(root_dir+'/sql/'+table+'_ddl.sql')
	
	# If the table requires loading from JSON data ('company', 'prices'), load it
	# 'orders' table is derived from existing tables, so it does not require loading from JSON
	if from_json_data != None:
		# Write parsed JSON data to unlogged Postgres table 
		json_to_postgres(from_json_data, table)

	# Create staging table with data from latest request and insert into prod table
	execute_from_file(root_dir+'/sql/'+table+'_insert.sql')

	# Make sure staging and prod tables actually have data
	# Note: staging table may not have data if there is nothing new to insert
	data_check_row_count(table)

	# If not backfilling, output the new values to be inserted from the current execution
	if not backfill:
		new_data = execute_from_text('SELECT DISTINCT {} FROM staging.{}'.format(new_data_field, table), full_output=True)

		# Format output
		new_data_clean = [value[0] for value in new_data]
		if 'date' in new_data_field:
			new_data_clean = [value.strftime('%Y-%m-%d') for value in new_data_clean]

		if len(new_data) == 0:
			logging.info('No new data to insert into public.{}'.format(table))
		else:
			# Log the new stock codes or dates to be appended 
			logging.info('New {} values inserted into public.{}: {}'.format(new_data_field, table, sorted(new_data_clean)))


def run():
	# Perform backfill if optional arg is set to True (default is False)
	backfill = get_args()
	logging.info('Performing backfill? {}'.format(backfill))

	logging.info('Loading company table...')
	load_table( 
		'company', 
		backfill,
		new_data_field='stock_code',
		from_json_data=parse_company_data(backfill)
	)

	logging.info('Loading prices table...')
	load_table(
		'prices', 
		backfill,
		new_data_field='date',
		from_json_data=parse_prices_data(backfill)
	)

	logging.info('Loading orders table...')
	load_table(
		'orders',
		backfill,
		new_data_field='date'
	)

	logging.info('Dropping staging tables...')
	execute_from_file(root_dir+'/sql/cleanup.sql')


if __name__ == "__main__":

	run()
