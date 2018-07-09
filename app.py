'''
Fetch IEX data to populate tables: company, prices
Takes optional argument --backfill
	If true, drops, recreates, and backfills tables for the past 2 years of data
'''

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

root_dir = os.path.dirname(os.path.realpath(__file__))
root_url = 'https://api.iextrading.com/1.0'

stock_codes = []
with open('{}/data/stock_codes.txt'.format(root_dir)) as f:
    for code in csv.reader(f):
        stock_codes.append(code[0])


def get_args():
	# Command-line arguments to parse
    parser = argparse.ArgumentParser()

	# Backfill arg is optional
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
	try:
		# If Backfill arg is True, set range to 2y, otherwise, default (1m)
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

		# Load response as JSON blob
		json_data = json.loads(response.text)
		json_keys = json_data.keys()

		if response.status_code != 200:
			logging.error('Non-200 status code: {}'.format(response.status_code))

		# Make sure returned JSON is not empty
		if len(json_keys) == 0:
			logging.error('Returned empty json')

	except Exception, e:
		logging.critical('IEX HTTP call failed: {}'.format(e))

	else:
		return json_data, json_keys


def parse_company_data(backfill):
	# Hit endpoint to populate company table
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
	# Hit endpoint to populate prices table
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


def data_check_json(table):
	'''
	In the below function (json_to_postgres), we save the parsed JSON response as an unlogged Postgres table
	This function makes sure this "staging" JSON table has data, i.e. the copy_expert function worked correctly
	Note: the entire JSON blob should be stored in the field 'doc'
	'''

	query_check_length = 'SELECT LENGTH(doc::VARCHAR) FROM {}'.format(table)
	json_char_length = execute_from_text(query_check_length, single_output=True)

	if json_char_length != 0:
		logging.info('JSON data successfully copied to {}. Char length: {}'.format(table, json_char_length))
	else:
		logging.error('No JSON data copied to {}. Char length: {}'.format(table, json_char_length))	


def json_to_postgres(data, table):

	# Write parsed JSON data to file
	with open('{}/data/{}.json'.format(root_dir, table), 'w') as f:
	    json.dump(data, f)

	logging.info('Creating unlogged table to store JSON {} data...'.format(table))
	staging_json_table = AsIs('staging.'+table+'_json')

	# Create an unlogged Postgres table to copy the JSON data to
	# Entire JSON blob will be copied to one field: doc
	execute_from_text('''

		DROP TABLE IF EXISTS {unlogged};

		CREATE UNLOGGED TABLE {unlogged} (doc JSON);

		'''.format(unlogged=staging_json_table))

	# Copy from JSON file to unlogged table
	with open('{}/data/{}.json'.format(root_dir, table), 'r') as f:  
	    _, cur = cursor()
	    logging.info('Copying JSON {} data into table...'.format(table))
	    cur.copy_expert('COPY {} FROM STDIN'.format(staging_json_table), f)

	# Make sure JSON table has data
	data_check_json(staging_json_table)


def data_check_row_count(table):
	# Make sure each staging and production table has data
	for schema in ['staging', 'public']:
		query_row_count = 'SELECT COUNT(*) FROM {}.{}'.format(schema, table)
		row_count = execute_from_text(query_row_count, single_output=True)

		if row_count != 0:
			logging.info('Row count for {}.{}: {}'.format(schema, table, row_count))
		else:
			logging.info('Table {}.{} is empty'.format(schema, table))	


def load_table(table, backfill, new_data_field, json_data=None):
	# If backfilling, run the DDL script (to drop and recreate prod table)
	if backfill:
		execute_from_file(root_dir+'/sql/'+table+'_ddl.sql')
	
	if json_data != None:
		# Copy JSON data to Postgres
		json_to_postgres(json_data, table)

	# Create staging table with data from latest request and insert into prod table
	execute_from_file(root_dir+'/sql/'+table+'_insert.sql')

	# Make sure staging and prod tables actually have data
	# Staging table may not have data if there is nothing new to insert
	data_check_row_count(table)

	if not backfill:
		# Output the new values to be inserted from the current execution
		new_data = execute_from_text('SELECT DISTINCT {} FROM staging.{}'.format(new_data_field, table), full_output=True)

		# Format output
		new_data_clean = [value[0] for value in new_data]
		if 'date' in new_data_field:
			new_data_clean = [value.strftime('%Y-%m-%d') for value in new_data_clean]

		if len(new_data) == 0:
			logging.info('No new data to insert into public.{}'.format(table))
		else:
			logging.info('New {} values inserted into public.{}: {}'.format(new_data_field, table, sorted(new_data_clean)))


def run():
	# Perform backfill if optional arg is set to True (default is False)
	backfill = get_args()
	logging.info('Performing backfill? {}'.format(backfill))

	logging.info('Loading compnay table...')
	load_table( 
		'company', 
		backfill,
		'stock_code',
		json_data=parse_company_data(backfill)
	)

	logging.info('Loading prices table...')
	load_table(
		'prices', 
		backfill,
		'date',
		json_data=parse_prices_data(backfill)
	)

	logging.info('Loading orders table...')
	load_table(
		'orders',
		backfill,
		'date'
	)

	logging.info('Dropping staging tables...')
	execute_from_file(root_dir+'/sql/cleanup.sql')


if __name__ == "__main__":

	run()