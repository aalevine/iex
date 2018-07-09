'''
Helper functions for executing Postgres queries
'''

import psycopg2
import logging
import os 

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def connect_psql():

	try:
		conn = psycopg2.connect(
			dbname=os.environ['pg_database'],
			user=os.environ['pg_user'],
			password=os.environ['pg_password'],
			port=5432
		)
		conn.autocommit=True

	except Exception, e:
		logging.critical('Postgres connection failed: {}'.format(str(e)))

	else:
		return conn


def cursor():
	conn = connect_psql()
	cur = conn.cursor()
	return conn, cur 


def execute_from_text(query, single_output=False, full_output=False):
	conn, cur = cursor()
	try:
		# Executes without fetching results
		cur.execute(query)

	except Exception, e:
		logging.critical('Query failed: {}'.format(str(e)))

	else:
		# If the query returns an output, log it and return it
		if single_output:
			return cur.fetchone()[0]

		if full_output:
			return cur.fetchall()

	finally:
		conn.close()


def execute_from_file(filename, single_output=False, full_output=False):
    # Open and read the file as a single buffer
    f = open(filename, 'r')
    sql_file = f.read()
    f.close()

    # All SQL commands (split on ';')
    # Remove final item in the list -- it's empty (since last SQL command ends with ';')
    sql_commands = sql_file.split(';')[:-1]

    # Execute every command from the input file
    for command in sql_commands:
        try:
            execute_from_text(command, single_output, full_output)
        except Exception, e:
            logging.info('Command skipped: {}'.format(str(e)))