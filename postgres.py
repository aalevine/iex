"""
Helper functions for executing Postgres queries
"""

import psycopg2
import logging
import os 

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def connect_psql():
	"""
	Create a postgres connection
	User must first save the following variables to her bash_profile: 
		pg_database
		pg_user
		pg_password
	"""

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
	"""
	Create a cursor object
	"""

	conn = connect_psql()
	cur = conn.cursor()
	return conn, cur 


def execute_from_text(query, aggregate_output=False, full_output=False):
	"""
	Execute a SQL statement from a string

	:param query: query to execute, as a string
	:param aggregate_output: if True, log the first output value of the execution (i.e. for a table row count)
	:param full_output: if True, log all output rows of the execution
	"""

	conn, cur = cursor()
	try:
		# Execute the query
		cur.execute(query)

	except Exception, e:
		logging.critical('Query failed: {}'.format(str(e)))

	else:
		# If the query output is desired, return it
		if aggregate_output:
			return cur.fetchone()[0]
		elif full_output:
			return cur.fetchall()

	finally:
		conn.close()


def execute_from_file(filename, aggregate_output=False, full_output=False):
	"""
	Execute a SQL statement from a .sql file
	"""

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
			execute_from_text(command, aggregate_output, full_output)
		except Exception, e:
			logging.info('Command skipped: {}'.format(str(e)))