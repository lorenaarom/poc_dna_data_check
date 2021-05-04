import csv
import os
import sys

import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def get_schema_info(schema_name):
    csv_file_path = f'{schema_name}_tables.csv'
    data_list = []
    data_dict = {}

    with open(csv_file_path) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            schema = row['schema_name']
            data_dict[schema] = row['table_name']
            data_list.append(data_dict[schema])
    return data_list


def get_sql_query(schema, table):
    with open(f'../{schema}_queries/{table}', 'r') as sql_file:
        sql_query = sql_file.read()
    return sql_query


def get_db_connection_and_cursor(database_name, host, port, user, pwd):
    """This Function gets a connection to RDS or Redshift
        Params:
            user, pwd, host, port, database_name for the connection
        Returns:
            connection, cursor

    """
    try:
        connection = psycopg2.connect(dbname=database_name, host=host, port=port, user=user, password=pwd)
        cursor = connection.cursor()
        return connection, cursor
    except Exception:
        raise


def execute_query(cursor, sql_query):
    try:
        cursor.execute(sql_query)
    except psycopg2.Error as error:
        message = f'Error executing statement in the DB: {error}'
        print(message)
        raise Exception(message)


def close_cursor_and_connection(connection, cursor):
    if connection:
        connection.close()
        print('Connection to database closed')
    if cursor:
        cursor.close()
        print('Connection to database closed')


def get_row_count(cursor):
    data_list = cursor.fetchall()
    row_count = data_list[0][0]
    return row_count
