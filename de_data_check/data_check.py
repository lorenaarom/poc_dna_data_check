import great_expectations as ge
from utils import common_functions as cf
from utils import common_classes as cc
from utils import ge_workflow as wf
import logging
import concurrent.futures
import time
import threading
import os, sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def transactional_vs_dw_check(schema, table):
    transactional_table_count = get_table_count_rds(schema, table, rds_username, rds_password, rds_host, rds_port, rds_db_name)
    dw_table_count = get_table_count_dw(schema, table, dw_user, dw_pwd, dw_host, dw_port, dw_dbname)
    if dw_table_count == transactional_table_count:
        print(f'Number of columns match \nRDS count is {transactional_table_count} and DW count is {dw_table_count}')
    else:
        print(f'Count does not match \nRDS count is {transactional_table_count} and DW count is {dw_table_count}')


def rds_vs_dw_check(schema, tables):
    threads = []
    for table in tables:
        print(f'This is the table: {table}')
        t = threading.Thread(target=transactional_vs_dw_check(schema, table))
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()


def rds_vs_dw_check_concurrent(schema, tables):
    args = ((schema, table) for table in tables)
    print(type(args))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        print(args)
        rs = executor.map(lambda p: execute_ge_workflow(*p), args)
        print(rs)


def get_table_count_rds(schema, table, username, password, host, port, dbname):
    conn, cur = cf.get_db_connection_and_cursor(dbname, host, port, username, password)
    sql = cf.get_sql_query(schema, table)
    sql_query = f'SELECT COUNT(*) FROM ({sql}) t LIMIT 1'
    cur = conn.cursor()
    cur.execute(sql_query)
    table_count = cf.get_row_count(cur)
    return table_count


def get_table_count_dw(schema, table, username, password, host, port, dbname):
    conn, cur = cf.get_db_connection_and_cursor(dbname, host, port, username, password)
    sql_query = f'SELECT COUNT(*) FROM {schema}.{table} LIMIT 1'
    cur = conn.cursor()
    cur.execute(sql_query)
    table_count = cf.get_row_count(cur)
    return table_count


def transactional_vs_dw_check_all(schema, tables):
    """
        This function checks that all columns in a specific schema in redshift, match with the current metadata used for the dags
        Params:
        schema: name of the schema
        table: name of the table

    """
    threads = []
    for table in tables:
        print(f'This is the table: {table}')
        t = threading.Thread(target=transactional_vs_dw_check(schema, table))
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()


def get_rds_count():
    cc.DBParams()

    connection = cf.get_connection(rds_username, rds_password, rds_host, rds_port, rds_db_name, is_autocommit='is_autocommit')
    sql_query = 'select count(*) from assessments limit 1'
    cursor = connection.cursor()
    cursor.execute(sql_query)
    data_list = cursor.fetchall()
    cursor.close()
    transactional_table_count = data_list[0][0]
    return transactional_table_count


def get_dw_count():

    sql_query = f'SELECT count(*) FROM assessments.assessments'
    conn, cur = cf.get_db_connection_and_cursor(dw_user, dw_pwd, dw_host, dw_port, dw_dbname)
    cf.execute_query(cur, sql_query)
    row_count = cf.get_row_count(cur)
    cf.close_cursor_and_connection()
    return row_count


def execute_ge_workflow(schema, table):
    #schema, table = cf.get_schema_info(30)
    context = ge.data_context.DataContext()
    expectation_suite_name = "digital_platform_data"
    suite = context.get_expectation_suite(expectation_suite_name)
    suite.expectations = []

    #ge_dataset = wf.GEDataSet(schema, table)
    ge_dataset = wf.RedshiftDataSet(schema, table)
    ge_df = ge_dataset.create_data_set()

    ge_expectations = wf.GEExpectations(ge_df, context, suite)
    ge_batch = ge_expectations.set_expectations(table)

    ge_output = wf.GEOutput(context, ge_batch)
    ge_output.get_output()


def same_columns_check_all_cfutures(tables, schema):
    """
        This function checks that all columns in a specific schema in redshift, match with the current metadata used for the dags
        Params:
        schema: name of the schema
        table: name of the table

    """
    args = ((schema, table) for table in tables)
    print(type(args))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        print(args)
        rs = executor.map(lambda p: execute_ge_workflow(*p), args)
        print(rs)

    # Finished in  seconds account management - 20 tables


def same_columns_check_all(tables, schema):
    """
        This function checks that all columns in a specific schema in redshift, match with the current metadata used for the dags
        Params:
        schema: name of the schema
        table: name of the table

    """
    threads = []
    for table in tables:
        print(f'this is the table: {table}')
        t = threading.Thread(target=execute_ge_workflow(schema, table))
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()

    # Finished in 1199.26 seconds account_management 20 tables
    # Finished in 817.56 seconds assessments 13 tables
    #Finished in 127.39 seconds standards 2 tables


def same_columns_check(schema, table='assessments'):
    """
    This function checks that all columns in a specific table in redshift, match with the current metadata used for the dags

    Params:
    schema: name of the schema
    table: name of the table

    """
    execute_ge_workflow(schema, table)


if __name__ == '__main__':
    """ Getting environmental variables for:
        - Rds Connection
        - Redshift Connection
        - Workflow Execution"""

    dw_user = os.environ['dw_user']
    dw_pwd = os.environ['dw_pwd']
    dw_host = os.environ['dw_host']
    dw_port = os.environ['dw_port']
    dw_dbname = os.environ['dw_dbname']
    rds_host = os.environ['rds_host']
    rds_db_name = os.environ['rds_db_name']
    rds_username = os.environ['rds_username']
    rds_port = os.environ['rds_port']
    rds_password = os.environ['rds_password']
    schema_name = os.environ['schema_name']

    start = time.perf_counter()
    tables = cf.get_schema_info(schema_name)
    same_columns_check_all(tables, schema_name)
    rds_vs_dw_check(schema_name, tables)
    finish = time.perf_counter()
    print(f'Finished in {round(finish-start, 2)} seconds')

