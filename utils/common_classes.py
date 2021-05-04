import psycopg2


class DBParams:
    def __init__(self, user, pwd, host, port, database_name, sql_query):
        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.database_name = database_name
        self.sql_query = sql_query

    def get_db_connection_and_cursor(user, pwd, host, port, database_name):
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