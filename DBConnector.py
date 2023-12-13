from psycopg2 import pool
from config import DATABASE_PARAMS


def get_cursor(func):
    def wrapper(self, *args, **kwargs):
        connection = None
        cursor = None
        res = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            kwargs["cursor"] = cursor
            res = func(self, *args, **kwargs)
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.commit()
                self.return_connection(connection)

        return res
    return wrapper


class DBConnection:
    def __init__(self):
        self.connection_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            **DATABASE_PARAMS
        )

    def get_connection(self):
        return self.connection_pool.getconn()

    def return_connection(self, connection):
        self.connection_pool.putconn(connection)

    def close_all_connections(self):
        self.connection_pool.closeall()

    @get_cursor
    def get_diner_names(self, cursor):
        cursor.execute("SELECT * FROM diner_names;")
        data = cursor.fetchall()
        return data

    @get_cursor
    def add_diner_name(self, new_data, cursor=None):
        insert_query = "INSERT INTO diner_names (name) VALUES (%s);"
        cursor.execute(insert_query, (new_data, ))
