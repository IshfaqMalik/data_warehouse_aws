import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 to staging tables in Redshift.

    Parameters:
    cur (psycopg2.cursor): Cursor object to execute PostgreSQL commands in a Redshift session.
    conn (psycopg2.connection): Connection object to manage the connection to Redshift.

    Returns:
    None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from staging tables into final fact and dimension tables in Redshift.

    Parameters:
    cur (psycopg2.cursor): Cursor object to execute PostgreSQL commands in a Redshift session.
    conn (psycopg2.connection): Connection object to manage the connection to Redshift.

    Returns:
    None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Establishes a connection to Redshift, loads data into staging tables, and
    then populates the fact and dimension tables.

    Returns:
    None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()