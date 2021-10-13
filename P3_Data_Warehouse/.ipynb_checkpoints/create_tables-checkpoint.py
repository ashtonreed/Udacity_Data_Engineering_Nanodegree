import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    "Drop the tables if they already exist so that you have a fresh start without potential errors"
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    "Create the staging tables and fact and dimension tables so that they are ready for data ingestion"
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    "Run the above functions to drop and create tables"
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()