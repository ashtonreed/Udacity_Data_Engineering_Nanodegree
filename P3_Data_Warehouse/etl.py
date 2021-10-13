import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    "Loading the data from S3 into staging tables"
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    "Loading the data from the staging tables into the fact and dimension tables"
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    "Run the above functions to load data into staging tables then transform it into fact and dimension tables"
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()