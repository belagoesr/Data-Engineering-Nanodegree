import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Run SQL statements to drop already existent tables
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Run SQL statements to create staging and star schema tables
    """
    for query in create_table_queries:
        print("executing query: {}".format(query))
        cur.execute(query)
        conn.commit()


def main():
    """
    Load configuration file, connect to Redshift cluster and drop/create tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        config['CLUSTER']['HOST'],
        config['CLUSTER']['DB_NAME'],
        config['CLUSTER']['DB_USER'],
        config['CLUSTER']['DB_PASSWORD'],
        config['CLUSTER']['DB_PORT'],
    ))
    cur = conn.cursor()

    drop_tables(cur, conn)
    print("Dropped tables...")
    create_tables(cur, conn)
    print("Created tables...")

    conn.close()


if __name__ == "__main__":
    main()