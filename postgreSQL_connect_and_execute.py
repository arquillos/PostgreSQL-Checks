#!/opt/mesosphere/bin/python3
import psycopg2
from configparser import ConfigParser


def config(filename='/home/cloud-user/Arq/PostgreSQL/database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()

    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def connect_and_execute(query):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        # print('PostgreSQL database version:')
        # cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        # db_version = cur.fetchone()
        # print(db_version)

        cur.execute(query)

        row = cur.fetchone()

        while row is not None:
            print(row)
            row = cur.fetchone()


     # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    for one_query in ["SELECT count(*), usename FROM pg_stat_activity GROUP BY usename", "SELECT version()", "SELECT datname FROM pg_database WHERE datistemplate = false"]:
_        query = one_query
        print('Query a ejecutar: {}'.format(query))
        connect_and_execute(query)
        print('\n\n')
    print("Getting D.B. config")
    print("Consulta: show max_connections;")
    query = "show max_connections;"
    connect_and_execute(query)

    print("Consulta: show tcp_keepalives_idle;")
    query = "show tcp_keepalives_idle;"
    connect_and_execute(query)

