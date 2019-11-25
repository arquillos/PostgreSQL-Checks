#!/opt/mesosphere/bin/python3
import sys
import argparse
from pathlib import Path

import psycopg2


# VARs - Datos de conexión a la B.D. PostgreSQL
DBNAME=None
TENANT='priv02.daas.gl.igrupobbva'
HOST=None
PORT=5432
USER='operaciones'
SSLMODE='verify-full'
SSLROOTCERT='./datio.crt'
SSLCERT='./user-operaciones.crt'
SSLKEY='./user-operaciones.key'
# VARs - Conexión con la B.D. y cursor para el acceso a los datos
conn = None
cur = None
DELETE_RESULTS=True


def check_certificates():
    datio_crt = Path("./datio.crt")
    if not datio_crt.is_file():
        print('\n\n   --- ERROR --- Certificate datio.crt doesnt exists.\n\n\n')
        sys.exit(0)

    operaciones_crt = Path("./user-operaciones.crt")
    if not operaciones_crt.is_file():
        print('\n\n   --- ERROR --- Certificate operaciones.crt doesnt exists.\n\n\n')
        sys.exit(0)

    operaciones_key = Path("./user-operaciones.key")
    if not operaciones_key.is_file():
        print('\n\n   --- ERROR --- Certificate operaciones.key doesnt exists.\n\n\n')
        sys.exit(0)


def check_parameter():
    global DBNAME, HOST, DELETE_RESULTS
    """ Check input parameters """
    if ((len(sys.argv) < 2) or (len(sys.argv) > 4)):
        print('\n\n   --- ERROR --- Wrong number of arguments.')
        print('                --- Use: postgreSQL_test_DB.py <DB Name> [--nodelete True]\n\n\n')
        sys.exit(0)

    parser = argparse.ArgumentParser()
    parser.add_argument("db_name", help="Name of the DB to be tested.")
    parser.add_argument("--nodelete", help="Dont delete the created table.")
    args = parser.parse_args()
    DBNAME = args.db_name
    HOST='master.' + DBNAME + '.l4lb.' + TENANT
    print('   --- BD Host: {}'.format(HOST))
    if args.nodelete:
        print('   --- The created table wont be deleted after script execution.')
        DELETE_RESULTS = False


def db_connect():
    """ Connect to the PostgreSQL database server """
    global conn

    try:
        print('--- Connecting to the PostgreSQL database. ({})'.format(HOST))
        psql_creds = {
            'dbname': DBNAME,
            'user': USER,
            'host': HOST,
            'port': PORT,
            'sslmode': SSLMODE,
            'sslrootcert': SSLROOTCERT,
            'sslcert': SSLCERT,
            'sslkey': SSLKEY
        }
        conn = psycopg2.connect(**psql_creds)
    except:
        print('   --- ERROR - Cant connect to DB!!!!\n\n\n')
        sys.exit(0)


def db_execute(query):
    """ Execute query in the database server """
    global conn, cur

    # create a cursor
    cur = conn.cursor()

    try:
        cur.execute(query)
    except:
        print('   --- ERROR - Cant execute sentence: {}\n\n\n'.format(query))
        sys.exit(0)



def db_get_data():
    """ Get data from the last executed query in the database server """
    global cur

    print("The number of parts: ", cur.rowcount)
    row = cur.fetchone()

    while row is not None:
        print(row)
        row = cur.fetchone()


def db_close():
    """ Close the connection to the PostgreSQL database server """
    global conn, cur

    try:
        conn.commit()
        cur.close()
        print('--- Database connection closed.')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == '__main__':
    # Check input parameters
    print('\n--- Checking input parameters.')
    check_parameter()

    # Check certificates
    print('\n--- Checking needed certificates.')
    check_certificates()

    print('\n')
    print('--- D.B. connection')
    db_connect()

    query = 'CREATE TABLE tabletmp(t INT)'
    print('--- Creating tabletmp table: {}'.format(query))
    db_execute(query)
    print('\n')

    query = 'INSERT INTO tabletmp VALUES(generate_series(1, 100000))'
    print('--- Inserting data in table tabletmp: {}'.format(query))
    db_execute(query)
    print('\n')

    query = 'SELECT count(*) FROM tabletmp'
    print('--- Table tabletmp number of rows: {}'.format(query))
    db_execute(query)
    db_get_data()
    print('\n')

    if DELETE_RESULTS:
        query = 'DROP TABLE tabletmp CASCADE'
        print('--- Deleting table tabletmp: {}'.format(query))
        db_execute(query)
        print('\n')
    else:
        print('--- The tabletmp is NOT DELETED!')

    print('--- D.B. disconnection')
    db_close()
    print('\n\n')
