#!/opt/mesosphere/bin/python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 24 13:53

@author: Arquillos

bd-api.py - Reading and Writing in PostgreSQL
"""
import sys
import argparse

import psycopg2


# VARs - Datos de conexión a la B.D. PostgreSQL
_DB_NAME = None
_TENANT = 'priv02.daas.gl.igrupobbva'
_HOST = None
_PORT = 5432
_USER = 'operaciones'
_SSL_MODE = 'verify-full'
_SSL_ROOT_CERT = './certificates/datio.crt'
_SSL_CERT = './certificates/user-operaciones.crt'
_SSL_KEY = './certificates/user-operaciones.key'
# VARs - Conexión con la B.D. y cursor para el acceso a los datos
_conn = None
_cur = None
# Unit tests
_DELETE_RESULTS = True
_DB_VERSION = "9.6.8"  # Cassiopeia DB
# _DB_VERSION = "9.6.10" # Andromeda DB


def _check_parameter():
    global _DB_NAME, _HOST, _DELETE_RESULTS
    """ Check input parameters """
    if (len(sys.argv) < 2) or (len(sys.argv) > 4):
        print('\n\n   --- ERROR --- Wrong number of arguments.')
        print('                --- Use: bd_api.py <DB Name> [--nodelete True]\n\n\n')
        sys.exit(0)

    parser = argparse.ArgumentParser()
    parser.add_argument("db_name", help="Name of the DB to be tested.")
    parser.add_argument("--nodelete", help="Dont delete the created table.")
    args = parser.parse_args()
    _DB_NAME = args.db_name
    print('   --- BD Host: {}'.format(_HOST))
    if args.nodelete:
        print('   --- The created table wont be deleted after script execution.')
        _DELETE_RESULTS = False


def _db_connect(db_name):
    """ Connect to the PostgreSQL database server """
    global _conn, _USER, _HOST, _PORT, _SSL_MODE, _SSL_ROOT_CERT, _SSL_CERT, _SSL_KEY

    try:
        print('--- Connecting to the PostgreSQL database ({}).'.format(db_name))
        _HOST = 'master.' + db_name + '.l4lb.' + _TENANT
        psql_credentials = {
            'dbname': db_name,
            'user': _USER,
            'host': _HOST,
            'port': _PORT,
            'sslmode': _SSL_MODE,
            'sslrootcert': _SSL_ROOT_CERT,
            'sslcert': _SSL_CERT,
            'sslkey': _SSL_KEY
        }
        print(psql_credentials)
        _conn = psycopg2.connect(**psql_credentials)
    except psycopg2.Error as e:
        print('   --- ERROR - Cant connect to DB!!!!\n\n\n')
        print(e.pgerror)
        sys.exit(0)


def _db_execute(query):
    """ Execute query in the database server """
    global _conn, _cur

    # create a cursor
    _cur = _conn.cursor()

    try:
        _cur.execute(query)
        return True
    except psycopg2.Error as e:
        print('   --- ERROR - Cant execute sentence: {}\n\n\n'.format(query))
        print(e.pgerror)
        print('--- D.B. disconnection')
        _db_close()
        sys.exit(0)

    return False


def _db_get_data():
    """ Get data from the last executed query in the database server """
    global _cur

    row = _cur.fetchone()
    print("   --- Num. results: {} - Value: {} ".format(_cur.rowcount, row[0]))

    # while row is not None:
    #    print(row[0])
    #    row = _cur.fetchone()
    return row[0]


def _db_close():
    """ Close the connection to the PostgreSQL database server """
    global _conn, _cur

    try:
        _conn.commit()
        _cur.close()
        print('--- Database connection closed.')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def check_database(db_name, delete_results):
    _RESULT_COUNT = 100000

    print('--- D.B. name: {}'.format(db_name))
    _db_connect(db_name)

    # DB table creation
    query = 'CREATE TABLE tabletmp(t INT)'
    print('--- Creating tabletmp table: {}'.format(query))
    _db_execute(query)
    print('   --- tabletmp created!')

    query = 'INSERT INTO tabletmp VALUES(generate_series(1, 100000))'
    print('--- Inserting data in table tabletmp: {}'.format(query))
    _db_execute(query)
    print('   --- tabletmp populated!')

    query = 'SELECT count(*) FROM tabletmp'
    print('--- Table tabletmp number of rows: {}'.format(query))
    _db_execute(query)
    if _RESULT_COUNT == _db_get_data():
        print('   --- {} rows created!'.format(_RESULT_COUNT))
    else:
        print('--- Error counting table rows! - disconnection')
        _db_close()
        return False

    if delete_results:
        query = 'DROP TABLE tabletmp CASCADE'
        _db_execute(query)
        print('   --- table tabletmp deleted')
    print('\n')

    print('--- D.B. CRUD check OK! - disconnection')
    _db_close()

    return True


def check_database_config(db_name, db_version):
    _MAX_CONNECTIONS = "100"
    _TCP_KEEPALIVES_IDLE = "60"
    _TCP_KEEPALIVES_INTERVAL = "10"
    _TCP_KEEPALIVES_COUNT = "10"

    print('--- D.B. name: {} - Expected version: {}'.format(db_name, db_version))
    _db_connect(db_name)

    query = 'SELECT version()'
    print('--- DB. Version query: {}'.format(query))
    if _db_execute(query):
        actual_db_version = _db_get_data()
        if db_version in actual_db_version:
            print('   --- DB version Ok! ({})'.format(db_version))
        else:
            print('   --- DB version NOK! - D.B. disconnection')
            _db_close()
            return False

    query = 'SHOW max_connections'
    print('--- DB. configuration - max_connections query: {}'.format(query))
    if _db_execute(query):
        max_connections = _db_get_data()
        if max_connections == _MAX_CONNECTIONS:
            print('   --- DB max_connections Ok! ({})'.format(max_connections))
        else:
            print('   --- DB max_connections NOK! - D.B. disconnection')
            _db_close()
            return False

    query = 'SHOW tcp_keepalives_idle'
    print('--- DB. configuration - tcp_keepalives_idle query: {}'.format(query))
    if _db_execute(query):
        tcp_keepalives_idle = _db_get_data()
        if tcp_keepalives_idle == _TCP_KEEPALIVES_IDLE:
            print('   --- DB tcp_keepalives_idle Ok! ({})'.format(tcp_keepalives_idle))
        else:
            print('   --- DB tcp_keepalives_idle NOK! - D.B. disconnection')
            _db_close()
            return False

    query = 'SHOW tcp_keepalives_interval'
    print('--- DB. configuration - tcp_keepalives_interval query: {}'.format(query))
    if _db_execute(query):
        tcp_keepalives_interval = _db_get_data()
        if tcp_keepalives_interval == _TCP_KEEPALIVES_INTERVAL:
            print('   --- DB tcp_keepalives_interval Ok! ({})'.format(tcp_keepalives_interval))
        else:
            print('   --- DB tcp_keepalives_interval NOK! - D.B. disconnection')
            _db_close()
            return False

    query = 'SHOW tcp_keepalives_count'
    print('--- DB. configuration - tcp_keepalives_count query: {}'.format(query))
    if _db_execute(query):
        tcp_keepalives_count = _db_get_data()
        if tcp_keepalives_count == _TCP_KEEPALIVES_COUNT:
            print('   --- DB tcp_keepalives_count Ok! ({})'.format(tcp_keepalives_count))
        else:
            print('   --- DB tcp_keepalives_count NOK! - D.B. disconnection')
            _db_close()
            return False

    print('--- D.B. CRUD Check finished...disconnection')
    _db_close()

    return True


if __name__ == '__main__':
    print('\n--- Checking DB access')
    _check_parameter()

    if _DB_NAME:
        # Check B.D.
        if check_database(_DB_NAME, True):
            print('--- DB CRUD checks Ok!')
        else:
            print('--- DB CRUD checks NOK!')

        # Check B.D. Parameters
        if check_database_config(_DB_NAME, _DB_VERSION):
            print('--- DB Config checks Ok!')
        else:
            print('--- DB Config checks NOK!')
