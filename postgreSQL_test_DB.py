#!/opt/mesosphere/bin/python3
import sys
import argparse
import json

from pathlib import Path

import requests
from apis.marathon_api import find_marathon_app, framework_check_minuteman_vip, framework_check_framework_state
from apis.minuteman_api import check_minuteman_vip
from apis.bd_api import check_database, check_database_config


_TENANT = 'priv02.daas.gl.igrupobbva'
_DB_NAME = None
_DELETE_RESULTS = True
_DB_VERSION = "9.6.8"  # Cassiopeia DB
# _DB_VERSION = "9.6.10" # Andromeda DB

# VARs - Framework Endpoints
FRAMEWORK_STATUS_ENDPOINT_URL = ".marathon.l4lb." + _TENANT + "/v1/service/status"
framework_status_result_json = None


def check_certificates():
    datio_crt = Path("./certificates/datio.crt")
    if not datio_crt.is_file():
        print('\n\n   --- ERROR --- Certificate datio.crt doesnt exists.\n\n\n')
        sys.exit(0)

    operaciones_crt = Path("./certificates/user-operaciones.crt")
    if not operaciones_crt.is_file():
        print('\n\n   --- ERROR --- Certificate operaciones.crt doesnt exists.\n\n\n')
        sys.exit(0)

    operaciones_key = Path("./certificates/user-operaciones.key")
    if not operaciones_key.is_file():
        print('\n\n   --- ERROR --- Certificate operaciones.key doesnt exists.\n\n\n')
        sys.exit(0)


def check_parameters():
    """ Check input parameters """
    global _DB_NAME

    if len(sys.argv) != 2:
        print('\n\nERROR --- Wrong number of arguments.')
        print('        - Use: postgreSQL_test_DB.py <App Name>')
        sys.exit(0)

    parser = argparse.ArgumentParser()
    parser.add_argument("app_name", help="Name of the App deployed in Marathon.")
    args = parser.parse_args()
    _DB_NAME = args.app_name


# Depends on running check_parameter() FIRST -> DB_NAME
def framework_get_status_endpoint():
    """ Call framework API status endpoint """
    global FRAMEWORK_STATUS_ENDPOINT_URL, framework_status_result_json, _DB_NAME

    FRAMEWORK_STATUS_ENDPOINT_URL = "http://" + _DB_NAME + FRAMEWORK_STATUS_ENDPOINT_URL
    print('   --- Framework status endpoint: {}'.format(FRAMEWORK_STATUS_ENDPOINT_URL))

    try:
        framework_status_result = requests.get(FRAMEWORK_STATUS_ENDPOINT_URL)
        framework_status_result_json = json.loads(framework_status_result.text)
        # print(json.dumps(framework_status_result_json, indent=3, sort_keys=True))
    except requests.exceptions.ConnectionError as error:
        print('   --- ERROR - Check Minuteman IP. It doesnt seems to be working: {}\n\n\n'.format(FRAMEWORK_STATUS_ENDPOINT_URL))
        print(error)


def framework_check_status_endpoint_three_nodes():
    running_nodes = 0
    discarded_nodes = 0
    other_nodes = 0
    nodes = framework_status_result_json['status']

    print('   --- DB {} has {} nodes.'.format(_DB_NAME, len(nodes)))
    for node in nodes:
        node_status = node.get('status')
        if node_status == "RUNNING":
            running_nodes += 1
        elif node_status == "DISCARDED":
            discarded_nodes += 1
        else:
            other_nodes += 1
    print('   --- Running nodes: {}, Discarded nodes: {}, other nodes: {}'.format(running_nodes, discarded_nodes, other_nodes))


def framework_check_status_endpoint_three_nodes_status():
    error = False
    for node in framework_status_result_json['status']:
        node_id = node.get('dnsHostname')
        node_status = node.get('status')
        if (node_status != "RUNNING") and (node_status != "DISCARDED"):
            print('   --- ERROR - Node {} has status: {}'.format(node_id, node_status))
            error = True
    if not error:
        print('   --- All nodes are RUNNING!')


def framework_check_status_endpoint_three_roles():
    master, sync, async = False, False, False
    for node in framework_status_result_json['status']:
        node_id = node.get('dnsHostname')
        node_role = node.get('role')
        node_status = node.get('status')
        # print('   --- Node {} - Role: {}'.format(node_id, node_role))

        if node_status == "RUNNING":
            if node_role == "master":
                master = True
                print('   --- Master node: {}'.format(node_id))
            if node_role == "sync_slave":
                sync = True
                print('   --- Sync node: {}'.format(node_id))
            if node_role == "async_slave":
                async = True
                print('   --- Async node: {}'.format(node_id))
        else:
            print('   --- Node: {} - Role: {} - Status: {}'.format(node_id, node_role, node_status))

    if not (master and sync and async):
        print('   --- ERROR - DB node roles error!.')


if __name__ == '__main__':
    # Check input parameters
    print('\n--- Checking input parameters.')
    check_parameters()

    # Check certificates
    print('\n--- Checking needed certificates.')
    check_certificates()

    print('\n')
    print('--- Framework status Endpoint')
    framework_get_status_endpoint()
    framework_check_status_endpoint_three_nodes()
    framework_check_status_endpoint_three_nodes_status()
    framework_check_status_endpoint_three_roles()

    # Check B.D.
    print('\n')
    print('--- D.B. connection')
    if check_database(_DB_NAME, _DELETE_RESULTS):
        print('--- DB CRUD checks Ok!')
    else:
        print('--- DB CRUD checks NOK!')

    # Check B.D. Parameters
    if check_database_config(_DB_NAME, _DB_VERSION):
        print('--- DB Config checks Ok!')
    else:
        print('--- DB Config checks NOK!')

    # Marathon
    descriptor_json = find_marathon_app(_DB_NAME)
    if framework_check_framework_state(descriptor_json):
        print('--- Framework state Ok!')
    else:
        print('--- Framework state NOK!')

    if not framework_check_minuteman_vip(descriptor_json, _DB_NAME):
        print('--- VIP Ok!')
    else:
        print('--- VIP NOK!')
    print('\n\n\n')

    # Minuteman
    if check_minuteman_vip(_DB_NAME):
        print('--- Minuteman VIP Ok!')
    else:
        print('--- Minuteman VIP NOK!')
