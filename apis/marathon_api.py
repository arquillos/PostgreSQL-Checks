#!/opt/mesosphere/bin/python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 11:44

@author: Arquillos

apis-api.py - Getting information from Marathon
"""
import sys
import argparse
import requests
import json


# VARs - Marathon
# TODO - Obtener User y Pass de Marathon desde Vault (dcs/paswords/apis/rest)
MARATHON_USER = 'marathon'
MARATHON_PASS = 'TbfFZFP6dPHQgsvzHIrI8iMGlNApM0WTvxCH73A8'
MARATHON_APP_URL = "http://master-1:8080/v2/apps/"

# Only for local tests
_DB_NAME = None


def check_parameter():
    global _DB_NAME

    if len(sys.argv) != 2:
        print('\n\nERROR --- Wrong number of arguments.')
        print('        - Use: marathon-api.py <App Name>')
        sys.exit(0)

    parser = argparse.ArgumentParser()
    parser.add_argument("app_name", help="Name of the App deployed in Marathon.")
    args = parser.parse_args()
    _DB_NAME = args.app_name


def find_marathon_app(app_name):
    """
    Gets app descriptor from Marathon v2/apps API
    :param app_name: App name without foldering.
    :return: Json with App descriptor from Marathon or None if app_name doesnt exists
    """
    if app_name:
        print('--- Getting all Marathon descriptors.')
        s = requests.Session()
        s.auth = (MARATHON_USER, MARATHON_PASS)
        all_apps_descriptor = s.get(MARATHON_APP_URL)

        if all_apps_descriptor.status_code == 404:
            print("--- ERROR! - Cant get apps from Marathon!")
            return None
        else:
            descriptor_all_apps_json = json.loads(all_apps_descriptor.text)
            # print(json.dumps(descriptor_all_apps_json, indent=3, sort_keys=True))
            print('--- Searching for App: {}'.format(app_name))
            for app in descriptor_all_apps_json['apps']:
                if app_name in app['id']:
                    # print(json.dumps(app, indent=3, sort_keys=True))
                    print('--- App {} found! ({})'.format(app_name, app['id']))
                    return app
            print('--- App {} NOT found!'.format(app))
            return None
    else:
        print('--- ERROR - Parameter app_name is empty!.')
        sys.exit(0)


def get_marathon_app(app_name):
    """
    Gets app descriptor from Marathon v2/apps API
    :param app_name: App name with foldering.  Per example: platform/services/shared/pg120
    :return: Json with App descriptor from Marathon or None if app_name doesnt exists
    """
    if app_name:
        print('\n\n\n')
        print('--- Getting Marathon descriptor for App: {}\n'.format(app_name))
        s = requests.Session()
        s.auth = (MARATHON_USER, MARATHON_PASS)
        descriptor = s.get(MARATHON_APP_URL + app_name)

        if descriptor.status_code == 404:
            print(" --- ERROR! - App {} does NOT exists!".format(app_name))
            return None
        else:
            descriptor_json = json.loads(descriptor.text)
            # print(json.dumps(descriptor_json, indent=3, sort_keys=True))
            return descriptor_json
    else:
        print('--- ERROR - Parameter app_name is empty!.')
        sys.exit(0)


def framework_check_framework_state(json_descriptor):
    """
    Framework is running when task is Healthy and Running
    :param json_descriptor for the app
    :return: True if the framework is running otherwise False
    """
    return json_descriptor['tasksHealthy'] & json_descriptor['tasksRunning']


def framework_check_minuteman_vip(json_descriptor, db_name):
    """
    Framework VIP is Ok when name is the same as the param and port is 80
    :param json_descriptor for the app
    :return: True if the VIP is Ok otherwise False
    """
    # TODO: La VIP esta en el primero de los puertos!!! Habría que verificar
    # TODO: que hay sólo una VIP y que puede estar en cualquier posición
    #
    # TODO: Verificación del nombre de la VIP
    vip_error = False

    framework_vip_elements = (json_descriptor['portDefinitions'][0]['labels']['VIP_0']).split(':')
    framework_vip_name = framework_vip_elements[0]
    framework_vip_port = framework_vip_elements[1]

    if framework_vip_name != db_name:
        print('--- ERROR - VIP Name is not Ok! ({}, expected: {})'.format(framework_vip_name, db_name))
        vip_error = True
    else:
        print('--- Minuteman VIP Name is Ok! ({})'.format(framework_vip_name))

    if framework_vip_port != '80':
        print('--- ERROR - VIP Port is not Ok! ({}, expected: 80)'.format(framework_vip_port))
        vip_error = True
    else:
        print('--- Minuteman VIP Port is Ok! ({})'.format(framework_vip_port))

    return vip_error


if __name__ == '__main__':
    print('\n--- Checking input parameter')
    check_parameter()

    if _DB_NAME:
        app_descriptor = None
        print('\n--- Getting Marathon descriptor for {}'.format(_DB_NAME))
        app_descriptor = find_marathon_app(_DB_NAME)
        if app_descriptor:
            # print(json.dumps(app_descriptor, indent=3, sort_keys=True))

            if framework_check_framework_state(app_descriptor):
                print('--- Framework state: Ok!')
            else:
                print('--- Framework state: KO!')

            if not framework_check_minuteman_vip(app_descriptor, _DB_NAME):
                print('--- Minuteman VIP OK!:')
            else:
                print('--- ERROR - Checking Minuteman VIP!')
                print('\n\n\n')

