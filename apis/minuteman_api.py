#!/opt/mesosphere/bin/python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 22 14:06

@author: Arquillos

minuteman-api.py - Getting information from Minuteman
"""
import sys
import argparse
import requests
import json
import dns.resolver


# VARs - Minuteman
MINUTEMAN_APP_URL = "http://localhost:61421/vips"

DB_NAME = None


def check_parameter():
    global DB_NAME
    if len(sys.argv) != 2:
        print('\n\nERROR --- Wrong number of arguments.')
        print('        - Use: minuteman-api.py <App Name>\n\n')
        sys.exit(0)

    parser = argparse.ArgumentParser()
    parser.add_argument("app_name", help="Name of the App VIP registered in Minuteman.")
    args = parser.parse_args()
    DB_NAME = args.app_name


def check_vip(app_name, vip_ip, mesos_ip):
    """
    Gets the registered information in Minuteman with the vips API
    :param app_name: App name
    :param vip_ip: Minuteman VIP ip
    ;:param mesos_ip: Mesos B.D. ip
    :return: Json with registered info or None if app_name doesnt exists
    """
    if app_name:
        print('--- Getting Minuteman info for App: {}\n'.format(app_name))
        descriptor = requests.get(MINUTEMAN_APP_URL)

        descriptor_json = json.loads(descriptor.text)
        # print(json.dumps(descriptor_json, indent=3, sort_keys=True))

        for key, value in (descriptor_json['vips']).items():
            if vip_ip in key:
                print('Minuteman - key: {}'.format(key))
                print('Minuteman - value: {}'.format(value))

                # Only One IP for a Minuteman VIP
                if len(value) != 1:
                    print('--- ERROR - More than one entry! Service with more than one instance?')
                    return False

                # Healthy
                # First Element
                ip_key = next(iter(value))
                ip_value_healthy = value[ip_key]['is_healthy']
                if not ip_value_healthy:
                    print('--- ERROR - Minuteman VIP not healthy ({}).'.format(ip_value_healthy))
                    return False

                # Minuteman VIP IP == Mesos IP
                minuteman_ip = ip_key.split(':')[0]
                if minuteman_ip != mesos_ip:
                    print('--- ERROR - Minuteman VIP IP ({}) is not equal to Mesos IP ({}).'.format(minuteman_ip, mesos_ip))
                    return False

                return True
        print('--- ERROR - Minuteman VIP not found ({}).'.format(vip_ip))
        return False
    else:
        print('--- ERROR - Parameter app_name is empty!.')
        sys.exit(0)


def check_dns(bd_name):
    # Basic query
    for dns_data in dns.resolver.query(bd_name, 'A'):
        # print(dns_data.to_text())
        response = dns_data.to_text()
    return response


def check_minuteman_vip(bd_name):
    minuteman_ip = check_dns(bd_name + '.marathon.l4lb.thisdcos.directory')
    mesos_ip = check_dns(bd_name + '.mesos')

    print('\n--- Checking Minuteman info for App {}'.format(bd_name))
    if check_vip(bd_name, minuteman_ip, mesos_ip):
        print('--- Minuteman seems OK!')
        return True
    return False


if __name__ == '__main__':
    print('\n--- Checking input parameter')
    check_parameter()

    vip_ip = check_dns(DB_NAME + '.marathon.l4lb.thisdcos.directory')
    m_ip = check_dns(DB_NAME + '.mesos')

    if DB_NAME:
        print('\n--- Checking Minuteman info for App {}'.format(DB_NAME))
        if check_vip(DB_NAME, vip_ip, m_ip):
            print('--- Minuteman seems OK!')
