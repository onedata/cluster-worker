#!/usr/bin/env python

"""
Prepares a set dockers with oneclients that are configured and ready to start.
Run the script with -h flag to learn about script's running options.
"""

from __future__ import print_function
import argparse
import collections
import common
import copy
import docker
import json
import os
import time


def tweak_config(config, name, uid):
    cfg = copy.deepcopy(config)
    cfg['nodes'] = {'node': cfg['nodes'][name]}
    node = cfg['nodes']['node']
    node['name'] = common.format_hostname(node['name'], uid)
    node['op_hostname'] = common.format_hostname(node['op_hostname'], uid)
    node['gr_hostname'] = common.format_hostname(node['gr_hostname'], uid)

    return cfg


parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Set up dockers with oneclient preconfigured.')

parser.add_argument(
    '--image', '-i',
    action='store',
    default='onedata/worker',
    help='the image to use for the container',
    dest='image')

parser.add_argument(
    '--bin', '-b',
    action='store',
    default=os.getcwd(),
    help='path to oneclient repository (precompiled)',
    dest='bin')

parser.add_argument(
    '--dns', '-d',
    action='store',
    default='none',
    help='IP address of DNS or "none" - if no dns should be started or ' +
         '"auto" - if it should be started automatically',
    dest='dns')

parser.add_argument(
    '--uid', '-u',
    action='store',
    default=str(int(time.time())),
    help='uid that will be concatenated to docker names',
    dest='uid')

parser.add_argument(
    'config_path',
    action='store',
    help='path to gen_dev_args.json that will be used to configure oneclient instances')

args = parser.parse_args()
uid = args.uid

config = common.parse_json_file(args.config_path)['oneclient']
configs = [tweak_config(config, node, uid) for node in config['nodes']]

output = collections.defaultdict(list)

dns = args.dns
if dns == 'auto':
    dns_config = common.run_script_return_dict('dns_up.py', ['--uid', uid])
    dns = dns_config['dns']
    output['dns'] = dns_config['dns']
    output['docker_ids'] = dns_config['docker_ids']
elif dns == 'none':
    dns = None

for cfg in configs:
    node = cfg['nodes']['node']
    node_name = node['name']
    (name, sep, hostname) = node_name.partition('@')

    cert_file_path = node['user_cert']
    key_file_path = node['user_key']
    # cert_file_path and key_file_path can both be an absolute path
    # or relative to gen_dev_args.json
    if not os.path.isabs(cert_file_path):
        cert_file_path = common.get_file_dir(args.config_path) + '/' + cert_file_path
    if not os.path.isabs(key_file_path):
        key_file_path = common.get_file_dir(args.config_path) + '/' + key_file_path

    node['user_cert'] = '/tmp/cert'
    node['user_key'] = '/tmp/key'

    command = '''set -e
cp /root/build/release/oneclient /root/bin/oneclient
cat <<"EOF" > /tmp/cert
{cert_file}
EOF
cat <<"EOF" > /tmp/key
{key_file}
EOF
bash'''
    command = command.format(
        cert_file=open(cert_file_path, 'r').read(),
        key_file=open(key_file_path, 'r').read())

    container = docker.run(
        image=args.image,
        hostname=hostname,
        detach=True,
        interactive=True,
        tty=True,
        workdir='/root/bin',
        name='{0}_{1}'.format(name, uid),
        volumes=[(args.bin, '/root/build', 'ro')],
        dns=[dns],
        command=command)

    output['docker_ids'].append(container)
    output['client_nodes'].append(hostname)

# Print JSON to output so it can be parsed by other scripts
print(json.dumps(output))