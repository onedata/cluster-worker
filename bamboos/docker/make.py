#!/usr/bin/env python

"""
Runs 'make' command in a dockerized development environment. The files are
copied from 'source directory' to 'output directory' and then the make is ran.
The copy operation is optimized, so that only new and changed files are copied.
The script uses user's SSH keys in case dependency fetching is needed.

Run the script with -h flag to learn about script's running options.
"""

import argparse
import docker
import os
import platform
import sys

from os.path import expanduser

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Run make inside a dockerized development environment.')

parser.add_argument(
    '--image', '-i',
    action='store',
    default='onedata/builder',
    help='docker image to use for building',
    dest='image')

parser.add_argument(
    '--src', '-s',
    action='store',
    default=os.getcwd(),
    help='source directory to run make from',
    dest='src')

parser.add_argument(
    '--dst', '-d',
    action='store',
    default=os.getcwd(),
    help='destination directory where the build will be stored',
    dest='dst')

parser.add_argument(
    '--keys', '-k',
    action='store',
    default=expanduser("~/.ssh"),
    help='directory of ssh keys used for dependency fetching',
    dest='keys')

parser.add_argument(
    '--reflect-volume', '-r',
    action='append',
    default=[],
    help="host's paths that will be directly reflected in container's filesystem",
    dest='reflect')

parser.add_argument(
    'params',
    action='store',
    nargs='*',
    help='parameters passed to `make`')

args = parser.parse_args()
home = expanduser('~')

command = '''
import os, shutil, subprocess, sys

os.environ['HOME'] = '{home}'

ssh_home = '/root/.ssh'
if {shed_privileges}:
    subprocess.call(['useradd', '--create-home', '--uid', '{uid}', 'maketmp'])
    ssh_home = '/home/maketmp/.ssh'
    os.setregid({gid}, {gid})
    os.setreuid({uid}, {uid})

if '{src}' != '{dst}':
    ret = subprocess.call(['rsync', '--archive', '/tmp/src/', '{dst}'])
    if ret != 0:
        sys.exit(ret)

shutil.copytree('/tmp/keys', ssh_home)
for root, dirs, files in os.walk(ssh_home):
    for dir in dirs:
        os.chmod(os.path.join(root, dir), 0o700)
    for file in files:
        os.chmod(os.path.join(root, file), 0o600)

sh_command = 'set -e; eval $(ssh-agent) > /dev/null; ssh-add; make {params}'
ret = subprocess.call(['sh', '-c', sh_command])
sys.exit(ret)
'''
command = command.format(
    params=' '.join(args.params),
    uid=os.geteuid(),
    gid=os.getegid(),
    src=args.src,
    dst=args.dst,
    home=home,
    shed_privileges=(platform.system() == 'Linux'))

reflect = [(home, 'ro'), (args.dst, 'rw')]
reflect.extend(zip(args.reflect, ['rw'] * len(args.reflect)))

ret = docker.run(tty=True,
                 interactive=True,
                 rm=True,
                 reflect=reflect,
                 volumes=[(args.keys, '/tmp/keys', 'ro'),
                          (args.src, '/tmp/src', 'ro')],
                 workdir=args.dst,
                 image=args.image,
                 command=['python', '-c', command])
sys.exit(ret)