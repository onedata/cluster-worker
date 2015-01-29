#!/usr/bin/env python

import argparse
import os
import subprocess

from os.path import expanduser

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    description='Run make in the full development environment.')

parser.add_argument(
    '--image', '-i',
    action='store',
    default='onedata/builder',
    help='the image to use for building',
    dest='image')

parser.add_argument(
    '--src', '-s',
    action='store',
    default=os.getcwd(),
    help='the source directry to run make from',
    dest='src')

parser.add_argument(
    '--dst', '-d',
    action='store',
    default=os.getcwd(),
    help='the directory to store the build in',
    dest='dst')

parser.add_argument(
    '--keys', '-k',
    action='store',
    default=expanduser("~/.ssh"),
    help='the ssh keys directory needed for dependency fetching',
    dest='keys')

parser.add_argument(
    'params',
    action='store',
    nargs='*',
    help='parameters that will be passed to `make`')

args = parser.parse_args()

command = \
'''cp -RTf /root/keys /root/.ssh
chown -R root:root /root/.ssh
eval $(ssh-agent)
ssh-add
rsync -rogl /root/src/ /root/bin
make {params};
find . -user root -exec chown --reference /root/bin/[Mm]akefile -- '{{}}' +'''
command = command.format(params=' '.join(args.params))

subprocess.call(['docker', 'run', '--rm', '-ti',
                 '-v', '{src}:/root/src'.format(src=args.src),
                 '-v', '{dst}:/root/bin'.format(dst=args.dst),
                 '-v', '{keys}:/root/keys'.format(keys=args.keys),
                 '-w', '/root/bin',
                 args.image, 'sh', '-c', command])
