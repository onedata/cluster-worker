# coding=utf-8
"""Authors: Tomasz Lichoń, Konrad Zemek
Copyright (C) 2015 ACK CYFRONET AGH
This software is released under the MIT license cited in 'LICENSE.txt'

Brings up a set of Global Registry nodes along with databases.
They can create separate clusters.
"""

import copy
import json
import os
import re
from . import common, docker, dns, gui_livereload

LOGFILE = '/tmp/run.log'
# mounting point for globalregistry docker
DOCKER_BINDIR_PATH = '/root/build'



def oz_domain(oz_instance, uid):
    """Formats domain for a GR."""
    return common.format_hostname(oz_instance, uid)


def oz_hostname(node_name, oz_instance, uid):
    """Formats hostname for a docker hosting GR.
    NOTE: Hostnames are also used as docker names!
    """
    return common.format_hostname([node_name, oz_instance], uid)


def oz_db_hostname(db_node_name, oz_instance, uid):
    """Formats hostname for a docker hosting bigcouch for GR.
    NOTE: Hostnames are also used as docker names!
    """
    return common.format_hostname([db_node_name, oz_instance], uid)


def db_erl_node_name(db_node_name, oz_instance, uid):
    """Formats erlang node name for a vm on GR DB docker.
    """
    hostname = oz_db_hostname(db_node_name, oz_instance, uid)
    return common.format_erl_node_name('bigcouch', hostname)


def oz_erl_node_name(node_name, oz_instance, uid):
    """Formats erlang node name for a vm on GR docker.
    """
    hostname = oz_hostname(node_name, oz_instance, uid)
    return common.format_erl_node_name('oz', hostname)


def _tweak_config(config, oz_node, oz_instance, uid):
    cfg = copy.deepcopy(config)
    cfg['nodes'] = {'node': cfg['nodes'][oz_node]}

    sys_config = cfg['nodes']['node']['sys.config']['oz']
    sys_config['db_nodes'] = [db_erl_node_name(n, oz_instance, uid)
                              for n in sys_config['db_nodes']]

    if 'http_domain' in sys_config:
        sys_config['http_domain'] = {'string': oz_domain(oz_instance, uid)}

    if 'vm.args' not in cfg['nodes']['node']:
        cfg['nodes']['node']['vm.args'] = {}

    vm_args = cfg['nodes']['node']['vm.args']
    vm_args['name'] = oz_erl_node_name(oz_node, oz_instance, uid)

    return cfg


def _node_up(oz_id, domain, oz_ips, dns_ips, dns_config, gen_dev_config):
    """Updates dns.config and starts the GR node"""
    ip_addresses = {
        domain: oz_ips
    }
    ns_servers = []
    for i in range(len(dns_ips)):
        ns = 'ns{0}.{1}'.format(i, domain)
        ns_servers.append(ns)
        ip_addresses[ns] = [dns_ips[i]]
    primary_ns = ns_servers[0]
    mail_exchange = 'mail.{0}'.format(domain)
    ip_addresses[mail_exchange] = [oz_ips[0]]
    admin_mailbox = 'dns-admin.{0}'.format(domain)

    cname = '{{cname, "{0}"}},'.format(domain)
    dns_config = re.sub(
        re.compile(r"\{cname,\s*[^\}]*\},", re.MULTILINE),
        cname,
        dns_config)

    ip_addresses_entries = []
    for address in ip_addresses:
        ip_list = '"{0}"'.format('","'.join(ip_addresses[address]))
        ip_addresses_entries.append('        {{"{0}", [{1}]}}'
                                    .format(address, ip_list))
    ip_addresses = '{{ip_addresses, [\n{0}\n    ]}},'.format(
        ',\n'.join(ip_addresses_entries))
    dns_config = re.sub(
        re.compile(r"\{ip_addresses,\s*\[(\s*\{[^\}]*\}[,]?\s*)*\]\},",
                   re.MULTILINE),
        ip_addresses,
        dns_config)

    ns_servers = '{{ns_servers, [\n        "{0}"\n    ]}},'.format(
        '",\n        "'.join(ns_servers))
    dns_config = re.sub(
        re.compile(r"\{ns_servers,\s*\[[^\]\}]*\]\},", re.MULTILINE),
        ns_servers,
        dns_config)

    mail_exchange = '{{mail_exchange, [\n        {{10, "{0}"}}\n    ]}},' \
        .format(mail_exchange)
    dns_config = re.sub(
        re.compile(r"\{mail_exchange,\s*\[[^\]]*\]\},", re.MULTILINE),
        mail_exchange,
        dns_config)

    primary_ns = '{{primary_ns, "{0}"}},'.format(primary_ns)
    dns_config = re.sub(
        re.compile(r"\{primary_ns,\s*[^\}]*\},", re.MULTILINE),
        primary_ns,
        dns_config)

    admin_mailbox = '{{admin_mailbox, "{0}"}},'.format(admin_mailbox)
    dns_config = re.sub(
        re.compile(r"\{admin_mailbox,\s*[^\}]*\},", re.MULTILINE),
        admin_mailbox,
        dns_config)

    oz_command = '''set -e
mkdir -p /root/bin/node/log/
echo 'while ((1)); do chown -R {uid}:{gid} /root/bin/node/log; sleep 1; done' > /root/bin/chown_logs.sh
bash /root/bin/chown_logs.sh &
cat <<"EOF" > /tmp/gen_dev_args.json
{gen_dev_args}
EOF
escript bamboos/gen_dev/gen_dev.escript /tmp/gen_dev_args.json
cat <<"EOF" > /root/bin/node/data/dns.config
{dns_config}
EOF
/root/bin/node/bin/oz_worker console >> {logfile}'''
    oz_command = oz_command.format(
        uid=os.geteuid(),
        gid=os.getegid(),
        gen_dev_args=json.dumps({'oz_worker': gen_dev_config}),
        dns_config=dns_config,
        logfile=LOGFILE)

    docker.exec_(
        container=oz_id,
        detach=True,
        interactive=True,
        tty=True,
        command=oz_command)


def _docker_up(image, bindir, config, dns_servers, logdir):
    """Starts the docker but does not start GR
    as dns.config update is needed first
    """
    node_name = config['nodes']['node']['vm.args']['name']
    cookie = config['nodes']['node']['vm.args']['setcookie']
    db_nodes = config['nodes']['node']['sys.config']['oz']['db_nodes']

    (oz_name, sep, oz_hostname) = node_name.partition('@')

    # Start DB node for current GR instance.
    # Currently, only one DB node for GR is allowed, because we are using links.
    # It's impossible to create a bigcouch cluster with docker's links.
    db_node = db_nodes[0]
    (db_name, sep, db_hostname) = db_node.partition('@')

    db_command = '''echo '[httpd]' > /opt/bigcouch/etc/local.ini
echo 'bind_address = 0.0.0.0' >> /opt/bigcouch/etc/local.ini
sed -i 's/-name bigcouch/-name {name}@{host}/g' /opt/bigcouch/etc/vm.args
sed -i 's/-setcookie monster/-setcookie {cookie}/g' /opt/bigcouch/etc/vm.args
/opt/bigcouch/bin/bigcouch'''
    db_command = db_command.format(name=db_name, host=db_hostname,
                                   cookie=cookie)

    bigcouch = docker.run(
        image='onedata/bigcouch',
        name=db_hostname,
        hostname=db_hostname,
        detach=True,
        command=db_command)

    volumes = [(bindir, '/root/build', 'ro')]

    if logdir:
        logdir = os.path.join(os.path.abspath(logdir), oz_hostname)
        volumes.extend([(logdir, '/root/bin/node/log', 'rw')])

    if 'gui_livereload' in config:
        if config['gui_livereload']:
            volumes.extend(gui_livereload.required_volumes(
                os.path.join(bindir, 'rel/gui.config'),
                bindir,
                DOCKER_BINDIR_PATH))

    # Just start the docker, GR will be started later when dns.config is updated
    gr = docker.run(
        image=image,
        name=oz_hostname,
        hostname=oz_hostname,
        detach=True,
        interactive=True,
        tty=True,
        workdir='/root/build',
        volumes=volumes,
        dns_list=dns_servers,
        link={db_hostname: db_hostname},
        command=['bash'])

    return gr, {
        'docker_ids': [bigcouch, gr],
        'oz_db_nodes': ['{0}@{1}'.format(db_name, db_hostname)],
        'oz_worker_nodes': ['{0}@{1}'.format(oz_name, oz_hostname)]
    }


def up(image, bindir, dns_server, uid, config_path, logdir=None):
    config = common.parse_json_config_file(config_path)
    input_dir = config['dirs_config']['oz_worker']['input_dir']
    dns_servers, output = dns.maybe_start(dns_server, uid)

    for oz_instance in config['zone_domains']:
        gen_dev_cfg = {
            'config': {
                'input_dir': input_dir,
                'target_dir': '/root/bin'
            },
            'nodes': config['zone_domains'][oz_instance]['oz_worker']
        }

        # If present, include gui_livereload
        if 'gui_livereload' in config['zone_domains'][oz_instance]:
            gen_dev_cfg['gui_livereload'] = config['zone_domains'][
                oz_instance]['gui_livereload']

        tweaked_configs = [_tweak_config(gen_dev_cfg, oz_node, oz_instance, uid)
                           for oz_node in gen_dev_cfg['nodes']]

        oz_nodes = []
        oz_ips = []
        oz_configs = {}
        for cfg in tweaked_configs:
            oz, node_out = _docker_up(image, bindir, cfg, dns_servers, logdir)
            oz_nodes.append(oz)
            common.merge(output, node_out)
            oz_configs[oz] = cfg
            oz_ips.append(common.get_docker_ip(oz))

        domain = oz_domain(oz_instance, uid)

        dns_cfg_path = os.path.join(os.path.abspath(bindir),
                                    input_dir, 'data', 'dns.config')
        orig_dns_cfg = open(dns_cfg_path).read()
        # Update dns.config file on each OZ node
        for id in oz_configs:
            _node_up(id, domain, oz_ips, oz_ips, orig_dns_cfg, oz_configs[id])

        if 'gui_livereload' in gen_dev_cfg:
            if gen_dev_cfg['gui_livereload']:
                print 'Starting GUI livereload for zone {0}.'.format(
                    domain)
                for container_id in oz_nodes:
                    gui_livereload.run(
                        container_id,
                        os.path.join(bindir, 'rel/gui.config'),
                        DOCKER_BINDIR_PATH,
                        '/root/bin/node')

        domains = {
            'domains': {
                domain: {
                    'ns': oz_ips,
                    'a': []
                }
            }
        }
        common.merge(output, domains)

    # Make sure domains are added to the dns server
    dns.maybe_restart_with_configuration(dns_server, uid, output)

    return output
