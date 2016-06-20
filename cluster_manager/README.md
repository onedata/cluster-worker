# cluster-manager

*cluster-manager* is a component [Onedata](https://github.com/onedata/onedata) services (including Oneprovider and Onezone), which coordinates the [cluster-worker](https://github.com/onedata/cluster-worker) instances on a single Onedata deployment. 


The role of *cluster-manager* is to:
 * supervise *cluster-worker* nodes (monitoring and restarting workers)
 * support the load-balancing algorithm for internal requests
 * coordinate communication between *cluster-workers*

# Usage

*cluster-manager* is a helper application for *cluster-workers*. Thus, it should not be used as a standalone application standalone but always as dependency of cluster-worker. For testing purposes, it may be started in 2 ways:

Inside Erlang Virtual Machine using command “application:start(cluster_manager)”. It requires only compiled code at path of Erlang Virtual Machine.
From shell using script “cluster_manager” with argument “console” or “start”. To generate start script, release of application must be created.

To prepare cluster-manager script:

 * Use command “./make.py” to generate release.
 * Use “./make.py compile” to compile code only (faster than building release).

To provide HA, more than one *cluster-manager* instance should be started. Started instances should be configured to form [Erlang distributed application](http://erlang.org/doc/design_principles/distributed_applications.html).



# APIs
*cluster-manager* is implemented as an Erlang [gen-server](http://elixir-lang.org/docs/stable/elixir/GenServer.html) and exposes standard gen-server API. Thus, communication with *cluster-manager* is only possible from Erlang nodes which have same cookie (see http://erlang.org/doc/reference_manual/distributed.html - 13.7 Security)


# cluster-manager in Onedata
Onedata is a multi-cluster solution composed from various types of worker nodes. *cluster-manager* instances orchrestrate the work of a single Onedata cluster. 
