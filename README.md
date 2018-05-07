# cluster-worker

*cluster-worker* component instances are the basic building blocks of [Onedata](https://github.com/onedata/onedata) services (including Oneprovider and Onezone), which enable Onedata services to easily scale on large number of nodes on a single cluster. Each *cluster-worker* can be configured for different tasks depending on the current needs (data access, metadata management, etc.) by [cluster-manager](https://github.com/onedata/cluster-manager) component. 

*cluster-workers* are implemented in Erlang, and provide the following functionality:
 * internal request load balancing between cluster nodes
 * unified data store in local memory and [mnesia](http://erlang.org/doc/man/mnesia.html) distributed database
 * automatic scaling of Onedata services to multiple cluster nodes

# Usage

This component of Onedata is not to be used separately, but only as a dependency of a specific Onedata service (e.g. Oneprovider or Onezone) and configured for specific operation.

It should be included as rebar dependency in final application. Its behaviour may be configured via plug-ings provided by final application (see API). However, for testing and debugging purposes, it can be started as a standalone application with default plug-ins 2 ways:
 - Inside Erlang Virtual Machine using command "application:start(cluster_worker)". It requires only compiled code available to the Erlang Virtual Machine,
 - From shell using script "cluster_worker" with argument "console" or "start". To generate start script, release of application must be created.

To prepare cluster_worker to be started:
 - Use command "make submodules" to initialize submodules
 - Use command "./make.py" to generate release,
 - Use "./make.py compile" to compile code only (faster than building release).


# APIs
Cluster-worker behaviour is influenced by the following plug-ins that should be included in the final application:

 * *datastore_config_plugin* - module that provides datastore_config_behaviour to indicate which modules should be used by datastore,
 * *dns_worker_plugin* - module that provides dns_worker_plugin_default. It allows manipulation of build-in dns server,
 * *logger_plugin* - module that provides logger_plugin_behaviour. It allows manipulation of logger behaviour,
 * *node_manager_plugin* - module that provides ode_manager_plugin_behaviour. It allows configuration of modules (entities that provide functionality of application) and listeners.

Default implementation of plug-ins is in *src/plugins* directory.

# cluster-worker in Onedata

Two basic elements of Onedata - [op-worker](https://github.com/onedata/op-worker) and [oz-worker](https://github.com/onedata/oz-worker) base on cluster-worker.

