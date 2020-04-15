%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------------------------------------------------
%%% @doc
%%% Definition of messages and macros used by HA master and slave.
%%% HA master and slave are TP processes. Master process handles requests (standard role of tp process) and sends
%%% backup information to slave process. Slave process is responsible for management of backup information as long as
%%% it is needed (backup data is deleted after saving document to couchbase or deleting document) and handling requests
%%% if node of master fails. Single TP process can act as a master for some keys and as a slave for other set of keys.
%%% TP process is composed of datastore_writer, datastore_cache_writer and datastore_disc_writer.
%%% First two elements are important for HA. As datastore_writer typically caches requests and manages TP lifecycle,
%%% it is mainly used for HA lifecycle management and storing backup data (data used by slave to finish documents'
%%% flushing to couchbase when master is down). Backup data is deleted when master sends message to slave that informs
%%% that document has been inactivated (flushed to couchbase in case of cached documents).
%%% Datastore_cache_writer is typically responsible for handling requests and saving
%%% changed documents to memory. Thus, it is mainly responsible for broadcasting
%%% backup information (role of HA master) and handling requests as slave when master is down.
%%%
%%% Gen_server calls and casts can be used to propagate information about documents that should be protected by slave.
%%% Even if propagation method is cast, first request is sent using gen_server call to start slave process if it is
%%% not alive. After such request master and slave are linked so master knows that it can use gen_server cast.
%%% If slave is terminating, it sends unlink request to master.
%%%
%%% The requests connected with single datastore key can be processed only by one process at one time - master or slave.
%%% Slave processes requests if master is down. If master recovers during request processing by slave, it withholds
%%% processing until processing of current request is finished by slave. The same behaviour applies to documents'
%%% flushing - master is not flushing changes until flushing by slave is finished.
%%%
%%% After master recovery it is possible that some requests that should be processed by master are sent to slave (delay
%%% in propagation of information that master is working again). In such a case, slave process redirects request to
%%% master that will handle it and answer directly to calling process. For such redirection, datastore_internal_request
%%% is used - no new message in HA protocol is needed.
%%%
%%% HA is used during cluster reorganization (permanent node adding/deleting). In such a case reorganization status
%%% is set and keys migration is initialized. All tp processes remember operations on keys that will belong to other
%%% nodes after reorganization and new masters for such keys use failover mechanism (mechanism used when any node fails)
%%% to takeover such keys. Standby mode is restored after migration.
%%% @end
%%%-------------------------------------------------------------------------------------------------------------

-ifndef(HA_DATASTORE_HRL).
-define(HA_DATASTORE_HRL, 1).

%%%=============================================================================================================
%%% Generic messages' template
%%%=============================================================================================================

-record(ha_msg, {
    type :: ha_datastore:ha_message_type(),
    body :: ha_datastore:ha_message()
}).

% Convenience macros used to build ha_msg
-define(MASTER_MSG(Body), #ha_msg{type = master, body = Body}). % Message sent from master to slave
-define(SLAVE_MSG(Body), #ha_msg{type = slave, body = Body}). % Message sent from slave to master
-define(INTERNAL_MSG(Body), #ha_msg{type = internal, body = Body}). % HA related message sent between datastore_writer
                                                                    % and datastore_cache_writer
-define(MANAGEMENT_MSG(Body), #ha_msg{type = management, body = Body}). % Message sent to datastore_writer to change
                                                                        % its behaviour connected with HA

%%%=============================================================================================================
%%% Messages used to propagate information about documents that should be protected by slave.
%%% Backup is requested by master when handling datastore request (master sends forget_backup message).
%%% After flush of keys by master, backup can be forgotten by slave (master sends store_backup message).
%%% Master and slave can be linked using store_backup (see main doc of this hrl). In such a case,
%%% REQUEST_UNLINK is used by slave before termination.
%%%=============================================================================================================

% Request slave to store data until it is flushed to couchbase
% Can also request linking slave to master (see main doc of this hrl)
-record(store_backup, {
    keys :: datastore_doc_batch:cached_keys(),
    cache_requests :: [datastore_cache:cache_save_request()],
    link = false :: ha_datastore_slave:link_to_master()
}).

% Inform slave that data is flushed and it can forget backup data
-record(forget_backup, {
    keys :: datastore_doc_batch:cached_keys()
}).

% Request used to inform master that slave will terminate and will not handle further backup messages
-define(REQUEST_UNLINK, request_unlink).

%%%=============================================================================================================
%%% Message connected with recovering of master node. After master failure, slave works in failover state.
%%% When master node recovers, slave can be still processing requests. In such a case slave sends
%%% failover_request_data_processed messages to master to inform it when handling is over
%%% (to allow master processing of requests - see main doc of this hrl).
%%% If any request is sent to slave after master recovery (client is using old routing information and does not know
%%% that master is alive), slave redirect request to master (no opaque message needed).
%%% failover_request_data_processed message is sent not only between slave and master but also internally between
%%% datastore_cache_writer and datastore_writer of master and slave (it is sent by datastore_cache_writer of slave
%%% and is finally handled by datastore_cache_writer of master, traversing through datastore_writer of
%%% both master and slave).
%%%=============================================================================================================

% Information that data connected with failover request has been processed by slave
% (cache requests have been created or keys have been flushed)
-record(failover_request_data_processed, {
    finished_action = ha_datastore_master:failover_action(),
    cache_requests_saved = #{}:: ha_datastore_slave:cache_requests_map(),
    keys_flushed = sets:new() :: ha_datastore_slave:keys_set(),
    failed_node :: node() | undefined
}).

%%%=============================================================================================================
%%% Messages connected with processes lifecycle and processes configuration.
%%% Each master checks slave status with get_slave_failover_status on startup.
%%% MASTER_DOWN/UP appear when master node fails / is recovered.
%%% Management messages (CONFIG_CHANGED, MASTER_DOWN/UP) concern whole tp processes that act as master and slave for
%%% different keys at the same time. However, they are handled by ha_slave as they affect slave role more
%%% (it starts/ends performing master duties).
%%%=============================================================================================================

% Request used by master to check if slave is handling any master's keys (because master node was down)
-record(get_slave_failover_status, {
    answer_to :: pid()
}).

% Slave failover status is used to inform master if it is handling any master's keys
-record(slave_failover_status, {
    is_handling_requests :: boolean(),
    pending_cache_requests :: ha_datastore_slave:cache_requests_map(),
    finished_memory_cache_requests :: [datastore_cache:cache_save_request()],
    requests_to_handle :: datastore_writer:requests_internal()
}).

% Informs slave that master node is down
-define(MASTER_DOWN, master_down).

% Informs slave that master node is up
-define(MASTER_UP, master_up).

% Message sent to inform process that configuration has changed and should be reloaded
% (processes get configuration during initialization and cache it)
-define(CONFIG_CHANGED, config_changed).

% Message sent to inform process that cluster reorganization (node adding/deleting) has started
-define(CLUSTER_REORGANIZATION_STARTED, cluster_reorganization_started).
% Internal datastore message for reorganization handling
-record(cluster_reorganization_started, {
    caller_pid :: pid(),
    message_ref :: reference() % reference used to answer to caller
}).

%%%=============================================================================================================
%%% Propagation methods, slave mode and failover actions names.
%%% Propagation method determines whether backup data is sent using gen_server call or cast.
%%% Mode determines whether slave process only backups data (stores backup data until it is flushed to couchbase or
%%% deleted) or handles requests when master is down.
%%% Failover actions are used as values of field `finished_action` in #failover_request_data_processed message
%%% to describe activity that triggered sending message.
%%%=============================================================================================================

% Propagation methods
-define(HA_CALL_PROPAGATION, call).
-define(HA_CAST_PROPAGATION, cast).
% Slave modes
-define(STANDBY_SLAVE_MODE, standby). % process only backup data
-define(FAILOVER_SLAVE_MODE, failover). % handle requests that should be handled by master (master is down)
-define(CLUSTER_REORGANIZATION_SLAVE_MODE, cluster_reorganization). % special requests handling mode to prepare
                                                                    % permanent new master migration to other node
% Failover actions
% Note: Reorganization uses failover handling methods as node deletion/adding handling by tp process
% is similar to node failure/recovery handling by this process
-define(REQUEST_HANDLING_ACTION, request_handling).
-define(KEY_FLUSHING_ACTION, keys_flushing).
-record(preparing_reorganization, {
    node :: node()
}).

-endif.
