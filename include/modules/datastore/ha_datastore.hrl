%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------------------------------------------------
%%% @doc
%%% Definition of messages and macros used by HA master and slave.
%%% HA master and slave are TP processes. Single TP process can
%%% act as a master for some keys and as a slave for other set of keys.
%%% TP process is composed of datastore_writer, datastore_cache_writer
%%% and datastore_disc_writer. First two elements are important for HA.
%%% As datastore_writer typically caches requests and manages TP lifecycle, it is
%%% mainly used for HA lifecycle management and storing backup data (data
%%% used by slave to finish documents' flushing to couchbase when master is down).
%%% Datastore_cache_writer is typically responsible for handling requests and saving
%%% changed documents to memory. Thus, it is mainly responsible for broadcasting
%%% backup information (role of HA master) and handling requests as slave when master is down.
%%%
%%% Gen_server calls and casts can be used to propagate information about documents that should be protected by slave.
%%% Even if propagation method is cast, first request is sent using gen_server call to start slave process if it is
%%% not alive. After such request master and slave are linked so master knows that it can use gen_server cast as slave
%%% is alive. If slave is terminating, it sends unlink request to master.
%%%
%%% The requests connected with single datastore key can be processed only by master and slave at one time. Slave
%%% processes requests if master is down. If master recovers during request processing by slave, it withholds processing
%%% until processing of request is finished by slave. Similar behaviour can be observed with documents flushing - master
%%% is not flushing changes until flush of slave is finished.
%%%
%%% After master recovery it is possible that some requests that should be processed by master are sent to slave (delay
%%% in propagation of information that master is working again). In such a case, slave process redirects request to
%%% master that handles it and answers directly to calling process.

% Napisac ze proxy to datastre_internal_request
% poprawic analyse requests i typ requests_internal (wcale nie takie internal)
%%% @end
%%%-------------------------------------------------------------------------------------------------------------

-ifndef(HA_DATASTORE_HRL).
-define(HA_DATASTORE_HRL, 1).

%%%=============================================================================================================
%%% Generic messages' template
%%%=============================================================================================================

-record(ha_msg, {
    type :: ha_datastore_utils:ha_message_type(),
    body :: ha_datastore_utils:ha_message()
}).

% Convenience macros used to build ha_msg
% Include explanation of messages types
-define(MASTER_MSG(Body), #ha_msg{type = master, body = Body}). % Message sent from master to slave
-define(SLAVE_MSG(Body), #ha_msg{type = slave, body = Body}). % Message sent from slave to master
-define(INTERNAL_MSG(Body), #ha_msg{type = internal, body = Body}). % Message sent between datastore_writer
                                                                    % and datastore_cache_writer
-define(MANAGEMENT_MSG(Body), #ha_msg{type = management, body = Body}). % Message sent to datastore_writer to configure it

%%%=============================================================================================================
%%% Messages used to propagate information about documents that should be protected by slave.
%%% Backup is requested by master when handling datastore request.
%%% After flush of keys by master keys can be forgotten by slave.
%%% Master and slave can be linked using request_backup (see main doc of this hrl). In such a case,
%%% REQUEST_UNLINK is used by slave before termination.
%%%=============================================================================================================

% Request slave to store data until it is flushed to couchbase
% Can also request linking slave to master (see main doc of this hrl)
-record(request_backup, {
    keys :: datastore_doc_batch:cached_keys(),
    cache_requests :: [datastore_cache:cache_save_request()],
    link = false :: ha_slave:processes_link()
}).

% Inform slave that data is flushed and it can forget backup data
-record(forget_backup, {
    keys :: datastore_doc_batch:cached_keys()
}).

% Request used to inform master that slave will terminate and will not handle further backup messages
-define(REQUEST_UNLINK, request_unlink).

%%%=============================================================================================================
%%% Messages connected with recovering of master node. After master failure, slave works in failover state.
%%% Messages from this section are used when master node recovered but slave is still processing requests.
%%% If request is being processed during master recover it sends failover_request_data_processed messages to master
%%% to inform it when handling is over (to allow master processing of requests - see main doc of this hrl).
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
    request_handled = false :: boolean(),
    cache_requests_saved = #{}:: ha_slave:cache_requests_map(),
    keys_flushed = sets:new() :: ha_slave:keys_set()
}).

%%%=============================================================================================================
%%% Messages connected with processes lifecycle and processes configuration.
%%% Each master checks slave status with CHECK_SLAVE_STATUS on startup.
%%% MASTER_DOWN/UP appear when master node fails / is recovered.
%%%=============================================================================================================

% Request used by master to check if slave is handling any master's keys (because master node was down)
-record(get_slave_status, {
    answer_to :: pid()
}).

% Slave status is used to inform master if it is handling any master's keys
-record(slave_status, {
    failover_request_handling :: boolean(),
    failover_pending_cache_requests :: ha_slave:cache_requests_map(),
    failover_finished_memory_cache_requests :: [datastore_cache:cache_save_request()],
    failover_requests_to_handle :: datastore_writer:requests_internal()
}).

% Informs slave's datastore_writer that master node is down
-define(MASTER_DOWN, master_down).

% Informs slave's datastore_writer that master node is up
-define(MASTER_UP, master_up).

% Message sent to inform it that configuration has changed
-define(CONFIG_CHANGED, config_changed).

%%%=============================================================================================================
%%% Propagation method and slave mode names.
%%% Propagation method determines whether backup data is sent using gen_server call or cast.
%%% Mode determines whether slave process only backups data (stores backup data until it is flushed to couchbase or
%%% deleted) or handles requests when master is down.
%%%=============================================================================================================

% Propagation methods
-define(HA_CALL_PROPAGATION, call).
-define(HA_CAST_PROPAGATION, cast).
% Slave modes
-define(STANDBY_SLAVE_MODE, standby). % process only backup data
-define(FAILOVER_SLAVE_MODE, failover). % handle requests that should be handled by master (master is down)

-endif.
