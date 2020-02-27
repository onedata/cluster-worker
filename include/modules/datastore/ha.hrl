%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------------------------------------------------
%%% @doc
%%% Definition of messages used by ha master and slave.
%%% @end
%%%-------------------------------------------------------------------------------------------------------------

-ifndef(HA_HRL).
-define(HA_HRL, 1).

%%%=============================================================================================================
%%% Generic messages' templates
%%%=============================================================================================================

-define(MASTER_MSG(BODY), {ha_master_msg, BODY}). % Message sent from master to slave
-define(SLAVE_MSG(BODY), {ha_slave_msg, BODY}). % Message sent from slave to master
-define(SLAVE_INTERNAL_MSG(BODY), {ha_slave_internal_msg, BODY}). % Message sent between slave's
                                                                  % datastore_cache_writer and datastore_writer
-define(MASTER_INTERNAL_MSG(BODY), {ha_master_internal_msg, BODY}). % Message sent between master's
                                                                    % datastore_writer and datastore_cache_writer
-define(MANAGEMENT_MSG(BODY), {ha_config_msg, BODY}). % Message sent to datastore_writer to configure it

%%%=============================================================================================================
%%% Messages used to propagate information about documents that should be protected by slave
%%% They are all sent from master to slave - BACKUP_REQUEST/BACKUP_REQUEST_AND_LINK when handling datastore request
%%% and then KEYS_INACTIVATED after flush of keys
%%%=============================================================================================================

% Request slave to store data until it is flushed to couchbase
-define(BACKUP_REQUEST(KEYS, CACHE_REQUESTS), ?MASTER_MSG({backup_request, KEYS, CACHE_REQUESTS})).
% Equal BACKUP_REQUEST with additional request to link slave to master (to use gen_server casts instead of calls)
-define(BACKUP_REQUEST_AND_LINK(KEYS, CACHE_REQUESTS, PID),
    ?MASTER_MSG({{backup_request_and_link, KEYS, CACHE_REQUESTS, PID}})).
% Inform slave that data is saved to couchbase and it can forget backup data
-define(KEYS_INACTIVATED(KEYS), ?MASTER_MSG({keys_inactivated, KEYS})).

%%%=============================================================================================================
%%% Messages connected with processes lifecycle
%%% note: master and slave are linked with BACKUP_REQUEST_AND_LINK message from previous section
%%% Each master checks slave status with CHECK_SLAVE_STATUS on startup
%%% REQUEST_UNLINK is used by slave before termination if it was linked to master with BACKUP_REQUEST_AND_LINK before
%%% PROXY_REQUEST can appear shortly after master node repair when some clients processes think that master is still down
%%% MASTER_DOWN/UP appear when master node fails / is restarted
%%%=============================================================================================================

% Request used by master to check if slave is handling any master's keys (because master node was down)
-define(CHECK_SLAVE_STATUS(PID), ?MASTER_MSG({check_slave_status, PID})).
% Request used to inform master that slave will terminate and will not handle further backup messages
-define(REQUEST_UNLINK, ?SLAVE_MSG(request_unlink)).
% Request used when any process sends datastore requests to slave when master is already alive.
% Sends such request from slave to master
-define(PROXY_REQUESTS(REQUESTS), ?SLAVE_MSG({proxy_requests, REQUESTS})).
% Informs slave's datastore_cache_writer that master node is down and should flush protected keys
-define(MASTER_DOWN(KEYS), ?SLAVE_INTERNAL_MSG({master_down, KEYS})).
% Informs slave's datastore_writer that master node is down
-define(MASTER_DOWN, ?MANAGEMENT_MSG(master_down)).
% Informs slave's datastore_writer that master node is up
-define(MASTER_UP, ?MANAGEMENT_MSG(master_up)).

%%%=============================================================================================================
%%% Messages connected with emergency calls (handling request by slave because master is/was down)
%%% Handling emergency message is started on slave's datastore_cache_writer when any emergency request appears.
%%% After handling EMERGENCY_REQUEST_HANDLED message is sent by slave internally from datastore_cache_writer to
%%% datastore_writer and than further to master that sends it internally from datastore_writer to its
%%% datastore_cache_writer. After keys connected with emergency request inactivation EMERGENCY_KEYS_INACTIVATED
%%% message is sent similarly to EMERGENCY_REQUEST_HANDLED.
%%%=============================================================================================================

% Information that emergency request has been handled by slave
% Can be sent between master and slave or by master/slave internally so should be wrapped in generic message
-define(EMERGENCY_REQUEST_HANDLED(KEYS, CACHE_REQUESTS), {emergency_request_handled, KEYS, CACHE_REQUESTS}).
-define(EMERGENCY_REQUEST_HANDLED(KEYS), {emergency_request_handled, KEYS}).
% Information that keys connected with emergency request have been flushed
% Can be sent between master and slave or by master/slave internally so should be wrapped in generic message
-define(EMERGENCY_KEYS_INACTIVATED(KEYS), {emergency_keys_inactivated, KEYS}).

%%%=============================================================================================================
%%% Configuration messages
%%%=============================================================================================================

% Message sent to datastore_writer to inform it that configuration has changed
-define(CONFIG_CHANGED, ?MANAGEMENT_MSG(config_changed)).
% Request sent to datastore_cache_writer by datastore_writer to request reconfiguration
-define(CONFIGURE_BACKUP, ?MASTER_INTERNAL_MSG(configure_backup)).

-endif.
