%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains global definitions of component names, macros and types
%%% used accross the application.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(GLOBAL_DEFINITIONS_HRL).
-define(GLOBAL_DEFINITIONS_HRL, 1).

%%%===================================================================
%%% Global names
%%%===================================================================

%% Name of the application.
-define(APP_NAME, oneprovider_node).

%% Local name (name and node is used to identify it) of supervisor that
%% coordinates application at each node (one supervisor per node).
-define(APPLICATION_SUPERVISOR_NAME, oneprovider_node_sup).

%% Global name of gen_server that provides ccm functionality.
-define(CCM, central_cluster_manager).

%% Local name (name and node is used to identify it) of gen_server that
%% coordinates node life cycle.
-define(NODE_MANAGER_NAME, node_manager).

%% Local name (name and node is used to identify it) of supervisor that
%% coordinates workers at each node
-define(MAIN_WORKER_SUPERVISOR_NAME, main_worker_sup).

%% Local name (name and node is used to identify it) of gen_server that
%% works as a dispatcher.
-define(DISPATCHER_NAME, request_dispatcher).

%% Local name (name and node is used to identify it) of supervisor that
%% coordinates the processes started by concrete worker_host (given by arg)
-define(WORKER_HOST_SUPERVISOR_NAME(Module), list_to_atom(atom_to_list(Module) ++ "_sup")).

%%%===================================================================
%%% Global types
%%%===================================================================

% Macro that should be used to log an error during healthcheck
-define(HEALTHCHECK_ERROR_LOG_MSG(_Msg),
    HEALTHCHECK_ERROR_LOG(_Msg, [])).
-define(HEALTHCHECK_ERROR_LOG_MSG(_Msg, _Args),
    lists:flatten(io_lib:format("Healthcheck error in ~p on node ~p: " ++ _Msg, [?MODULE, node()] ++ _Args))).

-endif.
