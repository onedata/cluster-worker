%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of names used to identify
%%% different parts of application (or whole application).
%%% @end
%%%-------------------------------------------------------------------
-ifndef(REGISTERED_NAMES_HRL).
-define(REGISTERED_NAMES_HRL, 1).

%% Name of the application.
-define(APP_NAME, oneprovider_node).

%% Global name of gen_server that provides ccm functionality.
-define(CCM, central_cluster_manager).

%% Local name (name and node is used to identify it) of gen_server that 
%% coordinates node life cycle.
-define(NODE_MANAGER_NAME, node_manager).

%% Local name (name and node is used to identify it) of supervisor that 
%% coordinates application at each node (one supervisor per node).
-define(SUPERVISOR_NAME, sup_name).

%% Local name (name and node is used to identify it) of gen_server that
%% works as a dispatcher.
-define(DISPATCHER_NAME, request_dispatcher).

-endif.
