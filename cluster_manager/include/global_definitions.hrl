%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
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
-define(APP_NAME, cluster_manager).

%% Local name (name and node is used to identify it) of supervisor that
%% coordinates application at each node (one supervisor per node).
-define(APPLICATION_SUPERVISOR_NAME, cluster_manager_sup).

-endif.
