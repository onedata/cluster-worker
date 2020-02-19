%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used to configure ha.
%%% @end
%%%-------------------------------------------------------------------
-module(ha_management).
-author("Michał Wrzeszcz").

-include("global_definitions.hrl").

%% API
-export([get_propagation_method/0]).

-type propagation_method() :: call | cast.

-export_type([propagation_method/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns propagation method used by datastore_cache_writer when working as ha master.
%% @end
%%--------------------------------------------------------------------
-spec get_propagation_method() -> propagation_method().
get_propagation_method() ->
    application:get_env(?CLUSTER_WORKER_APP_NAME, ha_propagation_method, cast).