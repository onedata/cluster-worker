%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions related to the cluster_worker app.
%%% @end
%%%-------------------------------------------------------------------
-module(cluster_worker).
-author("Lukasz Opiola").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_env/1, get_env/2, set_env/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec get_env(Key :: atom()) -> term() | no_return().
get_env(Key) ->
    case get_env(Key, undefined) of
        undefined ->
            ?alert("Could not find required env variable for ~w: ~w", [?CLUSTER_WORKER_APP_NAME, Key]),
            error({missing_env_variable, Key});
        Value ->
            Value
    end.


-spec get_env(Key :: atom(), Default :: term()) -> term().
get_env(Key, Default) ->
    case application:get_env(?CLUSTER_WORKER_APP_NAME, Key, Default) of
        % undefined values are treated as if there was no env set at all
        undefined -> Default;
        Other -> Other
    end.


-spec set_env(Key :: atom(), Value :: term()) -> ok.
set_env(Key, Value) ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, Key, Value).
