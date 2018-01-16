%%%--------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module provides datastore supervision tree details
%%% and is responsible for datastore components initialization.
%%% @end
%%%--------------------------------------------------------------------
-module(datastore_worker).
-author("Krzysztof Trzepla").

-behaviour(worker_plugin_behaviour).

-include("modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([supervisor_flags/0, supervisor_children_spec/0]).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    datastore_cache_manager:init(),
    couchbase_batch:init_counters(),
    ets:new(?CHANGES_COUNTERS, [named_table, public, set]),
    case init_models() of
        ok -> {ok, #{}};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(ping | healthcheck | {init_models, list()}) -> pong | ok.
handle(ping) ->
    pong;
handle(healthcheck) ->
    ok;
handle(Request) ->
    ?log_bad_request(Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a datastore supervisor flags.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_one, intensity => 5, period => 1}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a children spec for a datastore supervisor.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_children_spec() -> [supervisor:child_spec()].
supervisor_children_spec() ->
    [
        #{
            id => couchbase_pool_sup,
            start => {couchbase_pool_sup, start_link, []},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [couchbase_pool_sup]
        },
        #{
            id => couchbase_changes_sup,
            start => {couchbase_changes_sup, start_link, []},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [couchbase_changes_sup]
        }
    ].

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec init_models() -> ok | {error, term()}.
init_models() ->
    Models = datastore_config:get_models(),
    lists:foldl(fun
        (Model, ok) ->
            Ctx = datastore_model_default:get_ctx(Model),
            case datastore_model:init(Ctx) of
                ok -> ok;
                {error, Reason} -> {error, {model_init_error, Model, Reason}}
            end;
        (_Model, {error, Reason}) ->
            {error, Reason}
    end, ok, Models).