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
-include("exometer_utils.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([supervisor_flags/0, supervisor_children_spec/0]).
-export([init_counters/0, init_report/0]).

-define(EXOMETER_COUNTERS,
    [save, update, create, create_or_update, get, delete, exists, add_links, 
		set_links, create_link, delete_links, fetch_link, foreach_link,
        mark_links_deleted, get_links, fold_links, get_links_trees
    ]).

-define(EXOMETER_NAME(Param), ?exometer_name(datastore, Param)).


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
    bounded_cache:init_group_manager(),
    datastore_config:init(),
    datastore_cache_manager:init(),
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
%% Initializes all counters.
%% @end
%%--------------------------------------------------------------------
-spec init_counters() -> ok.
init_counters() ->
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), counter}
    end, ?EXOMETER_COUNTERS),
    ?init_counters(Counters).

%%--------------------------------------------------------------------
%% @doc
%% Subscribe for reports for all parameters.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    Reports = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), [value]}
    end, ?EXOMETER_COUNTERS),
    ?init_reports(Reports).


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
