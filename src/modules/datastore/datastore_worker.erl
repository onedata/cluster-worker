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

-include("global_definitions.hrl").
-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("exometer_utils.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([supervisor_flags/0, supervisor_children_spec/0]).
-export([init_counters/0, init_report/0]).
-export([check_db_connection/0, get_application_closing_status/0]).

-define(EXOMETER_COUNTERS,
    [save, update, create, create_or_update, get, delete, exists, add_links, check_and_add_links,
		set_links, create_link, delete_links, fetch_link, foreach_link,
        mark_links_deleted, get_links, fold_links, get_links_trees
    ]).

-define(EXOMETER_NAME(Param), ?exometer_name(datastore, Param)).
-define(SAVE_CHECK_INTERVAL, timer:seconds(5)).
-define(APPLICATION_CLOSING_STATUS_ENV, application_closing_status).

-type closing_status() :: ?CLOSING_PROCEDURE_SUCCEEDED | ?CLOSING_PROCEDURE_FAILED | undefined.

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
    ?info("Attempting to gracefully stop in-memory database - persisting documents..."),
    wait_for_database_flush(),
    ?info("All in-memory documents persisted on disk, saving information about graceful shutdown"),
    persist_application_closing_info().

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

%%--------------------------------------------------------------------
%% @doc
%% Waits until db is ready to save documents.
%% @end
%%--------------------------------------------------------------------
-spec check_db_connection() -> ok.
check_db_connection() ->
    ?info("Waiting for database readiness..."),
    check_db_read_and_set_application_closing_status(),
    check_db_write_and_persist_initial_info(),
    ?info("Database ready").

-spec get_application_closing_status() -> closing_status().
get_application_closing_status() ->
    node_cache:get(?APPLICATION_CLOSING_STATUS_ENV, undefined).

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

-spec wait_for_database_flush() -> ok.
wait_for_database_flush() ->
    wait_for_database_flush(couchbase_config:get_flush_queue_size()).

-spec wait_for_database_flush(non_neg_integer()) -> ok.
wait_for_database_flush(0) ->
    ok;
wait_for_database_flush(Size) ->
    ?info("Waiting for couchbase to flush documents, current queue size ~p", [Size]),
    timer:sleep(5000),
    wait_for_database_flush(couchbase_config:get_flush_queue_size()).

-spec check_db_read_and_set_application_closing_status() -> ok.
check_db_read_and_set_application_closing_status() ->
    {ok, Value} = db_action_loop(fun read_application_closing_status/0, "Database not ready yet..."),
    set_application_closing_status(Value).

check_db_write_and_persist_initial_info() ->
    db_action_loop(fun() -> perform_test_db_write(?TEST_DOC_INIT_VALUE) end, "Database not ready yet..."),
    wait_for_database_flush().

persist_application_closing_info() ->
    db_action_loop(fun() -> perform_test_db_write(?TEST_DOC_FINAL_VALUE) end,
        "Waiting to save information about application closing"),
    wait_for_database_flush().

-spec db_action_loop(fun(() -> {ok, datastore:doc() | undefined} | {error, term()}), string()) ->
    {ok, datastore:doc() | undefined}.
db_action_loop(Operation, InfoLog) ->
    case Operation() of
        {ok, _} = OkAns ->
            OkAns;
        Error ->
            ?debug("Db action ~p failed with error ~p", [Operation, Error]),
            ?info(InfoLog),
            timer:sleep(?SAVE_CHECK_INTERVAL),
            db_action_loop(Operation, InfoLog)
    end.

-spec read_application_closing_status() -> {ok, datastore:doc() | undefined} | {error, term()}.
read_application_closing_status() ->
    Ctx = datastore_model_default:get_default_disk_ctx(),
    case couchbase_driver:get(Ctx, ?TEST_DOC_KEY) of
        {ok, _, #document{} = Doc} ->
            {ok, Doc};
        {error, not_found} ->
            % First start of the application - closing status does not exist
            {ok, undefined};
        Error ->
            Error
    end.

-spec set_application_closing_status(datastore:doc() | undefined) -> ok.
set_application_closing_status(#document{value = ?TEST_DOC_INIT_VALUE}) ->
    node_cache:put(?APPLICATION_CLOSING_STATUS_ENV, ?CLOSING_PROCEDURE_FAILED);
set_application_closing_status(#document{value = ?TEST_DOC_FINAL_VALUE}) ->
    node_cache:put(?APPLICATION_CLOSING_STATUS_ENV, ?CLOSING_PROCEDURE_SUCCEEDED);
set_application_closing_status(_) ->
    ok.

-spec perform_test_db_write(binary()) -> {ok, datastore:doc()} | {error, term()}.
perform_test_db_write(Value) ->
    Key = ?TEST_DOC_KEY,
    Ctx = datastore_model_default:get_default_disk_ctx(),
    TestDoc = #document{key = Key, value = Value},
    case couchbase_driver:save(Ctx#{no_seq => true}, Key, TestDoc) of
        {ok, _, #document{} = Doc} ->
            {ok, Doc};
        Error ->
            Error
    end.