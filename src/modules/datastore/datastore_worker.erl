%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc datastore worker's implementation
%%% @end
%%%--------------------------------------------------------------------
-module(datastore_worker).
-author("Rafal Slota").

-behaviour(worker_plugin_behaviour).

-include("global_definitions.hrl").
-include("timeouts.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).
-export([state_get/1, state_put/2]).
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
    DBNodes = datastore_config:db_nodes(),

    State = #{db_nodes => DBNodes},
    PersistenceDriverMod = datastore:driver_to_module(?PERSISTENCE_DRIVER),
    PersistenceDriverMod:init_driver(State),
    ?LOCAL_SLAVE_DRIVER:init_driver(State),
    ?GLOBAL_SLAVE_DRIVER:init_driver(State),

    State2 = lists:foldl(fun(Model, StateAcc) ->
        #model_config{name = RecordName} = ModelConfig = Model:model_init(),
        maps:put(RecordName, ModelConfig, StateAcc)
    end, State, datastore_config:models()),

    {ok, State2}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(Request) -> Result when
    Request :: ping | healthcheck |
    {driver_call, Module :: atom(), Method :: atom(), Args :: [term()]},
    Result :: nagios_handler:healthcheck_response() | ok | pong | {ok, Response} |
    {error, Reason},
    Response :: term(),
    Reason :: term().
handle(ping) ->
    pong;

handle(healthcheck) ->
    State = worker_host:state_to_map(?MODULE),
    PersistenceModule = datastore:driver_to_module(?PERSISTENCE_DRIVER),

    lists:foldl(fun
        (_, {error, Reason}) ->
            {error, Reason};
        ({_Driver, ok}, ok) ->
            ok;
        ({Driver, {error, Reason}}, ok) ->
            ?error("Driver ~p healthcheck error: ~p", [Driver, Reason]),
            {error, {Driver, Reason}};
        ({Driver, Error}, ok) ->
            ?error("Driver ~p unexpected healthcheck error: ~p", [Driver, Error]),
            {error, {Driver, Error}}
    end, ok, [
        {datastore_state_init, datastore:healthcheck()},
        {?PERSISTENCE_DRIVER, catch PersistenceModule:healthcheck(State)},
        {?LOCAL_SLAVE_DRIVER, catch ?LOCAL_SLAVE_DRIVER:healthcheck(State)},
        {?GLOBAL_SLAVE_DRIVER, catch ?GLOBAL_SLAVE_DRIVER:healthcheck(State)}
    ]);

handle({driver_call, Module, Method, Args}) ->
    try erlang:apply(Module, Method, Args) of
        ok -> ok;
        {ok, Response} -> {ok, Response};
        {error, Reason} -> {error, Reason}
    catch
        _:Reason ->
            ?error_stacktrace("datastore request ~p failed due to ~p",
                [{Module, Method, Args}, Reason]),
            {error, Reason}
    end;

handle(_Request) ->
    ?log_bad_request(_Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok.
cleanup() ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Stores key-value pair in the datastore worker's state.
%% @end
%%--------------------------------------------------------------------
-spec state_put(Key :: term(), Value :: term()) -> ok.
state_put(Key, Value) ->
    worker_host:state_put(?MODULE, Key, Value).

%%--------------------------------------------------------------------
%% @doc
%% Returns value associated with a key from the datastore worker's state.
%% @end
%%--------------------------------------------------------------------
-spec state_get(Key :: term()) -> Value :: term().
state_get(Key) ->
    worker_host:state_get(?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns a datastore supervisor flags.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => one_for_one, intensity => 1, period => 5}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a children spec for a datastore supervisor.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_children_spec() -> [supervisor:child_spec()].
supervisor_children_spec() ->
    [
        #{
            id => couchbase_gateway_sup,
            start => {couchbase_gateway_sup, start_link, []},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [couchbase_gateway_sup]
        },
        #{
            id => datastore_pool,
            start => {datastore_pool, start_link, []},
            restart => permanent,
            shutdown => timer:seconds(10),
            type => worker,
            modules => [datastore_pool]
        },
        #{
            id => datastore_pool_sup,
            start => {datastore_pool_sup, start_link, []},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [datastore_pool_sup]
        }
    ].