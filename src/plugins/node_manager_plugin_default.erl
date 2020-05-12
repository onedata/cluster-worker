%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Plugin which extends node manager.
%%% @end
%%%-------------------------------------------------------------------
-module(node_manager_plugin_default).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

-export([installed_cluster_generation/0]).
-export([oldest_known_cluster_generation/0]).
-export([app_name/0, cm_nodes/0, db_nodes/0]).
-export([renamed_models/0]).
-export([before_init/0]).
-export([upgrade_essential_workers/0]).
-export([upgrade_cluster/1]).
-export([custom_workers/0]).
-export([on_db_and_workers_ready/0]).
-export([listeners/0]).
-export([handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([clear_memory/1]).
-export([modules_with_exometer/0, exometer_reporters/0]).

-type model() :: datastore_model:model().
-type record_version() :: datastore_model:record_version().
-type state() :: term().

%%%===================================================================
%%% node_manager_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns the current cluster generation of this software.
%% @end
%%--------------------------------------------------------------------
-spec installed_cluster_generation() -> node_manager:cluster_generation().
installed_cluster_generation() ->
    1.

%%--------------------------------------------------------------------
%% @doc
%% Returns the oldest known generation - the lowest one that can be directly
%% upgraded to installed_cluster_generation.
%% @end
%%--------------------------------------------------------------------
-spec oldest_known_cluster_generation() ->
    {node_manager:cluster_generation(), HumanReadableVersion :: binary()}.
oldest_known_cluster_generation() ->
    {1, <<"19.02.*">>}.

%%--------------------------------------------------------------------
%% @doc
%% Returns the name of the application that bases on cluster worker.
%% @end
%%--------------------------------------------------------------------
-spec app_name() -> {ok, Name :: atom()}.
app_name() ->
    {ok, cluster_worker}.

%%--------------------------------------------------------------------
%% @doc
%% List cluster manager nodes to be used by node manager.
%% @end
%%--------------------------------------------------------------------
-spec cm_nodes() -> {ok, Nodes :: [atom()]} | undefined.
cm_nodes() ->
    application:get_env(?CLUSTER_WORKER_APP_NAME, cm_nodes).

%%--------------------------------------------------------------------
%% @doc
%% List db nodes to be used by node manager.
%% @end
%%--------------------------------------------------------------------
-spec db_nodes() -> {ok, Nodes :: [atom()]} | undefined.
db_nodes() ->
    application:get_env(?CLUSTER_WORKER_APP_NAME, db_nodes).

%%--------------------------------------------------------------------
%% @doc
%% Maps old model name to new one.
%% @end
%%--------------------------------------------------------------------
-spec renamed_models() -> #{{record_version(), model()} => model()}.
renamed_models() ->
    #{}.

%%--------------------------------------------------------------------
%% @doc
%% This callback is executed when node manager starts. At time
%% of invocation, node_manager is not set init'ed yet. Use to inject
%% custom initialisation.
%% This callback is executed on all cluster nodes.
%% @end
%%--------------------------------------------------------------------
-spec before_init() -> ok | {error, Reason :: term()}.
before_init() ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% List of workers modules with configs that should be started before upgrade.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_essential_workers() -> [{module(), list()}
| {module(), list(), [atom()]} | {singleton, module(), list()}].
upgrade_essential_workers() -> [].

%%--------------------------------------------------------------------
%% @doc
%% Upgrades cluster to newer generation. Should return new current generation.
%% This callback is executed only on one cluster node.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_cluster(node_manager:cluster_generation()) ->
    {ok, node_manager:cluster_generation()}.
upgrade_cluster(CurrentGeneration) ->
    {ok, CurrentGeneration + 1}.

%%--------------------------------------------------------------------
%% @doc
%% List of workers modules with configs to be loaded by node_manager.
%% @end
%%--------------------------------------------------------------------
-spec custom_workers() -> [{module(), list()}
| {module(), list(), [atom()]} | {singleton, module(), list()}].
custom_workers() -> [].

%%--------------------------------------------------------------------
%% @doc
%% This callback is executed when cluster has finished to initialize
%% (nagios has reported healthy status).
%% Use to run custom code required for application initialization that might
%% need working services (e.g. database).
%% This callback is executed on all cluster nodes.
%% @end
%%--------------------------------------------------------------------
-spec on_db_and_workers_ready() -> Result :: ok | {error, Reason :: term()}.
on_db_and_workers_ready() ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% List of listeners to be loaded by node_manager.
%% @end
%%--------------------------------------------------------------------
-spec listeners() -> Listeners :: [atom()].
listeners() -> [
    nagios_listener
].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles call messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: state()) ->
    {reply, Reply :: term(), NewState :: state()} |
    {reply, Reply :: term(), NewState :: state(), timeout() | hibernate} |
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: state()} |
    {stop, Reason :: term(), NewState :: state()}.
handle_call(Request, _From, State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_cast(Request, State) ->
    ?log_bad_request(Request),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles all non call/cast messages.
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: state()) ->
    {noreply, NewState :: state()} |
    {noreply, NewState :: state(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: state()}.
handle_info(Info, State) ->
    ?log_bad_request(Info),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: state()) -> term().
terminate(_Reason, State) ->
    State.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts process state when code is changed.
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()}, State :: state(),
    Extra :: term()) -> {ok, NewState :: state()} | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @doc
%% Clears memory of node. HighMemUse is true when memory clearing is
%% started because of high memory usage by node. When it is periodic memory
%% cleaning HighMemUse is false.
%% @end
%%--------------------------------------------------------------------
-spec clear_memory(HighMemUse :: boolean()) -> ok.
clear_memory(_HighMemUse) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of modules that register exometer reporters.
%% @end
%%--------------------------------------------------------------------
-spec modules_with_exometer() -> list().
modules_with_exometer() ->
  [].

%%--------------------------------------------------------------------
%% @doc
%% Returns list of exometer reporters.
%% @end
%%--------------------------------------------------------------------
-spec exometer_reporters() -> list().
exometer_reporters() ->
  [].

