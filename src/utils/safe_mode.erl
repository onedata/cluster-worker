%%%-------------------------------------------------------------------
%%% @author Michal Stanisz, Lukasz Opiola
%%% @copyright (C) 2023-2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions to manipulate safe mode in the cluster.
%%% Enabled safe mode denotes that the cluster is in init or maintenance
%%% state and should not handle requests. NOTE that it's merely a indicator
%%% and proper mechanisms MUST be implemented in all relevant places to
%%% read the safe mode state and react properly.
%%%
%%% NOTE: safe mode is tracked CLUSTER-WIDE; it cannot be enabled or disabled
%%% for a single node. Since cluster-worker-based apps work in distributed manner,
%%% it does not make sense to have different safe mode settings on different nodes.
%%%
%%% There are 3 possible states of safe mode:
%%%     * AWAITING_CLUSTER_INIT_STATE - implicit state (safe mode is implicitly enabled),
%%%         before the cluster is ready and fully operational
%%%         (it is possible to whitelist PIDs that are excluded from safe mode limitations).
%%%     * NODE_INITIALIZED_STATE - the node where this is set has been initialized, but
%%%         the whole cluster has not; it will happen when all nodes transition to this state.
%%%     * DISABLED_STATE - the safe mode is disabled, denotes that the cluster is fully
%%%         operation and can handle requests. The transition to this state happens
%%%         automatically on cluster init when all nodes enter NODE_INITIALIZED_STATE.
%%%     * MANUALLY_ENABLED_STATE - manually activated by cluster admin. CANNOT be activated
%%%         for a non-fully-initialized cluster.
%%% @end
%%%-------------------------------------------------------------------
-module(safe_mode).
-author("Michal Stanisz").
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").


% API
-export([enable_manually/0, disable_manually/0, report_node_initialized/0]).
-export([should_enforce/0, should_enforce_for_pid/1, whitelist_pid/1]).

% RPC API
-export([transition_on_current_node/1, get_state_on_current_node/0]).


-define(SAFE_MODE_ENV, safe_mode).
-define(SAFE_MODE_WHITELIST_CACHE, safe_mode_whitelist_cache).

-define(AWAITING_CLUSTER_INIT_STATE, awaiting_cluster_init).
-define(NODE_INITIALIZED_STATE, node_initialized_state).
-define(DISABLED_STATE, disabled).
-define(MANUALLY_ENABLED_STATE, manually_enabled).

-type state() :: ?AWAITING_CLUSTER_INIT_STATE
               | ?NODE_INITIALIZED_STATE
               | ?DISABLED_STATE
               | ?MANUALLY_ENABLED_STATE.


-define(safe_mode_info_log(Msg), ?safe_mode_info_log(Msg, [])).
-define(safe_mode_info_log(Fmt, Args), ?info("Safe mode: " ++ Fmt, Args)).


%%%===================================================================
%%% API
%%%===================================================================


-spec enable_manually() -> ok | error.
enable_manually() ->
    transition_on_all_nodes(?MANUALLY_ENABLED_STATE).


-spec disable_manually() -> ok.
disable_manually() ->
    transition_on_all_nodes(?DISABLED_STATE).


%% @doc idempotent; does not have an effect if the node has already been reported as initialized
-spec report_node_initialized() -> ok.
report_node_initialized() ->
    % avoid races when nodes transition in the same time
    critical_section:run(?MODULE, fun() ->
        case get_state_on_current_node() of
            ?AWAITING_CLUSTER_INIT_STATE ->
                report_node_initialized_unsafe();
            _ ->
                ok
        end
    end).


-spec should_enforce() -> boolean().
should_enforce() ->
    get_state_on_current_node() =/= ?DISABLED_STATE.


-spec should_enforce_for_pid(pid()) -> boolean().
should_enforce_for_pid(Pid) ->
    case should_enforce() of
        false ->
            false;
        true ->
            not lists:member(Pid, node_cache:get(?SAFE_MODE_WHITELIST_CACHE, []))
    end.


-spec whitelist_pid(pid()) -> ok.
whitelist_pid(Pid) ->
    {ok, _} = node_cache:update(?SAFE_MODE_WHITELIST_CACHE, fun(PrevList) ->
        {ok, [Pid | PrevList], infinity}
    end, [Pid]),
    ok.


%%%===================================================================
%%% RPC API
%%%===================================================================


-spec transition_on_current_node(state()) -> ok | no_return().
transition_on_current_node(To) ->
    From = get_state_on_current_node(),
    case transition_allowed(From, To) of
        true ->
            ?safe_mode_info_log("transitioned from ~ts to ~ts", [From, To]),
            set_state_on_current_node(To);
        false ->
            error({disallowed_safe_mode_transition, From, To})
    end.


-spec get_state_on_current_node() -> state().
get_state_on_current_node() ->
    cluster_worker:get_env(?SAFE_MODE_ENV, ?AWAITING_CLUSTER_INIT_STATE).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec transition_on_all_nodes(state()) -> ok | no_return().
transition_on_all_nodes(To) ->
    lists:foreach(fun(Node) ->
        ok = rpc:call(Node, ?MODULE, transition_on_current_node, [To])
    end, consistent_hashing:get_all_nodes()).


%% @private
-spec report_node_initialized_unsafe() -> ok.
report_node_initialized_unsafe() ->
    transition_on_current_node(?NODE_INITIALIZED_STATE),
    case all_nodes_initialized() of
        true ->
            ?safe_mode_info_log("all nodes initialized; disabling safe mode"),
            transition_on_all_nodes(?DISABLED_STATE);
        false ->
            ?safe_mode_info_log("some cluster nodes are still not initialized")
    end.


%% @private
-spec all_nodes_initialized() -> boolean().
all_nodes_initialized() ->
    lists:all(fun(Node) ->
        rpc:call(Node, ?MODULE, get_state_on_current_node, []) == ?NODE_INITIALIZED_STATE
    end, consistent_hashing:get_all_nodes()).


%% @private
-spec set_state_on_current_node(state()) -> ok.
set_state_on_current_node(State) ->
    State == ?DISABLED_STATE andalso node_cache:clear(?SAFE_MODE_WHITELIST_CACHE),
    cluster_worker:set_env(?SAFE_MODE_ENV, State).


%% @private
-spec transition_allowed(state(), state()) -> boolean().
transition_allowed(?AWAITING_CLUSTER_INIT_STATE, ?NODE_INITIALIZED_STATE) -> true;
transition_allowed(?AWAITING_CLUSTER_INIT_STATE, _) -> false;
transition_allowed(?NODE_INITIALIZED_STATE, ?DISABLED_STATE) -> true;
transition_allowed(?NODE_INITIALIZED_STATE, _) -> false;
transition_allowed(?DISABLED_STATE, ?MANUALLY_ENABLED_STATE) -> true;
transition_allowed(?DISABLED_STATE, _) -> false;
transition_allowed(?MANUALLY_ENABLED_STATE, ?DISABLED_STATE) -> true;
transition_allowed(?MANUALLY_ENABLED_STATE, _) -> false.
