%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions to manipulate safe mode in the cluster.
%%% There are 3 possible states of safe mode:
%%%     * awaiting_cluster_init - implicit state (safe mode is implicitly enabled),
%%%         before the cluster is ready and fully operational
%%%         (it is possible to whitelist PIDs that are excluded from safe mode limitations);
%%%     * disabled - safe mode is disabled;
%%%     * enabled_manually - manually activated by cluster admin.
%%% @end
%%%-------------------------------------------------------------------
-module(safe_mode).
-author("Michal Stanisz").

% API
-export([enable_manually/0, disable_manually/0, report_node_initialized/0]).
-export([should_enforce/0, should_enforce_for_pid/1, whitelist_pid/1]).

% RPC API
-export([enable_manually_on_current_node/0, disable_manually_on_current_node/0]).

-define(SAFE_MODE_ENV, safe_mode).
-define(SAFE_MODE_WHITELIST_CACHE, safe_mode_whitelist_cache).

-define(AWAITING_CLUSTER_INIT_STATE, awaiting_cluster_init).
-define(MANUALLY_ENABLED_STATE, manually_enabled).
-define(DISABLED_STATE, disabled).

-type state() :: ?AWAITING_CLUSTER_INIT_STATE | ?DISABLED_STATE | ?MANUALLY_ENABLED_STATE.

%%%===================================================================
%%% API
%%%===================================================================

-spec enable_manually() -> ok | error.
enable_manually() ->
    rpc_all_nodes(enable_manually_on_current_node).


-spec disable_manually() -> ok.
disable_manually() ->
    rpc_all_nodes(disable_manually_on_current_node).


-spec report_node_initialized() -> ok.
report_node_initialized() ->
    case get_state() of
        ?AWAITING_CLUSTER_INIT_STATE ->
            disable_on_current_node();
        _ ->
            ok
    end.


-spec should_enforce() -> boolean().
should_enforce() ->
    get_state() =/= ?DISABLED_STATE.


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

-spec enable_manually_on_current_node() -> ok.
enable_manually_on_current_node() ->
    cluster_worker:set_env(?SAFE_MODE_ENV, ?MANUALLY_ENABLED_STATE).


-spec disable_manually_on_current_node() -> ok | error.
disable_manually_on_current_node() ->
    case get_state() of
        ?AWAITING_CLUSTER_INIT_STATE ->
            error;
        _ ->
            disable_on_current_node()
    end.


%%%===================================================================
%%% API
%%%===================================================================

%% @private
-spec get_state() -> state().
get_state() ->
    cluster_worker:get_env(?SAFE_MODE_ENV, ?AWAITING_CLUSTER_INIT_STATE).


%% @private
-spec rpc_all_nodes(atom()) -> ok.
rpc_all_nodes(Function) ->
    lists:foreach(fun(Node) ->
        ok = rpc:call(Node, ?MODULE, Function, [])
    end, consistent_hashing:get_all_nodes()).


%% @private
-spec disable_on_current_node() -> ok.
disable_on_current_node() ->
    node_cache:clear(?SAFE_MODE_WHITELIST_CACHE),
    cluster_worker:set_env(?SAFE_MODE_ENV, ?DISABLED_STATE).
