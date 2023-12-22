%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2023 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions to manipulate safe mode in the cluster.
%%% @end
%%%-------------------------------------------------------------------
-module(safe_mode).
-author("Michal Stanisz").

-export([enable/0, disable/0, disable_if_not_set/0, is_enabled/0]).
-export([is_pid_allowed/1, whitelist_pid/1]).

-define(SAVE_MODE_ENV, save_mode).
-define(SAVE_MODE_WHITELIST_CACHE, save_mode_whitelist_cache).

%%%===================================================================
%%% API
%%%===================================================================

-spec enable() -> ok.
enable() ->
    cluster_worker:set_env(?SAVE_MODE_ENV, true).


-spec disable() -> ok.
disable() ->
    node_cache:clear(?SAVE_MODE_WHITELIST_CACHE),
    cluster_worker:set_env(?SAVE_MODE_ENV, false).


-spec disable_if_not_set() -> ok.
disable_if_not_set() ->
    case cluster_worker:get_env(?SAVE_MODE_ENV, not_set) of
        not_set ->
            disable();
        _ ->
            ok
    end.


-spec is_enabled() -> boolean().
is_enabled() ->
    cluster_worker:get_env(?SAVE_MODE_ENV, true).


-spec is_pid_allowed(pid()) -> boolean().
is_pid_allowed(Pid) ->
    case is_enabled() of
        false ->
            true;
        true ->
            lists:member(Pid, node_cache:get(?SAVE_MODE_WHITELIST_CACHE, []))
    end.


-spec whitelist_pid(pid()) -> ok.
whitelist_pid(Pid) ->
    {ok, _} = node_cache:update(?SAVE_MODE_WHITELIST_CACHE, fun(PrevList) ->
        {ok, [Pid | PrevList], infinity}
    end, [Pid]),
    ok.