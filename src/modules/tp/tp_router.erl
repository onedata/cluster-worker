%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements {@link worker_plugin_behaviour} and is responsible
%%% for mapping transaction process key to a pid of a handling process (tp_server).
%%% @end
%%%-------------------------------------------------------------------
-module(tp_router).
-author("Krzysztof Trzepla").

-behaviour(worker_plugin_behaviour).

-include("modules/tp/tp.hrl").
-include_lib("ctool/include/logging.hrl").

%% worker_plugin_behaviour callbacks
-export([init/1, handle/1, cleanup/0]).

%% API
-export([create/2, get/1, delete/2]).
-export([supervisor_flags/0, supervisor_children_spec/0]).

%%%===================================================================
%%% worker_plugin_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback init/1.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> Result when
    Result :: {ok, State :: worker_host:plugin_state()} | {error, Reason :: term()}.
init(_Args) ->
    ets:new(?TP_ROUTING_TABLE, [
        set,
        public,
        named_table,
        {read_concurrency, true}
    ]),
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback handle/1.
%% @end
%%--------------------------------------------------------------------
-spec handle(ping | healthcheck) -> pong | ok.
handle(ping) ->
    pong;

handle(healthcheck) ->
    ok;

handle(_Request) ->
    ?log_bad_request(_Request).

%%--------------------------------------------------------------------
%% @doc
%% {@link worker_plugin_behaviour} callback cleanup/0
%% @end
%%--------------------------------------------------------------------
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
cleanup() ->
    ets:delete(?TP_ROUTING_TABLE),
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds routing entry if missing.
%% @end
%%--------------------------------------------------------------------
-spec create(tp:key(), tp:server()) -> ok | {error, already_exists}.
create(Key, Pid) ->
    case ets:insert_new(?TP_ROUTING_TABLE, {Key, Pid}) of
        true -> ok;
        false -> {error, already_exists}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns pid of a transaction process server associated with provided key.
%% @end
%%--------------------------------------------------------------------
-spec get(tp:key()) -> {ok, tp:server()} | {error, not_found}.
get(Key) ->
    case ets:lookup(?TP_ROUTING_TABLE, Key) of
        [] -> {error, not_found};
        [{Key, Pid}] -> {ok, Pid}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes routing entry.
%% @end
%%--------------------------------------------------------------------
-spec delete(tp:key(), tp:server()) -> ok.
delete(Key, Pid) ->
    ets:delete_object(?TP_ROUTING_TABLE, {Key, Pid}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns a tp_router supervisor flags.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_flags() -> supervisor:sup_flags().
supervisor_flags() ->
    #{strategy => simple_one_for_one, intensity => 1, period => 5}.

%%--------------------------------------------------------------------
%% @doc
%% Returns a children spec for a tp_router supervisor.
%% @end
%%--------------------------------------------------------------------
-spec supervisor_children_spec() -> [supervisor:child_spec()].
supervisor_children_spec() ->
    [#{
        id => tp_server,
        start => {tp_server, start_link, []},
        restart => transient,
        shutdown => infinity,
        type => worker,
        modules => [tp_server]
    }].