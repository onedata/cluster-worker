%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements {@link worker_plugin_behaviour} and is responsible
%%% for mapping transaction process key to a pid of a handling process
%%% (tp_server).
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
-export([create/2, get/1, delete/1, delete/2, size/0]).
-export([supervisor_flags/0, supervisor_children_spec/0,
    main_supervisor_flags/0, main_supervisor_children_spec/0]).

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
    ets:new(?TP_ROUTING_TABLE, [
        set,
        public,
        named_table,
        {read_concurrency, true}
    ]),

    lists:foreach(fun(Name) ->
        {ok, _} = supervisor:start_child(
            ?TP_ROUTER_SUP,
            {Name, {tp_subtree_supervisor, start_link, [Name]},
                transient, infinity, supervisor, [tp_subtree_supervisor]}
        )
    end, datastore_multiplier:get_names(?TP_ROUTER_SUP)),

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
%% Adds routing entry if missing and if it does not exceed the limit
%% for the number of active processes.
%% @end
%%--------------------------------------------------------------------
-spec create(tp:key(), tp:server()) -> ok | {error, Reason :: term()}.
create(Key, Pid) ->
    case update_size(1) > tp:get_processes_limit() of
        true ->
            update_size(-1),
            {error, limit_exceeded};
        false ->
            case ets:insert_new(?TP_ROUTING_TABLE, {Key, Pid}) of
                true -> ok;
                false ->
                    update_size(-1),
                    {error, already_exists}
            end
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
%% Deletes routing key.
%% @end
%%--------------------------------------------------------------------
-spec delete(tp:key()) -> ok.
delete(Key) ->
    ets:delete(?TP_ROUTING_TABLE, Key),
    update_size(-1),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Deletes routing entry.
%% @end
%%--------------------------------------------------------------------
-spec delete(tp:key(), tp:server()) -> ok.
delete(Key, Pid) ->
    case ets:select_delete(?TP_ROUTING_TABLE, [{{Key, Pid}, [], [true]}]) of
        0 -> ok;
        1 -> update_size(-1)
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns routing table size.
%% @end
%%--------------------------------------------------------------------
-spec size() -> Size :: non_neg_integer().
size() ->
    case ets:lookup(?TP_ROUTING_TABLE, ?TP_ROUTING_TABLE_SIZE) of
        [] -> 0;
        [{?TP_ROUTING_TABLE_SIZE, Size}] -> Size
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns a tp_router supervisor flags.
%% @end
%%--------------------------------------------------------------------
-spec main_supervisor_flags() -> supervisor:sup_flags().
main_supervisor_flags() ->
    #{strategy => one_for_one, intensity => 1, period => 5}.

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
-spec main_supervisor_children_spec() -> [supervisor:child_spec()].
main_supervisor_children_spec() ->
    [].

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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates the routing table size by a difference.
%% @end
%%--------------------------------------------------------------------
-spec update_size(integer()) -> integer().
update_size(Diff) ->
    ets:update_counter(?TP_ROUTING_TABLE, ?TP_ROUTING_TABLE_SIZE,
        {2, Diff}, {?TP_ROUTING_TABLE_SIZE, 0}).
