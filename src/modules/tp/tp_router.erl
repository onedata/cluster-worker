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
-export([create/2, report_process_initialized/2, get/1, get_initialized/1, delete/1, delete/2, size/0]).
-export([update_process_size/2, delete_process_size/1, get_process_size_sum/0]).
-export([supervisor_flags/0, supervisor_children_spec/0,
    main_supervisor_flags/0, main_supervisor_children_spec/0,
    init_supervisors/0]).
-export([send_to_each/1]).

% TP process states
-define(INITIALIZING, initializing).
-define(INITIALIZED, initialized).

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
    lists:foreach(fun(Name) ->
        ets:new(Name, [
        set,
        public,
        named_table,
        {read_concurrency, true}
        ])
    end, datastore_multiplier:get_names(?TP_ROUTING_TABLE)),

    lists:foreach(fun(Name) ->
        ets:new(Name, [
            set,
            public,
            named_table,
            {read_concurrency, true}
        ])
    end, datastore_multiplier:get_names(?TP_SIZE_TABLE)),

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
    lists:foreach(fun(Name) ->
        ets:delete(Name)
    end, datastore_multiplier:get_names(?TP_ROUTING_TABLE)),

    lists:foreach(fun(Name) ->
        ets:delete(Name)
    end, datastore_multiplier:get_names(?TP_SIZE_TABLE)),
    ok.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes subtrees supervisors.
%% @end
%%--------------------------------------------------------------------
-spec init_supervisors() -> ok.
init_supervisors() ->
    lists:foreach(fun(Name) ->
        case supervisor:start_child(
            ?TP_ROUTER_SUP,
            {Name, {tp_subtree_supervisor, start_link, [Name]},
                transient, infinity, supervisor, [tp_subtree_supervisor]}
        ) of
            {ok, _} -> ok;
            {error, {already_started, _}} ->
                ?debug("Tp supervisor ~p already started", [Name]),
                ok
        end
    end, datastore_multiplier:get_names(?TP_ROUTER_SUP)).

%%--------------------------------------------------------------------
%% @doc
%% Adds routing entry if missing and if it does not exceed the limit
%% for the number of active processes.
%% @end
%%--------------------------------------------------------------------
-spec create(tp:key(), tp:server()) -> ok | {error, Reason :: term()}.
create(Key, Pid) ->
    Table = datastore_multiplier:extend_name(Key, ?TP_ROUTING_TABLE),
    case update_size(Table, 1) > tp:get_processes_limit() of
        true ->
            update_size(Table, -1),
            {error, limit_exceeded};
        false ->
            case ets:insert_new(Table, {Key, Pid, ?INITIALIZING}) of
                true -> ok;
                false ->
                    update_size(Table, -1),
                    {error, already_exists}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Adds information that transaction process server is initialized.
%% @end
%%--------------------------------------------------------------------
-spec report_process_initialized(tp:key(), tp:server()) -> ok.
report_process_initialized(Key, Pid) ->
    Table = datastore_multiplier:extend_name(Key, ?TP_ROUTING_TABLE),
    ets:insert(Table, {Key, Pid, ?INITIALIZED}),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns pid of a transaction process server associated with provided key.
%% @end
%%--------------------------------------------------------------------
-spec get(tp:key()) -> {ok, tp:server()} | {error, not_found}.
get(Key) ->
    Table = datastore_multiplier:extend_name(Key, ?TP_ROUTING_TABLE),
    case ets:lookup(Table, Key) of
        [] -> {error, not_found};
        [{Key, Pid, _}] -> {ok, Pid}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns pid of a transaction process server associated with provided key.
%% If process is not initialized returns not_found.
%% @end
%%--------------------------------------------------------------------
-spec get_initialized(tp:key()) -> {ok, tp:server()} | {error, not_found}.
get_initialized(Key) ->
    Table = datastore_multiplier:extend_name(Key, ?TP_ROUTING_TABLE),
    case ets:lookup(Table, Key) of
        [{Key, Pid, ?INITIALIZED}] -> {ok, Pid};
        _ -> {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes routing key.
%% @end
%%--------------------------------------------------------------------
-spec delete(tp:key()) -> ok.
delete(Key) ->
    Table = datastore_multiplier:extend_name(Key, ?TP_ROUTING_TABLE),
    ets:delete(Table, Key),
    update_size(Table, -1),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Deletes routing entry.
%% @end
%%--------------------------------------------------------------------
-spec delete(tp:key(), tp:server()) -> ok.
delete(Key, Pid) ->
    Table = datastore_multiplier:extend_name(Key, ?TP_ROUTING_TABLE),
    case ets:select_delete(Table, [{{Key, Pid, '_'}, [], [true]}]) of
        0 -> ok;
        1 -> update_size(Table, -1)
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns largest routing table size.
%% @end
%%--------------------------------------------------------------------
-spec size() -> Size :: non_neg_integer().
size() ->
    lists:foldl(fun(Name, Acc) ->
        case ets:lookup(Name, ?TP_ROUTING_TABLE_SIZE) of
            [] -> Acc;
            [{?TP_ROUTING_TABLE_SIZE, Size}] -> max(Size, Acc)
        end
    end, 0, datastore_multiplier:get_names(?TP_ROUTING_TABLE)).

%%--------------------------------------------------------------------
%% @doc
%% Returns a tp_router main supervisor flags.
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
%% Returns a children spec for a main tp_router supervisor.
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

%%--------------------------------------------------------------------
%% @doc
%% Inserts size of tp process.
%% @end
%%--------------------------------------------------------------------
-spec update_process_size(pid(), non_neg_integer()) -> true.
update_process_size(Proc, Size) ->
    Table = datastore_multiplier:extend_name(Proc, ?TP_SIZE_TABLE),
    ets:insert(Table, {Proc, Size}).

%%--------------------------------------------------------------------
%% @doc
%% Deletes size of tp process for ets.
%% @end
%%--------------------------------------------------------------------
-spec delete_process_size(pid()) -> true.
delete_process_size(Proc) ->
    Table = datastore_multiplier:extend_name(Proc, ?TP_SIZE_TABLE),
    ets:delete(Table, Proc).

%%--------------------------------------------------------------------
%% @doc
%% Returns sum of sizes of all processes.
%% @end
%%--------------------------------------------------------------------
-spec get_process_size_sum() -> non_neg_integer().
get_process_size_sum() ->
    lists:foldl(fun(Name, Acc) ->
        List = ets:tab2list(Name),
        lists:sum(lists:map(fun({_K, V}) -> V end, List)) + Acc
    end, 0, datastore_multiplier:get_names(?TP_SIZE_TABLE)).

%%--------------------------------------------------------------------
%% @doc
%% Sends message to all tp processes.
%% @end
%%--------------------------------------------------------------------
-spec send_to_each(term()) -> ok.
send_to_each(Msg) ->
    lists:foreach(fun(Name) ->
        List = ets:tab2list(Name),
        lists:foreach(fun
            ({_, Pid, _}) -> catch gen_server:call(Pid, Msg); % Catch in case of process termination
            (_) -> ok
        end, List)
    end, datastore_multiplier:get_names(?TP_ROUTING_TABLE)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates the routing table size by a difference.
%% @end
%%--------------------------------------------------------------------
-spec update_size(atom(), integer()) -> integer().
update_size(Table, Diff) ->
    ets:update_counter(Table, ?TP_ROUTING_TABLE_SIZE,
        {2, Diff}, {?TP_ROUTING_TABLE_SIZE, 0}).
