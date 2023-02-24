%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for routing datastore function calls
%%% to designated nodes and modules.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_router).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").
-include("global_definitions.hrl").
-include("exometer_utils.hrl").
-include_lib("ctool/include/hashing/consistent_hashing.hrl").

%% API
-export([route/2, route_time_series_collection_operation/2, route_infinite_log_operation/2, execute_on_node/4]).
-export([init_counters/0, init_report/0]).
-export([get_routing_key/2]).
%% Internal RPC API
-export([execute_on_local_node/4, process/3]).

-type local_read() :: boolean(). % true if read should be tried locally before delegation to chosen node

-define(EXOMETER_COUNTERS,
    [save, update, create_backup, create, create_or_update, get, delete, exists, add_links, check_and_add_links,
        set_links, create_link, delete_links, fetch_link, foreach_link,
        mark_links_deleted, get_links, fold_links, get_links_trees, unset_link_ignore_in_changes
    ]).
-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE, Param)).

% Macros used when node is down and request is routed again
-define(RETRY_COUNT, application:get_env(?CLUSTER_WORKER_APP_NAME, datastore_router_retry_count, 5)).
-define(RETRY_SLEEP_BASE, application:get_env(?CLUSTER_WORKER_APP_NAME, datastore_router_retry_sleep_base, 100)).

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
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), [value]}
    end, ?EXOMETER_COUNTERS),
    ?init_reports(Counters).

%%--------------------------------------------------------------------
%% @doc
%% Redirects datastore call to designated node and module.
%% @end
%%--------------------------------------------------------------------
-spec route(atom(), list()) -> term().
route(Function, [Ctx | Args] = OriginalArgs) ->
    Module = select_module(Function),
    case route_internal(Module, Function, Ctx, Args) of
        {error, nodedown} -> retry_route(Module, Function, OriginalArgs, ?RETRY_SLEEP_BASE, ?RETRY_COUNT);
        Ans -> Ans
    end.

-spec route_time_series_collection_operation(atom(), list()) -> term().
route_time_series_collection_operation(Function, [Ctx | Args]) when
    % TODO VFS-8540 - handle race with metric adding/deletion
    % Function =:= get_slice orelse
    Function =:= get_layout ->
    route_internal(datastore_reader, time_series_collection_get, Ctx, [Function, Args]);
route_time_series_collection_operation(Function, [Ctx | Args]) ->
    route_internal(datastore_writer, time_series_collection_operation, Ctx, [Function, Args]).

-spec route_infinite_log_operation(atom(), list()) -> term().
route_infinite_log_operation(list, [Ctx | Args]) ->
    route_internal(datastore_reader, infinite_log_operation, Ctx, [list, Args]);
route_infinite_log_operation(Function, [Ctx | Args]) ->
    route_internal(datastore_writer, infinite_log_operation, Ctx, [Function, Args]).

%%--------------------------------------------------------------------
%% @doc
%% Processes call and throttles datastore request if necessary.
%% @end
%%--------------------------------------------------------------------
-spec process(module(), atom(), list()) -> term().
process(datastore_reader, Function, Args) ->
    ?update_counter(?EXOMETER_NAME(Function)),
    apply(datastore_reader, Function, Args);
process(Module, Function, Args = [#{model := Model} | _]) ->
    ?update_counter(?EXOMETER_NAME(Function)),
    case datastore_throttling:throttle_model(Model) of
        ok -> apply(Module, Function, Args);
        {error, Reason} -> {error, Reason}
    end.

-spec execute_on_node(node(), module(), atom(), list()) -> term().
execute_on_node(Node, Module, Fun, Args) ->
    MasterPid = datastore_cache_writer:get_master_pid(),
    case rpc:call(Node, datastore_router, execute_on_local_node, [Module, Fun, Args, MasterPid]) of
        {badrpc, Reason} -> {error, Reason};
        Result -> Result
    end.

-spec execute_on_local_node(module(), atom(), list(), pid() | undefined) -> term().
execute_on_local_node(Module, Fun, Args, MasterPid) ->
    datastore_cache_writer:save_master_pid(MasterPid),
    apply(Module, Fun, Args).

-spec get_routing_key(datastore:ctx(), datastore:doc()) -> datastore:key().
get_routing_key(_Ctx, #document{value = #links_forest{key = Key}}) ->
    Key;
get_routing_key(_Ctx, #document{value = #links_node{key = Key}}) ->
    Key;
get_routing_key(_Ctx, #document{value = #links_mask{key = Key}}) ->
    Key;
get_routing_key(#{model := Model} = _Ctx, #document{key = Key}) ->
    datastore_model:get_unique_key(Model, Key).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec route_internal(module(), atom(), datastore:ctx(), list()) -> term().
route_internal(Module, Function, Ctx, Args) ->
    try
        {Node, Ctx2, TryLocalRead} = select_node(Ctx),
        case {Module, TryLocalRead} of
            {datastore_writer, _} ->
                execute_on_node(Node, datastore_router, process, [Module, Function, [Ctx2 | Args]]);
            {_, true} ->
                datastore_router:process(Module, Function, [Node, Ctx2 | Args]);
            _ ->
                execute_on_node(Node, datastore_router, process, [Module, Function, [Node, Ctx2 | Args]])
        end
    catch
        _:Reason2 -> {error, Reason2}
    end.

%% @private
-spec retry_route(module(), atom(), list(), non_neg_integer(), non_neg_integer()) -> term().
retry_route(_Module, _Function, _Args, _Sleep, 0) ->
    {error, nodedown};
retry_route(Module, Function, [Ctx | ArgsTail] = Args, Sleep, Attempts) ->
    timer:sleep(Sleep),
    case route_internal(Module, Function, Ctx, ArgsTail) of
        {error, nodedown} -> retry_route(Module, Function, Args, 2 * Sleep, Attempts - 1);
        Ans -> Ans
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns node responsible for handling datastore call.
%% Extends context with information about memory_copies nodes.
%% @end
%%--------------------------------------------------------------------
-spec select_node(datastore:ctx()) -> {node(), datastore:ctx(), local_read()}.
select_node(#{routing := local} = Ctx) ->
    {node(), Ctx, true};
select_node(#{memory_copies := MemCopies, routing_key := Key} = Ctx) ->
    Seed = datastore_key:get_chash_seed(Key),
    #node_routing_info{assigned_nodes = [MasterNode | _] = Nodes,
        failed_nodes = FailedNodes, all_nodes = AllNodes} = consistent_hashing:get_routing_info(Seed),
    case Nodes -- FailedNodes of
        [Node | _] ->
            Ctx2 = Ctx#{failed_nodes => FailedNodes, failed_master => lists:member(MasterNode, FailedNodes)},
            {Ctx3, TryLocalRead} = case MemCopies of
                all ->
                    % TODO VFS-6168 - duplicates activity of HA but memory copy must be saved before operation finished
                    MemCopiesNodes = AllNodes, % -- Nodes -- FailedNodes,
                    % TODO VFS-6168 - Try local reads from HA slaves
                    {Ctx2#{memory_copies_nodes => MemCopiesNodes -- [Node]}, true};
                none ->
                    {Ctx2, false}
            end,
            {Node, Ctx3, TryLocalRead};
        [] ->
            throw(all_responsible_nodes_failed)
    end;
select_node(Ctx) ->
    select_node(Ctx#{memory_copies => none}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns module responsible for handling datastore call.
%% @end
%%--------------------------------------------------------------------
-spec select_module(atom()) -> module().
select_module(Function) when
    Function == get;
    Function == exists;
    Function == get_links;
    Function == get_links_trees ->
    datastore_reader;
select_module(_) ->
    datastore_writer.