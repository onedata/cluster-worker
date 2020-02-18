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

-include("global_definitions.hrl").
-include("exometer_utils.hrl").

%% API
-export([route/2, process/3]).
-export([init_counters/0, init_report/0]).

-type local_read() :: boolean(). % true if read should be tried locally before delegation to chosen node

-define(EXOMETER_COUNTERS,
    [save, update, create, create_or_update, get, delete, exists, add_links, check_and_add_links,
        set_links, create_link, delete_links, fetch_link, foreach_link,
        mark_links_deleted, get_links, fold_links, get_links_trees
    ]).
-define(EXOMETER_DATASTORE_NAME(Param), ?exometer_name(?MODULE, Param)).

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
        {?EXOMETER_DATASTORE_NAME(Name), counter}
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
        {?EXOMETER_DATASTORE_NAME(Name), [value]}
    end, ?EXOMETER_COUNTERS),
    ?init_reports(Counters).

%%--------------------------------------------------------------------
%% @doc
%% Redirects datastore call to designated node and module.
%% @end
%%--------------------------------------------------------------------
-spec route(atom(), list()) -> term().
route(Function, Args) ->
    Module = select_module(Function),
    {Node, Args2, TryLocalRead} = select_node(Args),
    try
        case {Module, TryLocalRead} of
            {datastore_writer, _} ->
                case rpc:call(Node, datastore_router, process, [Module, Function, Args2]) of
                    {badrpc, Reason} -> {error, Reason};
                    Result -> Result
                end;
            {_, true} ->
                datastore_router:process(Module, Function, [Node | Args2]);
            _ ->
                case rpc:call(Node, datastore_router, process, [Module, Function, [Node | Args2]]) of
                    {badrpc, Reason} -> {error, Reason};
                    Result -> Result
                end
        end
    catch
        _:Reason2 -> {error, Reason2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Throttles datastore request if necessary.
%% @end
%%--------------------------------------------------------------------
-spec process(module(), atom(), list()) -> term().
process(datastore_doc, Function, Args) ->
    ?update_datastore_counter(?EXOMETER_DATASTORE_NAME(Function)),
    apply(datastore_doc, Function, Args);
process(Module, Function, Args = [#{model := Model} | _]) ->
    ?update_datastore_counter(?EXOMETER_DATASTORE_NAME(Function)),
    case datastore_throttling:throttle_model(Model) of
        ok -> apply(Module, Function, Args);
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns node responsible for handling datastore call.
%% Extends context with information about memory_copies nodes.
%% @end
%%--------------------------------------------------------------------
-spec select_node(list()) -> {node(), list(), local_read()}.
select_node([#{routing := local} | _] = Args) ->
    {node(), Args, true};
select_node([#{memory_copies := MemCopies, routing_key := Key} = Ctx | ArgsTail]) ->
    {[Node | KeyConnectedNodesTail] = KeyConnectedNodes, OtherRequestedNodes, BrokenNodes} =
        datastore_key:responsible_nodes(Key, MemCopies),
    {Node, [Ctx#{key_connecyed_nodes => KeyConnectedNodesTail, memory_copies_nodes => OtherRequestedNodes,
        broken_nodes => BrokenNodes} | ArgsTail], lists:member(node(), KeyConnectedNodes)};
select_node([#{routing_key := Key} | _] = Args) ->
    Node = datastore_key:responsible_node(Key),
    {Node, Args, false}.

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
    datastore_doc;
select_module(_) ->
    datastore_writer.
