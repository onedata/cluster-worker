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
-export([route/4, process/3]).
-export([init_counters/0, init_report/0]).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().

-define(EXOMETER_COUNTERS,
    [save, update, create, create_or_update, get, delete, exists, add_links, 
        set_links, create_link, delete_links, fetch_link, foreach_link, 
        mark_links_deleted, get_links, fold_links, get_links_trees
    ]). 

-define(EXOMETER_NAME(Param), ?exometer_name(?MODULE,
        list_to_atom(atom_to_list(Param) ++ "_time"))).
-define(EXOMETER_DATASTORE_NAME(Param), ?exometer_name(?MODULE, Param)).
-define(EXOMETER_DEFAULT_DATA_POINTS_NUMBER, 10000).

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
    Size = application:get_env(?CLUSTER_WORKER_APP_NAME, 
        exometer_data_points_number, ?EXOMETER_DEFAULT_DATA_POINTS_NUMBER),
    Histograms = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), uniform, [{size, Size}]}
    end, ?EXOMETER_COUNTERS),
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_DATASTORE_NAME(Name), counter}
    end, ?EXOMETER_COUNTERS),
    ?init_counters(Counters ++ Histograms).

%%--------------------------------------------------------------------
%% @doc
%% Subscribe for reports for all parameters.
%% @end
%%--------------------------------------------------------------------
-spec init_report() -> ok.
init_report() ->
    Histograms = lists:map(fun(Name) ->
        {?EXOMETER_NAME(Name), [min, max, median, mean, n]}
    end, ?EXOMETER_COUNTERS),
    Counters = lists:map(fun(Name) ->
        {?EXOMETER_DATASTORE_NAME(Name), [value]}
    end, ?EXOMETER_COUNTERS),
    ?init_reports(Histograms ++ Counters).

%%--------------------------------------------------------------------
%% @doc
%% Redirects datastore call to designated node and module.
%% @end
%%--------------------------------------------------------------------
-spec route(ctx(), key(), atom(), list()) -> term().
route(Ctx, Key, Function, Args) ->
    Node = select_node(Ctx, Key),
    Module = select_module(Function),
    Args2 = [Module, Function, Args],
    case rpc:call(Node, datastore_router, process, Args2) of
        {badrpc, Reason} -> {error, Reason};
        Result -> Result
    end.

%%--------------------------------------------------------------------
%% @doc
%% Throttles datastore request if necessary.
%% @end
%%--------------------------------------------------------------------
-spec process(module(), atom(), list()) -> term().
process(Module, Function, Args = [#{model := Model} | _]) ->
    case datastore_throttling:throttle_model(Model) of
        ok -> 
            ?update_counter(?EXOMETER_DATASTORE_NAME(Function)),
            {Time, Ans} = timer:tc(erlang, apply, [Module, Function, Args]),
            ?update_counter(?EXOMETER_NAME(Function), Time),
            Ans;
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns node responsible for handling datastore call.
%% @end
%%--------------------------------------------------------------------
-spec select_node(ctx(), key()) -> node().
select_node(#{routing := local}, _Key) ->
    node();
select_node(_Ctx, Key) ->
    consistent_hashing:get_node(Key).

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
