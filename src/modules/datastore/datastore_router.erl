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

%% API
-export([route/4, process/3]).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().

%%%===================================================================
%%% API
%%%===================================================================

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
        ok -> erlang:apply(Module, Function, Args);
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
    consistent_hasing:get_node(Key).

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