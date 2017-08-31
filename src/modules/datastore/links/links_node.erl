%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model of node of datastore document links tree.
%%% @end
%%%-------------------------------------------------------------------
-module(links_node).
-author("Krzysztof Trzepla").

-include_lib("bp_tree/include/bp_tree.hrl").

%% API
-export([encode/1, decode/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type ctx() :: datastore:ctx().
-type id() :: datastore:key().
-type links_node() :: bp_tree:tree_node().

-export_type([id/0, links_node/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Encodes link node to JSON format.
%% @end
%%--------------------------------------------------------------------
-spec encode(links_node()) -> binary().
encode(#bp_tree_node{leaf = Leaf, children = Children}) ->
    Children2 = maps:fold(fun
        (Key, {LinkTarget, LinkRev}, Map) ->
            Map#{Key => #{
                <<"target">> => LinkTarget,
                <<"_rev">> => LinkRev
            }};
        (Key, Value, Map) ->
            Map#{Key => Value}
    end, #{}, bp_tree_array:to_map(Children)),
    jiffy:encode(#{
        <<"leaf">> => Leaf,
        <<"children">> => Children2
    }).

%%--------------------------------------------------------------------
%% @doc
%% Decodes link node from JSON format.
%% @end
%%--------------------------------------------------------------------
-spec decode(binary()) -> links_node().
decode(Term) ->
    #{
        <<"leaf">> := Leaf,
        <<"children">> := Children
    } = jiffy:decode(Term, [return_maps]),
    Children2 = maps:fold(fun
        (Key, #{<<"target">> := LinkTarget, <<"_rev">> := LinkRev}, Map) ->
            Map#{Key => {LinkTarget, LinkRev}};
        (Key, Value, Map) ->
            Map#{Key => Value}
    end, #{}, Children),
    #bp_tree_node{
        leaf = Leaf,
        children = bp_tree_array:from_map(Children2)
    }.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> ctx().
get_ctx() ->
    #{
        model => ?MODULE,
        memory_driver => undefined,
        disc_driver => undefined
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of model in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {model, atom},
        {key, string},
        {node, {custom, {?MODULE, encode, decode}}}
    ]}.