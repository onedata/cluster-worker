%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model of node of datastore document links tree.
%%% It represents single B+ tree node.
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
encode(#bp_tree_node{leaf = Leaf, children = Children, order = Order, rebalance_info = RI}) ->
    Children2 = maps:fold(fun
        (Key, {LinkTarget, LinkRev}, Map) when is_binary(Key) ->
            Map#{Key => #{
                <<"target">> => LinkTarget,
                <<"_rev">> => LinkRev
            }};
        (Key, Value, Map) when is_binary(Key) ->
            Map#{Key => Value};
        (Key, {LinkTarget, LinkRev}, Map) when is_integer(Key) ->
            Map#{integer_to_binary(Key) => #{
                <<"target">> => LinkTarget,
                <<"_rev">> => LinkRev,
                <<"type">> => <<"int">>
            }};
        (Key, Value, Map) when is_integer(Key) ->
            Map#{integer_to_binary(Key) => #{
                <<"target">> => Value,
                <<"type">> => <<"int">>
            }}
    end, #{}, bp_tree_children:to_map(Children)),

    FinalMap = #{
        <<"leaf">> => Leaf,
        <<"children">> => Children2
    },

    FinalMap2 = case Order of
        undefined -> FinalMap;
        _ -> maps:put(<<"order">>, Order, FinalMap)
    end,

    FinalMap3 = case RI of
        undefined ->
            FinalMap2;
        _ ->
            RI_Lists = lists:map(fun({_NodeId, _Key} = Term) -> base64:encode(term_to_binary(Term)) end, RI),
            maps:put(<<"rebalance_info">>, RI_Lists, FinalMap2)
    end,

    json_utils:encode(FinalMap3).

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
    } = InputMap = json_utils:decode(Term),
    Children2 = maps:fold(fun
        (Key, #{<<"target">> := LinkTarget, <<"_rev">> := <<"undefined">>,
            <<"type">> := <<"int">>}, Map) ->
            Map#{binary_to_integer(Key) => {LinkTarget, undefined}};
        (Key, #{<<"target">> := LinkTarget, <<"_rev">> := LinkRev,
            <<"type">> := <<"int">>}, Map) ->
            Map#{binary_to_integer(Key) => {LinkTarget, LinkRev}};
        (Key, #{<<"target">> := LinkTarget, <<"_rev">> := <<"undefined">>}, Map) ->
            Map#{Key => {LinkTarget, undefined}};
        (Key, #{<<"target">> := LinkTarget, <<"_rev">> := LinkRev}, Map) ->
            Map#{Key => {LinkTarget, LinkRev}};
        (Key, #{<<"target">> := Value, <<"type">> := <<"int">>}, Map) ->
            Map#{binary_to_integer(Key) => Value};
        (Key, Value, Map) ->
            Map#{Key => Value}
    end, #{}, Children),

    RI = case maps:get(<<"rebalance_info">>, InputMap, undefined) of
        undefined -> undefined;
        List -> lists:map(fun(Binary) -> binary_to_term(base64:decode(Binary)) end, List)
    end,

    #bp_tree_node{
        leaf = Leaf,
        children = bp_tree_children:from_map(Children2),
        order = maps:get(<<"order">>, InputMap, undefined),
        rebalance_info = RI
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
        {node, {custom, json, {?MODULE, encode, decode}}}
    ]}.