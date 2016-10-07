%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for datastore_json module.
%%% @end
%%%--------------------------------------------------------------------
-module(datastore_json_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-record(test, {
    field1,
    field2,
    field3,
    field4,
    field5,
    field6,
    field7,
    field8,
    field9,
    field11,
    field10,
    field12
}).

-record(nasted, {
    field1,
    field2
}).

term_encoder_test() ->

    Struct1 = {record, 1, [
        {field1, integer},
        {field2, float},
        {field3, string},
        {field4, boolean},
        {field5, atom},
        {field6, binary},
        {field7, {integer, integer, integer, integer}},
        {field8, [atom]},
        {field9, {integer, atom, string, {float, float, string}}},
        {field11, #{string => [integer]}},
        {field10, {record, 1, [
            {field1, term},
            {field2, json}
        ]}},
        {field12, #{integer => {integer, [{binary, term, term, atom}]}}}
    ]},
    Term = #test{
        field1 = 12313,
        field2 = 432.423,
        field3 = "omfg yey",
        field4 = false,
        field5 = test_atom,
        field6 = <<"binary yey">>,
        field7 = {53, 243, undefined, 5435},
        field8 = [atom1, atom2, atom3, other_atom],
        field9 = {134, atom_test, "string in tuple", {6546.6456, 42432.43242, "string test"}},
        field11 = #{
            "str1" => [1,2,3],
            "str2" => [5,3,6,undefined,2,5,7,3,42,21]
        },
        field10 = #nasted{
            field1 = {self(), 9, [<<"dasdagfwerg">>, {1, 2, false}]},
            field2 = json_utils:encode([null, false, 5, <<"string">>])
        },
        field12 = #{56 => {321, [{<<"bin2">>, self(), atomfds, other_atom2}, {<<"bin3">>, self(), atomfds, other_atom3}]}}
    },

    Encoded = jiffy:encode(datastore_json:encode_record(
        Term,
        Struct1)),

    io:format(user, "~p~n", [Encoded]),

    Decoded = datastore_json:decode_record(jiffy:decode(Encoded), Struct1),

    ?assertMatch(Term, Decoded),

    ok.


-endif.