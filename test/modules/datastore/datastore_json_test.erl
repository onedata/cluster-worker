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
-include("modules/datastore/datastore_common_internal.hrl").

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
    field12,
    field13
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
        {field10, {record, 5, [
            {field1, term},
            {field2, json}
        ]}},
        {field12, #{integer => {integer, [{binary, term, term, atom}]}}},
        {field13, {set, atom}}
    ]},

    NastedF1Value = {self(), 9, [<<"dasdagfwerg">>, {1, 2, false}]},
    Set = sets:from_list([at1, at2, at2, at3, at2, at1, at5, at3]),
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
            field1 = NastedF1Value,
            field2 = json_utils:encode([null, false, 5, <<"string">>])
        },
        field12 = #{56 => {321, [{<<"bin2">>, self(), atomfds, other_atom2}, {<<"bin3">>, self(), atomfds, other_atom3}]}},
        field13 = Set
        },

    {Props} = EJSON = datastore_json:encode_record(Term, Struct1),
    Keys = proplists:get_keys(Props),

    ?assertMatch(true, lists:member(<<"<record_version>">>, Keys)),
    FVValue = proplists:get_value(<<"<record_version>">>, Props),
    ?assertMatch(1, FVValue),

    ?assertMatch(true, lists:member(<<"<record_type>">>, Keys)),
    FTValue = proplists:get_value(<<"<record_type>">>, Props),
    ?assertMatch(<<"test">>, FTValue),

    ?assertMatch(true, lists:member(<<"field1">>, Keys)),
    F1Value = proplists:get_value(<<"field1">>, Props),
    ?assertMatch(12313, F1Value),

    ?assertMatch(true, lists:member(<<"field2">>, Keys)),
    F2Value = proplists:get_value(<<"field2">>, Props),
    ?assertMatch(432.423, F2Value),

    ?assertMatch(true, lists:member(<<"field3">>, Keys)),
    F3Value = proplists:get_value(<<"field3">>, Props),
    ?assertMatch(<<"omfg yey">>, F3Value),

    ?assertMatch(true, lists:member(<<"field4">>, Keys)),
    F4Value = proplists:get_value(<<"field4">>, Props),
    ?assertMatch(false, F4Value),

    ?assertMatch(true, lists:member(<<"field5">>, Keys)),
    F5Value = proplists:get_value(<<"field5">>, Props),
    ?assertMatch(<<"test_atom">>, F5Value),

    ?assertMatch(true, lists:member(<<"field6">>, Keys)),
    F6Value = proplists:get_value(<<"field6">>, Props),
    ?assertMatch(<<"binary yey">>, F6Value),

    ?assertMatch(true, lists:member(<<"field7">>, Keys)),
    F7Value = proplists:get_value(<<"field7">>, Props),
    ?assertMatch([53, 243, null, 5435], F7Value),

    ?assertMatch(true, lists:member(<<"field8">>, Keys)),
    F8Value = proplists:get_value(<<"field8">>, Props),
    ?assertMatch([<<"atom1">>, <<"atom2">>, <<"atom3">>, <<"other_atom">>], F8Value),

    ?assertMatch(true, lists:member(<<"field9">>, Keys)),
    F9Value = proplists:get_value(<<"field9">>, Props),
    ?assertMatch([134, <<"atom_test">>, <<"string in tuple">>, [6546.6456, 42432.43242, <<"string test">>]], F9Value),

    SelfTerm = base64:encode(term_to_binary(self())),
    AtomTerm = base64:encode(term_to_binary(atomfds)),

    ?assertMatch(true, lists:member(<<"field10">>, Keys)),
    F10Value = proplists:get_value(<<"field10">>, Props),
    ?assertMatch({_}, F10Value),
    {Props10} = F10Value,
    ?assertMatch(true, lists:member(<<"<record_version>">>, proplists:get_keys(Props10))),
    ?assertMatch(5, proplists:get_value(<<"<record_version>">>, Props10)),
    ?assertMatch(true, lists:member(<<"<record_type>">>, proplists:get_keys(Props10))),
    ?assertMatch(<<"nasted">>, proplists:get_value(<<"<record_type>">>, Props10)),
    ?assertMatch(true, lists:member(<<"field1">>, proplists:get_keys(Props10))),
    NastedF1ValueBin = base64:encode(term_to_binary(NastedF1Value)),
    ?assertMatch(NastedF1ValueBin, proplists:get_value(<<"field1">>, Props10)),
    ?assertMatch(true, lists:member(<<"field2">>, proplists:get_keys(Props10))),
    ?assertMatch([null, false, 5, <<"string">>], proplists:get_value(<<"field2">>, Props10)),


    ?assertMatch(true, lists:member(<<"field11">>, Keys)),
    F11Value = proplists:get_value(<<"field11">>, Props),
    ?assertMatch({_}, F11Value),
    {Props11} = F11Value,
    ?assertMatch(true, lists:member(<<"str1">>, proplists:get_keys(Props11))),
    ?assertMatch(true, lists:member(<<"str2">>, proplists:get_keys(Props11))),
    ?assertMatch([1,2,3], proplists:get_value(<<"str1">>, Props11)),
    ?assertMatch([5,3,6,null,2,5,7,3,42,21], proplists:get_value(<<"str2">>, Props11)),

    ?assertMatch(true, lists:member(<<"field12">>, Keys)),
    F12Value = proplists:get_value(<<"field12">>, Props),
    ?assertMatch({[{<<"56">>, [321, [
        [<<"bin2">>, SelfTerm, AtomTerm, <<"other_atom2">>],
        [<<"bin3">>, SelfTerm, AtomTerm, <<"other_atom3">>]
    ]]}]}, F12Value),

    ?assertMatch(true, lists:member(<<"field13">>, Keys)),
    F13Value = proplists:get_value(<<"field13">>, Props),
    ?assertMatch([<<"at1">>,<<"at2">>,<<"at3">>,<<"at5">>], lists:sort(F13Value)),


    Encoded = jiffy:encode(EJSON),
    Decoded = datastore_json:decode_record(jiffy:decode(Encoded), Struct1),

    ?assertMatch(Term, Decoded),

    ok.


-endif.