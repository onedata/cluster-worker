%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for links_utils module.
%%% @end
%%%--------------------------------------------------------------------
-module(links_utils_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include("modules/datastore/datastore_common_internal.hrl").

diff_test() ->

    ?assertMatch(
        {
            #{b := {_, [b1, b2]}},
            #{a := {_, [a1, a2, a3]}}
        },
        links_utils:diff(
            #links{link_map = #{a => {0, [a1, a2, a3]}}},
            #links{link_map = #{b => {0, [b1, b2]}}}
        )),

    ?assertMatch(
        {
            #{b := {_, [b1, b2]}},
            #{a := {_, [a2]}}
        },
        links_utils:diff(
            #links{link_map = #{a => {0, [a1, a2, a3]}}},
            #links{link_map = #{b => {0, [b1, b2]}, a => {0, [a1, a3]}}}
        )),

    ?assertMatch(
        {
            #{b := {_, [b1]}},
            #{a := {_, [a2]}}
        },
        links_utils:diff(
            #links{link_map = #{a => {0, [a1, a2, a3]}, b => {0, [b2]}}},
            #links{link_map = #{b => {0, [b1, b2]}, a => {0, [a1, a3]}}}
        )),


    ?assertMatch(
        {
            #{b := {_, [b1]}, a := {_, [a4]}},
            #{a := {_, [a2]}}
        },
        links_utils:diff(
            #links{link_map = #{a => {0, [a1, a2, a3]}, b => {0, [b2]}}},
            #links{link_map = #{b => {0, [b1, b2]}, a => {0, [a1, a3, a4]}}}
        )),

    ?assertMatch(
        {
            #{b := {_, [b1, b6, b7]}, a := {_, [a4, a5, a6]}},
            #{a := {_, [a2, a7, a8]}, b := {_, [b3, b4, b5]}}
        },
        links_utils:diff(
            #links{link_map = #{a => {0, [a1, a2, a3, a7, a8]}, b => {0, [b2, b3, b4, b5]}}},
            #links{link_map = #{b => {0, [b1, b2, b6, b7]}, a => {0, [a1, a3, a4, a5, a6]}}}
        )),


    ?assert(true).

deduplicate_targets_test() ->

    %% Second tuple field shall be ignored while comparing link targets
    ?assertMatch([], links_utils:deduplicate_targets([])),
    ?assertMatch([{1,2,3,4}], links_utils:deduplicate_targets([{1,2,3,4}])),
    ?assertMatch([{1,2,3,4}], links_utils:deduplicate_targets([
        {1,2,3,4},
        {1,2,3,4}
    ])),

    ?assertMatch([{1,2,3,4}], links_utils:deduplicate_targets(lists:usort([
        {1,2,3,4},
        {1,2,3,4},
        {1,2,3,4},
        {1,2,3,4}
    ]))),


    ?assertMatch([{1,2,3,4}, {1,2,3,5}], links_utils:deduplicate_targets(lists:usort([
        {1,2,3,4},
        {1,2,3,4},
        {1,2,3,4},
        {1,2,3,5},
        {1,2,3,4}
    ]))),


    ?assertMatch([{1,1,3,4}, {1,1,4,4}, {1,9,3,4}], links_utils:deduplicate_targets(lists:usort([
        {1,1,3,4},
        {1,1,4,4},
        {1,3,3,4},
        {1,2,3,4},
        {1,2,3,4},
        {1,8,3,4},
        {1,9,3,4},
        {1,2,3,4}
    ]))),

    ok.


make_scoped_link_name_test() ->

    ?assertMatch(<<"Name", ?LINK_NAME_SCOPE_SEPARATOR, "s">>,
        links_utils:make_scoped_link_name(<<"Name">>, <<"scope">>, undefined, 1)),

    ?assertMatch(<<"Name", ?LINK_NAME_SCOPE_SEPARATOR, "sco">>,
        links_utils:make_scoped_link_name(<<"Name">>, <<"scope">>, undefined, 3)),

    ?assertMatch(<<"Name", ?LINK_NAME_SCOPE_SEPARATOR, "s", ?LINK_NAME_SCOPE_SEPARATOR, "vh">>,
        links_utils:make_scoped_link_name(<<"Name">>, <<"scope">>, <<"vh">>, 1)),

    ?assertMatch(<<"Name", ?LINK_NAME_SCOPE_SEPARATOR, "sco", ?LINK_NAME_SCOPE_SEPARATOR, "vh">>,
        links_utils:make_scoped_link_name(<<"Name">>, <<"scope">>, <<"vh">>, 3)),


    ?assertMatch({scoped_link, linkname, <<"scope">>, <<"vh">>},
        links_utils:make_scoped_link_name(linkname, <<"scope">>, <<"vh">>, 3)),

    ?assertMatch({scoped_link, <<"name">>, scope, <<"vh">>},
        links_utils:make_scoped_link_name(<<"name">>, scope, <<"vh">>, 3)),

    ok.

unpack_link_scope_test() ->

    ?assertMatch({link, undefined, undefined},
        links_utils:unpack_link_scope(model_name, link)),

    ?assertMatch({<<"link">>, undefined, undefined},
        links_utils:unpack_link_scope(model_name, <<"link">>)),


    ?assertMatch({a, b, c},
        links_utils:unpack_link_scope(model_name, {scoped_link, a, b, c})),

    ?assertMatch({<<"link">>, undefined, undefined},
        links_utils:unpack_link_scope(model_name, <<"link", ?LINK_NAME_SCOPE_SEPARATOR>>)),

    ?assertMatch({<<"link">>, <<"scope">>, undefined},
        links_utils:unpack_link_scope(model_name, <<"link", ?LINK_NAME_SCOPE_SEPARATOR, "scope">>)),

    ?assertMatch({<<"link", ?LINK_NAME_SCOPE_SEPARATOR, "scope">>, <<"nonvhash">>, undefined},
        links_utils:unpack_link_scope(model_name,
            <<"link", ?LINK_NAME_SCOPE_SEPARATOR, "scope", ?LINK_NAME_SCOPE_SEPARATOR, "nonvhash">>)),

    ?assertMatch({<<"link">>, <<"scope">>, <<"__VH__vhash">>},
        links_utils:unpack_link_scope(model_name,
            <<"link", ?LINK_NAME_SCOPE_SEPARATOR, "scope", ?LINK_NAME_SCOPE_SEPARATOR, "__VH__vhash">>)),

    ok.

select_scope_related_link_test() ->

%%select_scope_related_link(LinkName, RequestedScope, VHash, Targets) ->
%%    case lists:filter(
%%        fun
%%            ({Scope, VH, _, _}) when is_binary(LinkName), is_binary(Scope), is_binary(RequestedScope) ->
%%                lists:prefix(binary_to_list(RequestedScope), binary_to_list(Scope))
%%                    andalso (VHash == undefined orelse VHash == VH);
%%            ({Scope, VH, _, _}) ->
%%                RequestedScope =:= Scope andalso (VHash == undefined orelse VHash == VH);
%%            ({Scope, {deleted, VH}, _, _}) when is_binary(LinkName), is_binary(Scope), is_binary(RequestedScope) ->
%%                lists:prefix(binary_to_list(RequestedScope), binary_to_list(Scope))
%%                    andalso (VHash == undefined orelse VHash == VH);
%%            ({Scope, {deleted, VH}, _, _}) ->
%%                RequestedScope =:= Scope andalso (VHash == undefined orelse VHash == VH)
%%        end, Targets) of
%%        [] -> undefined;
%%        [L | _] ->
%%            L
%%    end.

    ok.

-endif.