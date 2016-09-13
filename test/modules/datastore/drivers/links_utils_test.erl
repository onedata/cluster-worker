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

-endif.