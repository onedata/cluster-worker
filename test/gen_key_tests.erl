%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% Performance tests of gen_key function
%%% @end
%%%-------------------------------------------------------------------
-module(gen_key_tests).

-ifdef(TEST).

-include("global_definitions.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Tests description
%%%===================================================================

gen_key_test_() ->
    {timeout, 60, fun test_gen_key/0}.

%%%===================================================================
%%% Test functions
%%%===================================================================

test_gen_key() ->
    lists:foreach(fun(_) -> gen_key_test_base() end, lists:seq(1,10)).

gen_key_test_base() ->
    Repeats = 100000,
    Key = gen_hex(16),
    Times = lists:map(fun(_) ->
        gen_key(<<"seed">>,Key)
    end, lists:seq(0,Repeats)),
    
    TT = lists:foldl(fun(TimesList, AccList) -> 
        lists:map(fun({T, A}) -> T+A end, lists:zip(TimesList, AccList))
    end, [0,0,0,0,0], Times),
    
    List = lists:map(fun(T) -> T/Repeats end, TT),
    Total = lists:sum(List),
    ct:print("Init[ns]: ~p~n"
             "Hash update1[ns]: ~p~n"
             "Hash update2[ns]: ~p~n"
             "Hash final[ns]: ~p~n"
             "Hex[ns]: ~p~n"
             "Total[ns]: ~p~n"
             "Hex in Total[%]: ~p"
        ,List ++ [Total, lists:nth(5, List)/Total*100]).

-endif.


%%%===================================================================
%%% Internal
%%%===================================================================

gen_hex(Size) ->
    hex_utils:hex(crypto:strong_rand_bytes(Size)).

gen_key(Seed, Key) when is_binary(Seed) ->
    Time1 = erlang:monotonic_time(nanosecond),
    Ctx = crypto:hash_init(md5),

    Time2 = erlang:monotonic_time(nanosecond),
    Ctx2 = crypto:hash_update(Ctx, Seed),

    Time3 = erlang:monotonic_time(nanosecond),
    Ctx3 = crypto:hash_update(Ctx2, Key),

    Time4 = erlang:monotonic_time(nanosecond),
    Digest = crypto:hash_final(Ctx3),

    Time5 = erlang:monotonic_time(nanosecond),
    hex_utils:hex(Digest),
    Time6 = erlang:monotonic_time(nanosecond),

    [Time2-Time1, Time3-Time2, Time4-Time3, Time5-Time4, Time6-Time5].