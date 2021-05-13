%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Eunit tests for the datastore_router module.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_router_tests).
-author("Michal stanisz").

-ifdef(TEST).

-include("global_definitions.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/hashing/consistent_hashing.hrl").

-define(CTX, #{routing_key => <<"example_key">>, model => model}).

%%%===================================================================
%%% Setup and teardown
%%%===================================================================

datastore_router_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        lists:flatmap(fun(FunPlaceholder) ->
            lists:flatmap(fun(ExpResult) ->
                lists:map(fun(MemCopies) -> 
                    {
                        gen_test_name(FunPlaceholder, ExpResult, MemCopies), 
                        fun() -> route_test_base(FunPlaceholder, ExpResult, MemCopies) end
                    }
                end, [all, none])
            end, [{ok, ok}, {error, nodedown}])
        end, [datastore_reader, datastore_writer])
    }.

setup() ->
    meck:new(consistent_hashing, []),
    meck:new(datastore_router, [passthrough]),
    meck:expect(consistent_hashing, get_routing_info, fun(_Key) ->
        #node_routing_info{assigned_nodes = [node()], failed_nodes = [], all_nodes = []}
    end),
    application:set_env(?CLUSTER_WORKER_APP_NAME, datastore_router_retry_sleep_base, 0).

teardown(_) ->
    ?assert(meck:validate(consistent_hashing)),
    ?assert(meck:validate(datastore_router)),
    ok = meck:unload(consistent_hashing),
    ok = meck:unload(datastore_router).

%%%===================================================================
%%% Tests
%%%===================================================================

route_test_base(FunPlaceholder, ExpResult, MemCopies) ->
    FunctionName = placeholder_to_function_name(FunPlaceholder),
    PassedArgs = lists:seq(1, rand:uniform(8) - 1),
    meck:expect(datastore_router, process, 
        fun (Module, Function, AllArgs) ->
            case {Module, Function, retrieve_passed_args(FunPlaceholder, AllArgs)} of
                {FunPlaceholder, FunctionName, PassedArgs} -> ExpResult;
                _ -> error
            end
        end),
    ?assertEqual(ExpResult, 
        datastore_router:route(
            FunctionName, 
            [?CTX#{memory_copies => MemCopies} | PassedArgs]
        )).

%%%===================================================================
%%% Helper functions
%%%===================================================================

gen_test_name(Function, ExpResult, MemCopies) ->
    [T | _] = tuple_to_list(ExpResult),
    atom_to_list(Function) ++ "; " ++ atom_to_list(T) ++ "; " ++ atom_to_list(MemCopies).


placeholder_to_function_name(datastore_reader) ->
    lists_utils:random_element([get, exists,  get_links,  get_links_trees]);
placeholder_to_function_name(datastore_writer) ->
    some_writer_function.


retrieve_passed_args(datastore_reader, [_Node, _Ctx | ArgsTail]) -> ArgsTail;
retrieve_passed_args(datastore_writer, [_Ctx | ArgsTail]) -> ArgsTail.

-endif.