%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @todo write me!
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_test_utils).
-author("Krzysztof Trzepla").

-include("datastore_test_utils.hrl").

%% API
-export([init_suite/1, init_suite/2, init_suite/3]).
-export([init_models/1, init_models/2]).
-export([get_memory_driver/1, get_disc_driver/1]).

-define(TIMEOUT, timer:seconds(60)).

%%%===================================================================
%%% API
%%%===================================================================

init_suite(Config) ->
    init_suite(?TEST_MODELS, Config).

init_suite(Models, Config) ->
    init_suite(Models, Config, fun(Config2) -> Config2 end).

init_suite(Models, Config, Fun) ->
    PostHook = fun(Config2) ->
        Workers = ?config(cluster_worker_nodes, Config2),
        datastore_test_utils:init_models(Workers, Models),
        Fun(Config2)
    end,
    [
        {?LOAD_MODULES, [datastore_test_utils]},
        {?ENV_UP_POSTHOOK, PostHook}
        | Config
    ].

init_models(Workers) ->
    init_models(Workers, ?TEST_MODELS).

init_models(Workers, Models) ->
    {Results, []} = gen_server:multi_call(Workers, node_manager,
        {apply, erlang, apply, [fun() ->
            mock_models(Models),
            lists:foreach(fun(Model) ->
                mock_model(Model),
                datastore_model:init(get_ctx(Model))
            end, Models)
        end, []]}, ?TIMEOUT
    ),

    ?assertAllMatch({_, ok}, Results).

mock_models(Models) ->
    Module = datastore_config_plugin,
    meck:new(Module, [no_history, non_strict]),
    meck:expect(Module, get_models, fun() -> Models end).

mock_model(Model) ->
    meck:new(Model, [no_history, non_strict]),
    lists:foreach(fun({Function, Expectation}) ->
        meck:expect(Model, Function, Expectation)
    end, [
        {get_ctx, fun() ->
            get_ctx(Model)
        end},
        {get_record_struct, fun(Version) ->
            get_record_struct(Model, Version)
        end},
        {create, fun(Doc) ->
            datastore_model:create(get_ctx(Model), Doc)
        end},
        {save, fun(Doc) ->
            datastore_model:save(get_ctx(Model), Doc)
        end},
        {update, fun(Key, Diff) ->
            datastore_model:update(get_ctx(Model), Key, Diff)
        end},
        {update, fun(Key, Diff, Doc) ->
            datastore_model:update(get_ctx(Model), Key, Diff, Doc)
        end},
        {get, fun(Key) ->
            datastore_model:get(get_ctx(Model), Key)
        end},
        {exists, fun(Key) ->
            datastore_model:exists(get_ctx(Model), Key)
        end},
        {delete, fun(Key) ->
            datastore_model:delete(get_ctx(Model), Key)
        end},
        {delete, fun(Key, Pred) ->
            datastore_model:delete(get_ctx(Model), Key, Pred)
        end},
        {fold, fun(Fun, Acc) ->
            datastore_model:fold(get_ctx(Model), Fun, Acc)
        end},
        {fold_keys, fun(Fun, Acc) ->
            datastore_model:fold_keys(get_ctx(Model), Fun, Acc)
        end},
        {add_links, fun(Key, TreeId, Links) ->
            datastore_model:add_links(get_ctx(Model), Key, TreeId, Links)
        end},
        {get_links, fun(Key, TreeId, LinkNames) ->
            datastore_model:get_links(get_ctx(Model), Key, TreeId, LinkNames)
        end},
        {delete_links, fun(Key, TreeId, Links) ->
            datastore_model:delete_links(get_ctx(Model), Key, TreeId, Links)
        end},
        {mark_links_deleted, fun(Key, TreeId, Links) ->
            Ctx = get_ctx(Model),
            datastore_model:mark_links_deleted(Ctx, Key, TreeId, Links)
        end},
        {fold_links, fun(Key, TreeIds, Fun, Acc, Opts) ->
            Ctx = get_ctx(Model),
            datastore_model:fold_links(Ctx, Key, TreeIds, Fun, Acc, Opts)
        end},
        {get_links_trees, fun(Key) ->
            Ctx = get_ctx(Model),
            datastore_model:get_links_trees(Ctx, Key)
        end}
    ]).

get_memory_driver(ets_only_model) ->
    ets_driver;
get_memory_driver(mnesia_only_model) ->
    mnesia_driver;
get_memory_driver(ets_cached_model) ->
    ets_driver;
get_memory_driver(mnesia_cached_model) ->
    mnesia_driver;
get_memory_driver(_) ->
    undefined.

get_memory_driver_ctx(Model) ->
    case get_memory_driver(Model) of
        undefined -> #{};
        _ -> #{table => ?TABLE(Model)}
    end.

get_disc_driver(ets_cached_model) ->
    couchbase_driver;
get_disc_driver(mnesia_cached_model) ->
    couchbase_driver;
get_disc_driver(disc_only_model) ->
    couchbase_driver;
get_disc_driver(_) ->
    undefined.

get_disc_driver_ctx(Model) ->
    case get_disc_driver(Model) of
        undefined -> #{};
        _ -> #{bucket => ?BUCKET}
    end.

get_ctx(Model) ->
    #{
        model => Model,
        fold_enabled => true,
        memory_driver => get_memory_driver(Model),
        memory_driver_ctx => get_memory_driver_ctx(Model),
        memory_driver_opts => [],
        disc_driver => get_disc_driver(Model),
        disc_driver_ctx => get_disc_driver_ctx(Model)
    }.

get_record_struct(_Model, 1) ->
    {record, [
        {field1, integer},
        {field2, string},
        {field3, atom}
    ]}.