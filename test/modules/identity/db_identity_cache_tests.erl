%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(db_identity_cache_tests).
-author("Michal Zmuda").

-ifdef(TEST).
-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

-define(ENV, test_data).
-define(CACHE_TTL, 10).

%%%===================================================================
%%% Tests description
%%%===================================================================

identity_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            {"should create db records on put", fun should_create_record_on_put/0},
            {"should override db records on subsequent put", fun should_override_record_on_put/0},
            {"should evict record on invalidate", fun should_delete_record_on_invalidate/0},
            {"should return data on record present", fun should_get_data_on_record_present/0},
            {"should return nothing on record expired", fun should_no_get_data_on_record_expired/0}
        ]
    }.

%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

setup() ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, identity_cache_ttl_seconds, ?CACHE_TTL),

    meck:new(cached_identity),
    meck:expect(cached_identity, delete, fun(Key) -> delete_from_env(Key, ?ENV) end),
    meck:expect(cached_identity, get, fun(Key) -> get_from_env(Key, ?ENV) end),
    meck:expect(cached_identity, create_or_update, fun(DocToCreate, UpdateFun) ->
        Key = DocToCreate#document.key,
        NewDoc = case get_from_env(Key, ?ENV) of
            {error, _} -> DocToCreate;
            {ok, Current = #document{value = Value}} ->
                case UpdateFun(Value) of
                    {ok, Updated} -> Current#document{value = Updated};
                    _ -> Current
                end
        end,
        save_to_env(Key, NewDoc, ?ENV),
        {ok, DocToCreate#document.key}
    end),
    ok.

teardown(_) ->
    meck:unload(cached_identity),
    ok = application:unset_env(app, ?ENV),
    ok.

%%%===================================================================
%%% Tests functions
%%%===================================================================

should_create_record_on_put() ->
    %% given
    {ID1, PublicKey1, ID2, PublicKey2} = data(),

    %% when
    db_identity_cache:put(ID1, PublicKey1),
    db_identity_cache:put(ID2, PublicKey2),

    %% then
    ?assertMatch({ok, #document{key = ID1, value = #cached_identity{
        encoded_public_key = PublicKey1, id = ID1}}}, get_from_env(ID1, ?ENV)),
    ?assertMatch({ok, #document{key = ID2, value = #cached_identity{
        encoded_public_key = PublicKey2, id = ID2}}}, get_from_env(ID2, ?ENV)).


should_override_record_on_put() ->
    %% given
    {ID1, PublicKey1, ID2, PublicKey2} = data(),
    db_identity_cache:put(ID1, <<"to override">>),
    db_identity_cache:put(ID2, <<"to override">>),

    %% when
    db_identity_cache:put(ID1, PublicKey1),
    db_identity_cache:put(ID2, PublicKey2),

    %% then
    ?assertMatch({ok, #document{key = ID1, value = #cached_identity{
        encoded_public_key = PublicKey1, id = ID1}}}, get_from_env(ID1, ?ENV)),
    ?assertMatch({ok, #document{key = ID2, value = #cached_identity{
        encoded_public_key = PublicKey2, id = ID2}}}, get_from_env(ID2, ?ENV)).

should_delete_record_on_invalidate() ->
    %% given
    {ID1, PublicKey1, ID2, PublicKey2} = data(),
    db_identity_cache:put(ID1, PublicKey1),
    db_identity_cache:put(ID2, PublicKey2),

    %% when
    db_identity_cache:invalidate(ID1),

    %% then
    ?assertMatch({error, _}, get_from_env(ID1, ?ENV)),
    ?assertMatch({ok, #document{key = ID2, value = #cached_identity{
        encoded_public_key = PublicKey2, id = ID2}}}, get_from_env(ID2, ?ENV)).

should_get_data_on_record_present() ->
    %% given
    {ID1, PublicKey1, ID2, _PublicKey2} = data(),
    Now = erlang:system_time(seconds),
    create_record(ID1, PublicKey1, Now),

    %% when
    Res1 = db_identity_cache:get(ID1),
    Res2 = db_identity_cache:get(ID2),

    %% then
    ?assertMatch({ok, PublicKey1}, Res1),
    ?assertMatch({error, _}, Res2).

should_no_get_data_on_record_expired() ->
    %% given
    {ID1, PublicKey1, ID2, PublicKey2} = data(),
    Now = erlang:system_time(seconds),
    create_record(ID1, PublicKey1, Now - (2 * ?CACHE_TTL)),
    create_record(ID2, PublicKey2, Now - (0.5 * ?CACHE_TTL)),

    %% when
    Res1 = db_identity_cache:get(ID1),
    Res2 = db_identity_cache:get(ID2),

    %% then
    ?assertMatch({error, expired}, Res1),
    ?assertMatch({ok, PublicKey2}, Res2).


%%%===================================================================
%%% Internal functions
%%%===================================================================

create_record(ID1, PublicKey1, Now) ->
    save_to_env(ID1, #document{key = ID1, value = #cached_identity{id = ID1, encoded_public_key = PublicKey1, last_update_seconds = Now}}, ?ENV).

data() ->
    ID1 = <<"id1">>,
    PublicKey1 = <<"data1">>,
    ID2 = <<"id2">>,
    PublicKey2 = <<"data2">>,
    {ID1, PublicKey1, ID2, PublicKey2}.

save_to_env(ID, Data, Env) ->
    Saved = application:get_env(app, Env, #{}),
    application:set_env(app, Env, Saved#{ID => Data}),
    ok.

delete_from_env(ID, Env) ->
    Saved = application:get_env(app, Env, #{}),
    application:set_env(app, Env, maps:remove(ID, Saved)),
    ok.

get_from_env(ID, Env) ->
    case application:get_env(app, Env, #{}) of
        #{ID := Value} -> {ok, Value};
        _ -> {error, Env}
    end.

-endif.