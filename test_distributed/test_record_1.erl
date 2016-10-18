%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Test model.
%%% @end
%%%-------------------------------------------------------------------
-module(test_record_1).
-author("Mateusz Paciorek").
-behaviour(model_behaviour).

-include("datastore_test_models_def.hrl").
-include("modules/datastore/datastore_internal_model.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).
-export([record_struct/1]).
-export([record_upgrade/2]).
-export([maybe_init_versioning/1, get_test_record_version/1, set_test_record_version/2]).

maybe_init_versioning(ModelName) ->
    case catch is_process_alive(whereis(ModelName)) of
        true -> ok;
        _ ->
            Pid = spawn(fun() -> F = fun VFun(Version) ->
                receive
                    {get_version, Ref, Pid} ->
                        Pid ! {Ref, Version},
                        VFun(Version);
                    {set_version, NewVersion, Ref, Pid} ->
                        Pid ! {Ref, ok},
                        VFun(NewVersion)
                end
            end, F(1) end),
            register(ModelName, Pid),
            ok
    end.

get_test_record_version(ModelName) ->
    Ref = make_ref(),
    ModelName ! {get_version, Ref, self()},
    receive
        {Ref, Version} ->
            Version
    end.

set_test_record_version(ModelName, Version) ->
    Ref = make_ref(),
    ModelName ! {set_version, Version, Ref, self()},
    receive
        {Ref, ok} -> ok
    end.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1. 
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2. 
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1. 
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1. 
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0. 
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ok = maybe_init_versioning(?MODEL_NAME),
    ?MODEL_CONFIG(test_bucket, [{globally_cached_record, update}], ?DISK_ONLY_LEVEL)
        #model_config{version = get_test_record_version(?MODEL_NAME)}.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5. 
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4. 
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) ->
    ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

record_upgrade(1, {?MODEL_NAME, F1, F2, F3}) ->
    {2, {?MODEL_NAME, F1, F2, F3}};
record_upgrade(2, {?MODEL_NAME, F1, F2, _}) ->
    {3, {?MODEL_NAME, F1, F2}};
record_upgrade(3, {?MODEL_NAME, F1, F2}) ->
    {4, {?MODEL_NAME, F1, F2, {default, 5, atom}}};
record_upgrade(4, {?MODEL_NAME, F1, F2, {F31, F32, F33}}) when is_atom(F31), is_atom(F33), is_integer(F32) ->
    {5, {?MODEL_NAME, F1, F2, {F31, F32, F33}, [true, false]}};
record_upgrade(4, {?MODEL_NAME, F1, F2, {F31, F32, F33}}) when is_integer(F31) ->
    record_upgrade(4, {?MODEL_NAME, F1, F2, {list_to_atom(integer_to_list(F31)), F32, F33}});
record_upgrade(4, {?MODEL_NAME, F1, F2, {F31, F32, F33}}) when is_integer(F33) ->
    record_upgrade(4, {?MODEL_NAME, F1, F2, {F31, F32, list_to_atom(integer_to_list(F33))}}).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {field1, integer},
        {field2, integer},
        {field3, integer}
    ]};
record_struct(2) ->
    {record, [
        {field1, integer},
        {field2, term},
        {field3, integer}
    ]};
record_struct(3) ->
    {record, [
        {field1, integer},
        {field2, term}
    ]};
record_struct(4) ->
    {record, [
        {field1, term},
        {field2, term},
        {field3, term}
    ]};
record_struct(5) ->
    {record, [
        {field1, term},
        {field2, term},
        {field3, {atom, integer, atom}},
        {field4, [boolean]}
    ]}.


