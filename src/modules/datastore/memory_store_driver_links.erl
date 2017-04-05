%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Helper for memory_store_driver for links operations.
%%% @end
%%%-------------------------------------------------------------------
-module(memory_store_driver_links).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/tp/tp.hrl").
-include("modules/datastore/memory_store_driver.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle_link_messages/6, clear/3]).

%% API for link_utils
-export([save_link_doc/2, get_link_doc/2, delete_link_doc/2]).

%% for eunit
-export([merge_link_ops/1, merge_link_ops/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles operations on links.
%% @end
%%--------------------------------------------------------------------
-spec handle_link_messages(Messages :: [memory_store_driver:message()],
    CurrentValue :: memory_store_driver:value_link(), Driver :: atom(),
    FD :: atom(), ModelConfig :: model_behaviour:model_config(),
    Key :: datastore:ext_key()) -> {AnsList :: list(),
    NewCurrentValue :: memory_store_driver:value_link(),
    Changes :: [memory_store_driver:change()]}.
handle_link_messages(Messages, CurrentValue, Driver, FD,
    #model_config{name = MN} = ModelConfig, Key) ->
  put(doc_cache, CurrentValue),
  put(mem_driver, Driver),
  put(flush_driver, FD),

  Agg = merge_link_ops(Messages),

  Ans = lists:foldl(fun({M, Num}, TmpAns) ->
    put(current_message, M),
    A = handle_link_message(M, Driver, FD, ModelConfig),
    lists:duplicate(Num, A) ++ TmpAns
  end, [], Agg),

  NewOps = get_value(op_cache),
  Restored = get_value(restore_cache),

  lists:foreach(fun({K,  V}) ->
      case apply(Driver, save_link_doc, [ModelConfig,
        #document{key = K, value = V}]) of
        {ok, _} ->
          ok;
        RErr ->
          throw({{restore, K,  V, RErr}, CurrentValue, []})
      end
  end, Restored),

  case get(foreach_restore) of
    true ->
      % TODO - allow foreach_link that does not check whole links tree
      CCCUuid = caches_controller:get_cache_uuid(Key, MN),
      caches_controller:consistency_restored(Driver, CCCUuid);
    _ ->
      ok
  end,

  % TODO - revers changes applied to mnesia or ets
  {DumpAns, Changes} = lists:foldl(fun
    ({K, delete_link_doc}, {ok, TmpChanges}) ->
      case apply(Driver, delete_link_doc, [ModelConfig, K]) of
        ok ->
          {ok, [K | TmpChanges]};
        Err ->
          reverse_doc_change_in_memory(K, CurrentValue),
          {Err, TmpChanges}
      end;
    ({K,  {_, ToSave}}, {ok, TmpChanges}) ->
      case apply(Driver, save_link_doc, [ModelConfig,
        #document{key = K, value = ToSave}]) of
        {ok, _} ->
          {ok, [K | TmpChanges]};
        Err ->
          reverse_doc_change_in_memory(K, CurrentValue),
          {Err, TmpChanges}
      end;
    (_, Acc) ->
      Acc
  end, {ok, []}, NewOps),
  ResolveCache = get_value(dump_cache),

  NewCV = get_value(doc_cache),
  erase(),

  case DumpAns of
    ok ->
      {lists:reverse(Ans), NewCV, {Changes, ResolveCache}};
    _ ->
      {lists:map(fun(_) -> DumpAns end, Messages), NewCV, {Changes, ResolveCache}}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Handles clear operation.
%% @end
%%--------------------------------------------------------------------
-spec clear(Driver :: atom(), ModelConfig :: model_behaviour:model_config(),
    Key :: datastore:ext_key()) ->
  ok | datastore:generic_error().
clear(Driver, #model_config{name = MN} = ModelConfig, Key) ->
  CCCUuid = caches_controller:get_cache_uuid(Key, MN),
  case caches_controller:save_consistency_info_direct(Driver, CCCUuid, all) of
    true ->
      apply(links_utils, delete_links, [Driver, ModelConfig, Key]);
    _ ->
      {error, consistency_info_save_failed}
  end.

%%%===================================================================
%%% Link utils API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves document that describes links, not using transactions
%% (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec save_link_doc(model_behaviour:model_config(), datastore:document()) ->
  {ok, datastore:ext_key()} | datastore:generic_error().
save_link_doc(_MC, #document{key = Key, value = Value}) ->
  add_doc_to_memory(Key, Value),
  add_change_to_memory(Key, {save_link_doc, Value}),
  {ok, Key}.

%%--------------------------------------------------------------------
%% @doc
%% Gets document that describes links (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec get_link_doc(model_behaviour:model_config(), datastore:ext_key()) ->
  {ok, datastore:document()} | datastore:get_error().
get_link_doc(#model_config{name = MN} = ModelConfig, Key) ->
  get_link_doc(#model_config{name = MN} = ModelConfig, undefined, Key).

%%--------------------------------------------------------------------
%% @doc
%% Gets document that describes links (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec get_link_doc(model_behaviour:model_config(),
    BucketOverride:: binary() | undefined, datastore:ext_key()) ->
  {ok, datastore:document()} | datastore:get_error().
get_link_doc(#model_config{name = MN} = ModelConfig, BucketOverride, Key) ->
  case get_doc_from_memory(Key) of
    undefined ->
      get_from_stores(ModelConfig, Key, BucketOverride);
    % TODO consider: save {error, {not_found, MN}} in memory instead not_found
    not_found ->
      {error, {not_found, MN}};
    Cached ->
      add_restore_to_memory(Key, Cached, true),
      {ok, #document{key = Key, value = Cached}}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes document that describes links, not using transactions
%% (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec delete_link_doc(model_behaviour:model_config(), datastore:document()) ->
  ok | datastore:generic_error().
delete_link_doc(_MC, #document{key = Key} = _Document) ->
  add_doc_to_memory(Key, not_found),
  add_change_to_memory(Key, delete_link_doc),
  ok.

%%--------------------------------------------------------------------
%% @doc
%% Checks if document exist.
%% @end
%%--------------------------------------------------------------------
-spec exists_link_doc(model_behaviour:model_config(), datastore:ext_key(),
    links_utils:scope()) ->
  {ok, boolean()} | datastore:generic_error().
% TODO - remove operation (used only during ct tests)
exists_link_doc(MC, DocKey, Scope) ->
  Key = links_utils:links_doc_key(DocKey, Scope),
  case get_link_doc(MC, Key) of
    {error, {not_found, _}} -> {ok, false};
    {ok, _} -> {ok, true};
    Error -> Error
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles single link operation.
%% @end
%%--------------------------------------------------------------------
-spec handle_link_message(Messages :: memory_store_driver:message(),
    Driver :: atom(), FD :: atom(),
    ModelConfig :: model_behaviour:model_config()) -> ok | {error, term()}.
handle_link_message({force_save, Args}, _Driver, FD, ModelConfig) ->
  [Bucket, ToSave] = case Args of
    [TS] ->
      [FD:select_bucket(ModelConfig, TS), TS];
    _ ->
      Args
  end,
  case memory_store_driver:resolve_conflict(ModelConfig, FD, ToSave) of
    not_changed ->
      ok;
    {#document{key = Key, deleted = true} = Document, ToDel} ->
      delete_link_doc(ModelConfig, Document),
      add_change_to_dump_memory(Key, {Document, Bucket, ToDel});
    {#document{key = Key} = Document, ToDel} ->
      save_link_doc(ModelConfig, Document),
      add_change_to_dump_memory(Key, {Document, Bucket, ToDel});
    {Error, _} ->
      Error
  end;
handle_link_message({get_link_doc, [DocKey]}, _Driver, _FD, ModelConfig) ->
  get_link_doc(ModelConfig, DocKey);
handle_link_message({get_link_doc, [BucketOverride, DocKey]}, _Driver, _FD,
    ModelConfig) ->
  get_link_doc(ModelConfig, BucketOverride, DocKey);
handle_link_message({exists_link_doc, [DocKey, Scope]}, _Driver, _FD,
    ModelConfig) ->
  exists_link_doc(ModelConfig, DocKey, Scope);
handle_link_message({foreach_link, Args}, Driver, _FD, ModelConfig) ->
  put(mcd_driver, ?MODULE),
  Ans = apply(Driver, foreach_link, [ModelConfig | Args]),
  case Ans of
    {ok, _} ->
      put(foreach_restore, true);
    _ ->
      ok
  end,
  Ans;
handle_link_message({add_links, [Key, Links, LinkReplicaScope]}, Driver, FD,
    ModelConfig) ->
  handle_link_message({add_links, [Key, Links]}, Driver, FD,
    ModelConfig#model_config{link_replica_scope = LinkReplicaScope});
handle_link_message({Op, Args}, Driver, _FD, ModelConfig) ->
  put(mcd_driver, ?MODULE),
  apply(Driver, Op, [ModelConfig | Args]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Merge similar operations on links.
%% @end
%%--------------------------------------------------------------------
-spec merge_link_ops(Messages :: [memory_store_driver:message()]) ->
  [{Message :: memory_store_driver:message(), Count :: non_neg_integer()}].
merge_link_ops([M1 | Messages]) ->
  {LM, MN, AggReversed} = lists:foldl(fun(M, {LastM, MergedNum, Acc}) ->
    case merge_link_ops(LastM, M) of
      {merged, NewM} ->
        {NewM, MergedNum + 1, Acc};
      different ->
        {M, 1, [{LastM, MergedNum} | Acc]}
    end
  end, {M1, 1, []}, Messages),

  lists:reverse([{LM, MN} | AggReversed]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Merge two operations on links if they are similar.
%% @end
%%--------------------------------------------------------------------
-spec merge_link_ops({Op1 :: atom(), Args1 :: list()}, {Op2 :: atom(),
  Args2 :: list()}) -> {merged, MergedMessage :: term()} | different.
merge_link_ops({fetch_link, _}, _) ->
  different;
merge_link_ops({foreach_link, _}, _) ->
  different;
merge_link_ops({get_link_doc, _}, _) ->
  different;
merge_link_ops({exists_link_doc, _}, _) ->
  different;
merge_link_ops(_, {fetch_link, _}) ->
  different;
merge_link_ops(_, {foreach_link, _}) ->
  different;
merge_link_ops(_, {get_link_doc, _}) ->
  different;
merge_link_ops(_, {exists_link_doc, _}) ->
  different;
merge_link_ops({Op, [A1, [] | T]}, {Op, [A1, L2 | T]}) when is_list(L2) ->
  {merged, {Op, [A1, L2 | T]}};
merge_link_ops({Op, [A1, [H | _] = L1 | T]}, {Op, [A1, L2 | T]})
  when is_tuple(H), is_list(L2) ->
  Merged = lists:foldl(fun({K, _V} = Link, Acc) ->
    case proplists:lookup(K, Acc) of
      none ->
        [Link | Acc];
      _ ->
        Acc
    end
  end, L2, L1),
  {merged, {Op, [A1, Merged | T]}};
merge_link_ops({Op, [A1, L1 | T]}, {Op, [A1, L2 | T]})
  when is_list(L1), is_list(L2) ->
  {merged, {Op, [A1, L1 ++ (L2 -- L1) | T]}};
merge_link_ops(_, _) ->
  different.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves operation in memory.
%% @end
%%--------------------------------------------------------------------
-spec add_change_to_memory(datastore:ext_key(),
    Op :: atom() | {atom(), term()}) -> ok.
add_change_to_memory(Key, Op) ->
  Value = get_value(op_cache),
  put(op_cache, [{Key, Op} | proplists:delete(Key, Value)]),
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves key to be dumped to store in memory.
%% @end
%%--------------------------------------------------------------------
-spec add_change_to_dump_memory(datastore:ext_key(), term()) ->
  ok.
add_change_to_dump_memory(Key, Change) ->
  Value = get_value(dump_cache),
  put(dump_cache, [{Key, Change} | proplists:delete(Key, Value)]),
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves document to be restored from disk to memory.
%% @end
%%--------------------------------------------------------------------
-spec add_restore_to_memory(datastore:ext_key(), datastore:value(),
    FetchFilter :: boolean()) -> ok.
add_restore_to_memory(Key, V, FetchFilter) ->
  Message = get(current_message),
  add_restore_to_memory(Key, V, Message, FetchFilter).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves document to be restored from disk to memory.
%% @end
%%--------------------------------------------------------------------
-spec add_restore_to_memory(datastore:ext_key(), datastore:value(),
    Op :: atom(), FetchFilter :: boolean()) -> ok.
add_restore_to_memory(_Key, _V, clear, _FetchFilter) ->
  ok;
add_restore_to_memory(Key, V, {fetch_link, _}, true) ->
  add_restore_to_memory(Key, V);
add_restore_to_memory(Key, V, {foreach_link, _}, true) ->
  add_restore_to_memory(Key, V);
add_restore_to_memory(_Key, _V, _Message, true) ->
  ok;
add_restore_to_memory(Key, V, _, _) ->
  add_restore_to_memory(Key, V).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves document to be restored from disk to memory.
%% @end
%%--------------------------------------------------------------------
-spec add_restore_to_memory(datastore:ext_key(), datastore:value()) ->
  ok.
add_restore_to_memory(Key, V) ->
  Value = get_value(restore_cache),
  put(restore_cache, [{Key, V} | proplists:delete(Key, Value)]),
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves document in process memory to be cached by tp process.
%% @end
%%--------------------------------------------------------------------
-spec add_doc_to_memory(datastore:ext_key(), datastore:value()) ->
  ok.
add_doc_to_memory(Key, Doc) ->
  Value = get_value(doc_cache),
  put(doc_cache, [{Key, Doc} | proplists:delete(Key, Value)]),
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Revers document change in memory after failure.
%% @end
%%--------------------------------------------------------------------
-spec reverse_doc_change_in_memory(datastore:ext_key(), datastore:value()) ->
  ok.
reverse_doc_change_in_memory(Key, OldValue) ->
  Value = get_value(doc_cache),
  case proplists:get_value(Key, OldValue, undefined) of
    undefined ->
      put(doc_cache, proplists:delete(Key, Value));
    OldV ->
      put(doc_cache, [{Key, OldV} | proplists:delete(Key, Value)])
  end,
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets document from process memory.
%% @end
%%--------------------------------------------------------------------
-spec get_doc_from_memory(datastore:ext_key()) -> datastore:value() | undefined.
get_doc_from_memory(Key) ->
  case get(doc_cache) of
    undefined -> undefined;
    List ->
      proplists:get_value(Key, List, undefined)
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets value from process memory.
%% @end
%%--------------------------------------------------------------------
-spec get_value(atom()) -> list().
get_value(Key) ->
  case get(Key) of
    undefined -> [];
    List -> List
  end.

%%--------------------------------------------------------------------
%% @doc
%% Gets document that describes links from stores (memory or disk).
%% @end
%%--------------------------------------------------------------------
-spec get_from_stores(model_behaviour:model_config(), datastore:ext_key(),
    BucketOverride:: binary() | undefined) ->
  {ok, datastore:document()} | datastore:get_error().
get_from_stores(#model_config{name = MN} = ModelConfig, Key, BucketOverride) ->
  MemDriver = get(mem_driver),
  Ans = apply(MemDriver, get_link_doc, [ModelConfig, Key]),

  case {Ans, get(flush_driver)} of
    {{ok, #document{value = V}}, _} ->
      add_doc_to_memory(Key, V),
      Ans;
    {{error, {not_found, _}}, undefined} ->
      add_doc_to_memory(Key, not_found),
      Ans;
    {{error, {not_found, _}}, FlushDriver} ->
      CCCUuid = caches_controller:get_cache_uuid(Key, MN),
      case caches_controller:check_cache_consistency_direct(MemDriver,
        CCCUuid, MN) of
        {ok, _, _} ->
          add_doc_to_memory(Key, not_found),
          Ans;
        _ ->
          Ans2 = case BucketOverride of
            undefined -> apply(FlushDriver, get_link_doc, [ModelConfig, Key]);
            _ -> apply(FlushDriver, get_link_doc,
              [ModelConfig, BucketOverride, Key])
          end,
          case Ans2 of
            {ok, #document{value = V2}} ->
              add_doc_to_memory(Key, V2),
              add_restore_to_memory(Key, V2, false),
              Ans2;
            {error, {not_found, _}} ->
              add_doc_to_memory(Key, not_found),
              Ans2
          end
      end;
    {E, _} ->
      E
  end.