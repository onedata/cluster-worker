%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Helper for memory_store_driver for doc operations.
%%% @end
%%%-------------------------------------------------------------------
-module(memory_store_driver_docs).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").
-include("modules/tp/tp.hrl").
-include("modules/datastore/memory_store_driver.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([handle_messages/6, clear/3]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles operations on documents.
%% @end
%%--------------------------------------------------------------------
-spec handle_messages(Messages :: [model_behaviour:message()],
    CurrentValue :: model_behaviour:value_doc(), Driver :: atom(), FD :: atom(),
    ModelConfig :: model_behaviour:model_config(), Key :: datastore:ext_key()) ->
  {AnsList :: list(), NewCurrentValue :: model_behaviour:value_doc(),
    Status :: memory_store_driver:change()} | no_return().
handle_messages(Messages, CurrentValue0, Driver, FD, ModelConfig, Key) ->
  {CurrentValue, Restored} = case CurrentValue0 of
    undefined ->
      case get_from_memory(Driver, FD, ModelConfig, Key) of
        {error, _} = Error ->
          throw({{get_error, Error}, CurrentValue0, ok});
        GetAns ->
          GetAns
      end;
    _ ->
      {CurrentValue0, false}
  end,

  {NewValue, DiskValue, RestoreMem, OpAnsReversed} =
    lists:foldl(fun(M, {TmpValue, TmpDiskValue, TmpRestoreMem, AnsList}) ->
    OpAns = handle_message(M, TmpValue, Driver, FD, ModelConfig),
    {NTV, NDV, NRM} =
      translate_handle_ans(OpAns, TmpValue, TmpDiskValue, TmpRestoreMem),
    {NTV, NDV, NRM, [{M, OpAns} | AnsList]}
  end, {CurrentValue, CurrentValue, false, []}, Messages),

  apply_at_memory_store(ModelConfig, Driver, Key,
    NewValue, CurrentValue, Restored, RestoreMem),

  AnsList = map_ans_list(Key, lists:reverse(OpAnsReversed)),

  case NewValue =/= CurrentValue of
    true ->
      case DiskValue =/= CurrentValue of
        true ->
          {AnsList, NewValue, {to_save, DiskValue}};
        _ ->
          {AnsList, NewValue, to_save}
      end;
    _ ->
      {AnsList, NewValue, ok}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Handles clear operation.
%% @end
%%--------------------------------------------------------------------
-spec clear(Driver :: atom(), ModelConfig :: model_behaviour:model_config(),
    Key :: datastore:ext_key()) -> ok | datastore:generic_error().
clear(Driver, #model_config{name = MN} = ModelConfig, Key) ->
  % TODO - race at delete
  case caches_controller:save_consistency_info(
    memory_store_driver:driver_to_level(Driver), MN, Key) of
    true ->
      apply(Driver, delete, [ModelConfig, Key, ?PRED_ALWAYS]);
    _ ->
      {error, consistency_info_save_failed}
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles single operation on document.
%% @end
%%--------------------------------------------------------------------
-spec handle_message(Messages :: model_behaviour:message(),
    CurrentValue :: model_behaviour:value_doc(), Driver :: atom(), FD :: atom(),
    ModelConfig :: model_behaviour:model_config()) -> {ok | memory_restore,
  NewCurrentValue :: model_behaviour:value_doc()} | {disk_save,
  NewCurrentValue :: model_behaviour:value_doc(), DiskAction :: term()} |{error, term()}.
handle_message({save, [Document]}, _CurrentValue, _Driver, _FD, _ModelConfig) ->
  {ok, Document};
handle_message({force_save, Args}, CurrentValue, _Driver, FD, ModelConfig) ->
  [Bucket, ToSave] = case Args of
    [TS] ->
      [FD:select_bucket(ModelConfig, TS), TS];
    _ ->
      Args
  end,
  case memory_store_driver:resolve_conflict(ModelConfig, FD, ToSave) of
    not_changed ->
      {ok, CurrentValue};
    {#document{} = Document, ToDel} ->
      {disk_save, Document#document{rev = undefined}, {Document, Bucket, ToDel}};
    Error ->
      Error
  end;
handle_message({create, [Document]}, not_found, _Driver, _FD, _ModelConfig) ->
  {ok, Document};
handle_message({create, [_Document]}, _CurrentValue, _Driver, _FD, _ModelConfig) ->
  {error, already_exists};
handle_message({update, [_Key, _Diff]}, not_found, _Driver, _FD,
    #model_config{name = ModelName}) ->
  {error, {not_found, ModelName}};
handle_message({update, [_Key, Diff]}, CurrentValue, Driver, _FD, _ModelConfig) ->
  try
    case apply(Driver, update, [CurrentValue#document.value, Diff]) of
      {ok, V2} ->
        {ok, CurrentValue#document{value = V2}};
      Error ->
        Error
    end
  catch
    % Update function may throw exception to cancel update
    throw:Thrown ->
      {throw, Thrown}
  end;
handle_message({create_or_update, [Document, _Diff]}, not_found, _Driver, _FD,
    _ModelConfig) ->
  {ok, Document};
handle_message({create_or_update, [_Document, Diff]}, CurrentValue, Driver, _FD,
    _ModelConfig) ->
  try
    case apply(Driver, update, [CurrentValue#document.value, Diff]) of
      {ok, V2} ->
        {ok, CurrentValue#document{value = V2}};
      Error ->
        Error
    end
  catch
    % Update function may throw exception to cancel update
    throw:Thrown ->
      {throw, Thrown}
  end;
handle_message({delete, [_Key, _Pred]}, not_found, _Driver, _FD, _ModelConfig) ->
  {ok, not_found};
handle_message({delete, [_Key, Pred]}, CurrentValue, _Driver, _FD, _ModelConfig) ->
  try
    case Pred() of
      true ->
        {ok, not_found};
      false ->
        {ok, CurrentValue}
    end
  catch
    % Pred function may throw exception
    throw:Thrown ->
      {throw, Thrown}
  end;
handle_message({get, [_Key]}, not_found, _Driver, _FD, #model_config{name = ModelName}) ->
  {error, {not_found, ModelName}};
handle_message({get, [_Key]}, CurrentValue, _Driver, _FD, _ModelConfig) ->
  {memory_restore, CurrentValue};
handle_message({exists, [_Key]}, not_found, _Driver, _FD, _ModelConfig) ->
  {ok, false};
handle_message({exists, [_Key]}, CurrentValue, _Driver, _FD, _ModelConfig) ->
  {memory_restore, CurrentValue}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets document from memory if not cached in process.
%% @end
%%--------------------------------------------------------------------
-spec get_from_memory(Driver :: atom(), FlushDriver :: atom(),
    ModelConfig :: model_behaviour:model_config(), Key :: datastore:ext_key()) ->
  {model_behaviour:value_doc() | not_found, boolean()} | {error, term()}.
get_from_memory(Driver, undefined, ModelConfig, Key) ->
  case apply(Driver, get, [ModelConfig, Key]) of
    {error, {not_found, _}} ->
      {not_found, false};
    {ok, #document{} = D} ->
      {D, false};
    Other ->
      Other
  end;
get_from_memory(Driver, FlushDriver, #model_config{name = MN} = ModelConfig, Key) ->
  case apply(Driver, get, [ModelConfig, Key]) of
    {error, {not_found, _}} ->
      case caches_controller:check_cache_consistency(
        memory_store_driver:driver_to_level(Driver), MN) of
        {ok, _, _} ->
          {not_found, false};
        % TODO - simplify memory monitoring
        % (monitor whole memory consistency - not single docs)
        {monitored, ClearedList, _, _} ->
          case lists:member(Key, ClearedList) of
            true ->
              get_from_disk(FlushDriver, ModelConfig, Key);
            _ ->
              {not_found, false}
          end;
        _ ->
          get_from_disk(FlushDriver, ModelConfig, Key)
      end;
    {ok, #document{} = D} ->
      {D, false};
    Other ->
      Other
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets document from disk.
%% @end
%%--------------------------------------------------------------------
-spec get_from_disk(Driver :: atom(),
    ModelConfig :: model_behaviour:model_config(), Key :: datastore:ext_key()) ->
  {model_behaviour:value_doc(), boolean()} | {error, term()}.
get_from_disk(Driver, ModelConfig, Key) ->
  case apply(Driver, get, [ModelConfig, Key]) of
    {ok, Doc} ->
      % TODO - memory store driver understands revisions
      {Doc#document{rev = undefined}, true};
    {error, {not_found, _}} ->
      {not_found, false};
    Error ->
      Error
  end.

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Translates handle answer to structure used in further processing.
%% @end
%%--------------------------------------------------------------------
-spec translate_handle_ans(OpAns :: term(), TmpValue :: model_behaviour:value_doc(),
    TmpDiskValue :: model_behaviour:value_doc(), TmpRestoreMem :: boolean()) ->
    {Value :: model_behaviour:value_doc(), DiskValue :: model_behaviour:value_doc(),
      RestoreMem :: boolean()}.
translate_handle_ans(OpAns, TmpValue, TmpDiskValue, TmpRestoreMem) ->
  case OpAns of
    {ok, false} -> {TmpValue, TmpDiskValue, TmpRestoreMem};
    {ok, NewTmpValue} -> {NewTmpValue, TmpDiskValue, TmpRestoreMem};
    {error, {not_found, _}} -> {not_found, TmpDiskValue, TmpRestoreMem};
    % Newer forceSave will override disk action (its has newer revisions)
    {disk_save, SavedValue, DiskAction} -> {SavedValue, DiskAction, TmpRestoreMem};
    {memory_restore, RestoredValue} -> {RestoredValue, TmpDiskValue, true};
    _ -> {TmpValue, TmpDiskValue, TmpRestoreMem}
  end.

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Maps lists of answers to its final form.
%% @end
%%--------------------------------------------------------------------
-spec map_ans_list(Key :: datastore:ext_key(), List :: list()) -> list().
map_ans_list(Key, List) ->
  lists:map(fun
    ({{delete, _}, {ok, _}}) -> ok;
    ({{exists, _}, {ok, false}}) -> {ok, false};
    ({{exists, _}, {ok, _}}) -> {ok, true};
    ({{exists, _}, {memory_restore, _}}) -> {ok, true};
    ({{get, _}, {memory_restore, GetAns}}) -> {ok, GetAns};
    ({{force_save, _}, {ok, _}}) -> ok;
    ({_, {ok, _}}) -> {ok, Key};
    ({_, {disk_save, _, _}}) -> ok;
    ({_, Err}) -> Err
  end, List).

%%--------------------------------------------------------------------
%% @doc
%% @private
%% Applies changes to memory store.
%% @end
%%--------------------------------------------------------------------
-spec apply_at_memory_store(ModelConfig :: model_behaviour:model_config(),
    Driver :: atom(), Key :: datastore:ext_key(),
    NewValue :: model_behaviour:value_doc(),
    CurrentValue :: model_behaviour:value_doc(),
    Restored :: boolean(), RestoreMem :: boolean()) ->
    ok | no_return().
apply_at_memory_store(ModelConfig, Driver, Key,
    NewValue, CurrentValue, Restored, RestoreMem) ->
  case {NewValue =:= CurrentValue, Restored or RestoreMem} of
    {true, false} ->
      ok;
    {true, true} ->
      case NewValue of
        not_found ->
          ok;
        _ ->
          case apply(Driver, save, [ModelConfig, NewValue]) of
            {ok, Key} -> ok;
            Other -> throw({Other, CurrentValue, ok})
          end
      end;
    _ ->
      case NewValue of
        not_found ->
          case apply(Driver, delete, [ModelConfig, Key, ?PRED_ALWAYS]) of
            ok -> ok;
            Other -> throw({Other, CurrentValue, ok})
          end;
        _ ->
          case apply(Driver, save, [ModelConfig, NewValue]) of
            {ok, Key} -> ok;
            Other -> throw({Other, CurrentValue, ok})
          end
      end
  end.