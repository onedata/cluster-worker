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
-export([handle_messages/3, update/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Handles operations on documents.
%% @end
%%--------------------------------------------------------------------
-spec handle_messages(Messages :: [memory_store_driver:message()],
    Key :: datastore:ext_key(), Master :: pid()) ->
  {AnsList :: list(), memory_store_driver:change()}.
handle_messages(Messages, Key, Master) ->
  OpAnsReversedFinal = try
    {Ctx, NewValue, OpAnsReversed, AnsToProc} =
      lists:foldl(fun({Ctx0, Op} = _M0, {LastCtx, TmpValue0, AnsList, TmpAnsToProc}) ->
        try
          Ctx = datastore_context:override(mutator_pid, Master, Ctx0),
          M = {Ctx, Op},
          {Saved, TmpValue00} = apply_at_memory_store(Ctx, LastCtx, TmpValue0),
          TmpValue = get_if_needed(M, TmpValue00, Key),
          {OpAns, Value} = handle_message(M, TmpValue),
          case Saved of
            ok ->
              {Ctx, Value, AnsList, [OpAns | TmpAnsToProc]};
            _ ->
              {Ctx, Value, TmpAnsToProc ++ AnsList, [OpAns]}
          end
        catch
          throw:{datastore_cache_error, Error} ->
            ?error_stacktrace("Modify error for key ~p: ~p", [Key, Error]),
            throw({{datastore_cache_error, Error}, AnsList})
        end
    end, {undefined, undefined, [], []}, Messages),

    try
      apply_at_memory_store(undefined, Ctx, NewValue),
      AnsToProc ++ OpAnsReversed
    catch
      throw:{datastore_cache_error, Error2} ->
        [{datastore_cache_error, Error2} | OpAnsReversed]
    end
  catch
    throw:{{datastore_cache_error, DCError}, AList} ->
      lists:duplicate(length(Messages) - length(AList),
        {datastore_cache, DCError}) ++ AList
  end,
  AnsList = lists:reverse(OpAnsReversedFinal),
  {AnsList, memory_store_driver:get_durability_from_memory()}.

%%--------------------------------------------------------------------
%% @doc
%% Updates documents value.
%% @end
%%--------------------------------------------------------------------
-spec update(OldValue :: datastore:value(), Diff :: datastore:document_diff()) ->
  {ok, datastore:value()} | datastore:update_error().
update(OldValue, Diff) when is_map(Diff) ->
  NewValue = maps:merge(datastore_utils:shallow_to_map(OldValue), Diff),
  {ok, datastore_utils:shallow_to_record(NewValue)};
update(OldValue, Diff) when is_function(Diff) ->
  Diff(OldValue).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles single operation on document.
%% @end
%%--------------------------------------------------------------------
-spec handle_message(Messages :: memory_store_driver:message(),
    CurrentValue :: memory_store_driver:value_doc()) -> {Ans,
  NewCurrentValue :: memory_store_driver:value_doc()} when
  Ans :: {ok, boolean() | datastore:ext_key()} | datastore:generic_error().
handle_message({#{resolve_conflicts := false},
  {save, [#document{key = Key} = Document]}}, CurrentValue) ->
  {{ok, Key}, memory_store_driver:update_rev_if_needed(CurrentValue, Document)};
handle_message({_, {save, [Doc]}}, CurrentValue) ->
  case memory_store_driver:resolve_conflict(CurrentValue, Doc) of
    not_changed ->
      {ok, CurrentValue};
    Document ->
      {ok, Document}
  end;
handle_message({_, {create, [#document{key = Key} = Document]}},
    #document{deleted = true} = CurrentValue) ->
  {{ok, Key}, memory_store_driver:update_rev_if_needed(CurrentValue, Document)};
handle_message({_, {create, [_Document]}}, CurrentValue) ->
  {{error, already_exists}, CurrentValue};
handle_message({#{model_name := MN}, {update, [_Key, _Diff]}},
    #document{deleted = true} = CV) ->
  {{error, {not_found, MN}}, CV};
handle_message({_, {update, [Key, Diff]}}, CurrentValue) ->
  try
    case ?MODULE:update(CurrentValue#document.value, Diff) of
      {ok, V2} ->
        {{ok, Key}, CurrentValue#document{value = V2}};
      Error ->
        {Error, CurrentValue}
    end
  catch
    % Update function may throw exception to cancel update
    throw:Thrown ->
      {{throw, Thrown}, CurrentValue}
  end;
handle_message({_, {create_or_update, [#document{key = Key} = Document, _Diff]}},
    #document{deleted = true} = CurrentValue) ->
  {{ok, Key}, memory_store_driver:update_rev_if_needed(CurrentValue, Document)};
handle_message({_, {create_or_update, [#document{key = Key}, Diff]}}, CurrentValue) ->
  try
    case ?MODULE:update(CurrentValue#document.value, Diff) of
      {ok, V2} ->
        {{ok, Key}, CurrentValue#document{value = V2}};
      Error ->
        {Error, CurrentValue}
    end
  catch
    % Update function may throw exception to cancel update
    throw:Thrown ->
      {{throw, Thrown}, CurrentValue}
  end;
handle_message({_, {delete, [_Key, _Pred]}}, #document{deleted = true} = CV) ->
  {ok, CV};
handle_message({_, {delete, [_Key, Pred]}}, CurrentValue) ->
  try
    case Pred() of
      true ->
        {ok, CurrentValue#document{deleted = true}};
      false ->
        {ok, CurrentValue}
    end
  catch
    % Pred function may throw exception
    throw:Thrown ->
      {{throw, Thrown}, CurrentValue}
  end;
handle_message({#{model_name := MN}, {get, [_Key]}},
    #document{deleted = true} = CV) ->
  {{error, {not_found, MN}}, CV};
handle_message({_, {get, [_Key]}}, CurrentValue) ->
  {{ok, CurrentValue}, CurrentValue};
handle_message({_, {exists, [_Key]}}, #document{deleted = true} = CV) ->
  {{ok, false}, CV};
handle_message({_, {exists, [_Key]}}, CurrentValue) ->
  {{ok, true}, CurrentValue}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles single operation on document.
%% @end
%%--------------------------------------------------------------------
-spec get_if_needed(Ctx :: memory_store_driver:ctx(),
    CurrentValue :: memory_store_driver:value_doc(),
    Key :: datastore:ext_key()) -> memory_store_driver:value_doc() | no_return().
% TODO - models work on #document{deleted = true}
get_if_needed({#{generated_uuid := true}, {save, _}}, CurrentValue, _) ->
  CurrentValue;
get_if_needed({#{get_method := GetMethod} = Ctx, _Op}, undefined, Key) ->
  case apply(datastore_cache, GetMethod, [Ctx, Key]) of
    {ok, Doc} ->
      put(mem_value, Doc),
      Doc;
    {ok, _, Doc} ->
      put(mem_value, Doc),
      Doc;
    {error, key_enoent} ->
      DelDoc = #document{key = Key, deleted = true},
      put(mem_value, DelDoc),
      DelDoc;
    {error, _} = Error ->
      throw({datastore_cache_error, Error})
  end;
get_if_needed(_, CurrentValue, _) ->
  CurrentValue.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies changes to memory store.
%% @end
%%--------------------------------------------------------------------
-spec apply_at_memory_store(Ctx :: memory_store_driver:ctx(),
    LastCtx :: memory_store_driver:ctx(),
    NewValue :: memory_store_driver:value_doc()) ->
    {ok | saved, ValueAfterSave :: memory_store_driver:value_doc()} | no_return().
apply_at_memory_store(_Ctx, undefined, NewValue) ->
  {ok, NewValue};
apply_at_memory_store(Ctx, Ctx, NewValue) ->
  {ok, NewValue};
apply_at_memory_store(_Ctx, LastCtx, NewValue) ->
  case NewValue =:= get(mem_value) of
    true ->
      {ok, NewValue};
    _ ->
      ToSave = memory_store_driver:increment_rev(LastCtx, NewValue),
      case datastore_cache:save(LastCtx, ToSave) of
        {ok, disc, #document{key = Key} = Saved} ->
          put(mem_value, Saved),
          memory_store_driver:del_durability_from_memory(Key),
          {saved, Saved};
        {ok, _, #document{key = Key} = Saved} ->
          put(mem_value, Saved),
          memory_store_driver:add_durability_to_memory(Key, LastCtx),
          {saved, Saved};
        Other -> throw({datastore_cache_error, Other})
      end
  end.
