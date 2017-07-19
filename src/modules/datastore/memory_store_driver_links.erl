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
-export([handle_link_messages/2, get/2]).

%% API for links
-export([add_links/3, set_links/3, create_link/3, delete_links/4, fetch_link/3,
  foreach_link/4]).

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
    Master :: pid()) ->
  {AnsList :: list(), Changes :: memory_store_driver:change()}.
handle_link_messages(Messages, Master) ->
  Agg = merge_link_ops(Messages),

  FinalAns = try
    {Ctx, Ans, AnsToProc} = lists:foldl(fun
      ({{Ctx0, Op} = _M0, Num}, {LastCtx, TmpAns, TmpAnsToProc}) ->
        try
          Ctx = datastore_context:override(mutator_pid, Master, Ctx0),
          M = {Ctx, Op},
          case apply_at_memory_store(Ctx, LastCtx) of
            ok ->
              A = handle_link_message(M),
              {Ctx, TmpAns, lists:duplicate(Num, A) ++ TmpAnsToProc};
            saved ->
              A = handle_link_message(M),
              {Ctx, TmpAnsToProc ++ TmpAns, lists:duplicate(Num, A)}
          end
        catch
          throw:{datastore_cache_error, Error} ->
            ToMap = lists:duplicate(Num, error) ++ TmpAnsToProc,
            throw({{datastore_cache_error, Error},
              prepare_error({datastore_cache_error, Error}, ToMap) ++ TmpAns})
        end
    end, {undefined, [], []}, Agg),

    try
      apply_at_memory_store(undefined, Ctx),
      AnsToProc ++ Ans
    catch
      throw:{datastore_cache_error, Error2} ->
        prepare_error({datastore_cache_error, Error2}, AnsToProc) ++ Ans
    end
  catch
    throw:{{datastore_cache_error, DCError}, AList} ->
      lists:duplicate(length(Messages) - length(AList),
        {datastore_cache, DCError}) ++ AList
  end,

  {lists:reverse(FinalAns), memory_store_driver:get_durability_from_memory()}.

%%--------------------------------------------------------------------
%% @doc
%% Gets document that describes links (used by router).
%% @end
%%--------------------------------------------------------------------
-spec get(memory_store_driver:ctx(), datastore:ext_key()) ->
  {ok, datastore:document()} | datastore:get_error().
get(#{model_name := MN} = Ctx, Key) ->
  case datastore_cache:get(Ctx, Key) of
    {ok, #document{deleted = true}} ->
      {error, {not_found, MN}};
    Other ->
      Other
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
-spec save_link_doc(memory_store_driver:ctx(), datastore:document()) ->
  {ok, datastore:ext_key()} | datastore:generic_error().
save_link_doc(_Ctx, #document{key = Key} = Doc) ->
  add_doc_to_cache(Key, Doc),
  {ok, Key}.

%%--------------------------------------------------------------------
%% @doc
%% Gets document that describes links (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec get_link_doc(memory_store_driver:ctx(), datastore:ext_key()) ->
  {ok, datastore:document()} | datastore:get_error().
get_link_doc(#{get_method := get_direct, model_name := MN,
  persistence := false} = Ctx, Key) ->
  case datastore_cache:get(Ctx, Key) of
    {error, key_enoent} ->
      {error, {not_found, MN}};
    {ok, #document{deleted = true}} ->
      {error, {not_found, MN}};
    Other ->
      Other
  end;
get_link_doc(#{get_method := get_direct, model_name := MN} = Ctx, Key) ->
  case datastore_cache:get(Ctx, Key) of
    {error, key_enoent} ->
      {error, {not_found_in_memory, MN}};
    {ok, #document{deleted = true}} ->
      {error, {not_found, MN}};
    Other ->
      Other
  end;
get_link_doc(#{model_name := MN} = Ctx, Key) ->
  case get_doc(Ctx, Key) of
    #document{deleted = true} ->
      {error, {not_found, MN}};
    Cached ->
      {ok, Cached}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes document that describes links, not using transactions
%% (used by links utils).
%% @end
%%--------------------------------------------------------------------
-spec delete_link_doc(memory_store_driver:ctx(), datastore:document()) ->
  ok | datastore:generic_error().
delete_link_doc(_Ctx, #document{key = Key} = Document) ->
  add_doc_to_cache(Key, Document#document{deleted = true}),
  ok.

%%%===================================================================
%%% Link operations functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback add_links/3.
%% @end
%%--------------------------------------------------------------------
-spec add_links(memory_store_driver:ctx(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
  ok | datastore:generic_error().
add_links(Ctx, Key, Links) ->
  links_utils:save_links_maps(memory_store_driver_links, Ctx, Key, Links, add).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback set_links/3.
%% @end
%%--------------------------------------------------------------------
-spec set_links(memory_store_driver:ctx(), datastore:ext_key(), [datastore:normalized_link_spec()]) ->
  ok | datastore:generic_error().
set_links(Ctx, Key, Links) ->
  links_utils:save_links_maps(memory_store_driver_links, Ctx, Key, Links, set).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback create_link/3.
%% @end
%%--------------------------------------------------------------------
-spec create_link(memory_store_driver:ctx(), datastore:ext_key(), datastore:normalized_link_spec()) ->
  ok | datastore:create_error().
create_link(Ctx, Key, Link) ->
  links_utils:create_link_in_map(memory_store_driver_links, Ctx, Key, Link).

%%--------------------------------------------------------------------
%% @doc
%% Simmilar to {@link store_driver_behaviour} callback delete_links/3 witch delete predicate.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(memory_store_driver:ctx(), datastore:ext_key(), [datastore:link_name()] | all,
    datastore:delete_predicate()) -> ok | datastore:generic_error().
delete_links(Ctx, Key, Links, Pred) ->
  case Pred() of
    true ->
      ok = links_utils:delete_links(memory_store_driver_links,
        Ctx, Key, Links);
    false ->
      ok
  end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback fetch_link/3.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(memory_store_driver:ctx(), datastore:ext_key(), datastore:link_name()) ->
  {ok, datastore:link_target()} | datastore:link_error().
fetch_link(Ctx, Key, LinkName) ->
  links_utils:fetch_link(memory_store_driver_links, Ctx, LinkName, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback foreach_link/4.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(memory_store_driver:ctx(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
  {ok, Acc :: term()} | datastore:link_error().
foreach_link(Ctx, Key, Fun, AccIn) ->
  try
    links_utils:foreach_link(memory_store_driver_links, Ctx, Key, Fun, AccIn)
  catch
    throw:Thrown ->
      {throw, Thrown}
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies changes to memory store.
%% @end
%%--------------------------------------------------------------------
-spec apply_at_memory_store(Ctx :: memory_store_driver:ctx(),
    LastCtx :: memory_store_driver:ctx()) -> ok | saved | no_return().
apply_at_memory_store(_Ctx, undefined) ->
  ok;
apply_at_memory_store(Ctx, Ctx) ->
  ok;
apply_at_memory_store(_Ctx, LastCtx) ->
  Docs = case get(doc_cache) of
    undefined -> [];
    List -> List
  end,

  % TODO - incosistency if error occured at part of documents
  lists:foreach(fun
    ({Key, Doc}) ->
      case Doc =:= get_doc_from_memory(Key) of
        true ->
          ok;
        _ ->
          Scope = maps:get(scope, LastCtx, <<>>), % Scope may be not present in Ctx
          ToSave0 = memory_store_driver:increment_rev(LastCtx, Doc),
          MC = links:model_init(),

          ToSave = case Scope of
            <<>> ->
              ToSave0#document{version = MC#model_config.version};
            _ ->
              ToSave0#document{scope = Scope, version = MC#model_config.version}
          end,

          % TODO - delete with new links
          SaveCtx = case catch binary:match(Key, ?NOSYNC_KEY_OVERRIDE_PREFIX) of
            {0, _} ->
              case maps:get(disc_driver_ctx, LastCtx, undefined) of
                DiskCtx when is_map(DiskCtx) ->
                  datastore_context:override(disc_driver_ctx,
                    datastore_context:override(no_seq, true, DiskCtx), LastCtx);
                _ ->
                  LastCtx
              end;
            _ ->
              LastCtx
          end,

          case datastore_cache:save(SaveCtx, ToSave) of
            {ok, disc, #document{key = Key} = Saved} ->
              add_doc_to_memory(Key, Saved),
              add_doc_to_cache(Key, Saved),
              memory_store_driver:del_durability_from_memory(Key);
            {ok, _, #document{key = Key} = Saved} ->
              add_doc_to_memory(Key, Saved),
              add_doc_to_cache(Key, Saved),
              memory_store_driver:add_durability_to_memory(Key, SaveCtx);
            Other ->
              throw({datastore_cache_error, Other})
          end
      end
  end, Docs),
  saved.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Prepare list of errors to answer.
%% @end
%%--------------------------------------------------------------------
-spec prepare_error(DumpAns :: term(), AnsList :: list()) ->
  NewAnsList :: list().
prepare_error(DumpAns, AnsList) ->
  lists:map(fun(_) -> DumpAns end, AnsList).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets document that describes links from stores.
%% @end
%%--------------------------------------------------------------------
-spec get_doc(memory_store_driver:ctx(), datastore:ext_key()) ->
  datastore:document() | datastore:get_error().
get_doc(#{generated_uuid := true}, Key) ->
  DelDoc = #document{key = Key, deleted = true},
  add_doc_to_memory(Key, DelDoc),
  add_doc_to_cache(Key, DelDoc),
  DelDoc;
get_doc(#{get_method := GetMethod} = Ctx, Key) ->
  case get_doc_from_cache(Key) of
    undefined ->
      case apply(datastore_cache, GetMethod, [Ctx, Key]) of
        {ok, Doc} ->
          add_doc_to_memory(Key, Doc),
          add_doc_to_cache(Key, Doc),
          Doc;
        {ok, _, Doc} ->
          add_doc_to_memory(Key, Doc),
          add_doc_to_cache(Key, Doc),
          Doc;
        {error, key_enoent} ->
          DelDoc = #document{key = Key, deleted = true},
          add_doc_to_memory(Key, DelDoc),
          add_doc_to_cache(Key, DelDoc),
          DelDoc;
        {error, _} = Error ->
          throw({datastore_cache_error, Error})
      end;
    Cached ->
      Cached
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handles single link operation.
%% @end
%%--------------------------------------------------------------------
-spec handle_link_message(Ctx :: memory_store_driver:ctx()) ->
  ok | {error, term()}.
handle_link_message({Ctx, {save, [#document{key = Key} = ToSave]}}) ->
  case memory_store_driver:resolve_conflict(get_doc(Ctx, Key), ToSave) of
    not_changed ->
      ok;
    Document ->
      save_link_doc(Ctx, Document),
      ok
  end;
handle_link_message({Ctx, {get, [DocKey]}}) ->
  get_link_doc(Ctx, DocKey);
handle_link_message({Ctx, {delete_links, [Key, LinkNames]}}) ->
  handle_link_message({Ctx, {delete_links, [Key, LinkNames, ?PRED_ALWAYS]}});
handle_link_message({Ctx, {Op, Args}}) ->
  apply(?MODULE, Op, [Ctx | Args]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Merge similar operations on links.
%% @end
%%--------------------------------------------------------------------
-spec merge_link_ops(Messages :: [memory_store_driver:message()]) ->
  [{Message :: memory_store_driver:message(), Count :: non_neg_integer()}].
merge_link_ops([M1 | Messages]) ->
  {LM, MN, AggReversed} = lists:foldl(fun
    ({Ctx, M}, {{Ctx, LastM}, MergedNum, Acc}) ->
      case merge_link_ops(LastM, M) of
        {merged, NewM} ->
          {{Ctx, NewM}, MergedNum + 1, Acc};
        different ->
          {{Ctx, M}, 1, [{{Ctx, LastM}, MergedNum} | Acc]}
      end;
    (M, {LastM, MergedNum, Acc}) ->
      {M, 1, [{LastM, MergedNum} | Acc]}
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
merge_link_ops({get, _}, _) ->
  different;
merge_link_ops({save, _}, _) ->
  different;
merge_link_ops(_, {fetch_link, _}) ->
  different;
merge_link_ops(_, {foreach_link, _}) ->
  different;
merge_link_ops(_, {get, _}) ->
  different;
merge_link_ops(_, {save, _}) ->
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
%% Saves document in process memory to be cached by tp process.
%% @end
%%--------------------------------------------------------------------
-spec add_doc_to_memory(datastore:ext_key(), memory_store_driver:value()) ->
  ok.
add_doc_to_memory(Key, Doc) ->
  memory_store_driver:add_to_proc_mem(mem_cache, Key, Doc).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets document from process memory.
%% @end
%%--------------------------------------------------------------------
-spec get_doc_from_memory(datastore:ext_key()) -> memory_store_driver:value() | undefined.
get_doc_from_memory(Key) ->
  memory_store_driver:get_from_proc_mem(mem_cache, Key).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves document in process memory to be cached by tp process.
%% @end
%%--------------------------------------------------------------------
-spec add_doc_to_cache(datastore:ext_key(), memory_store_driver:value()) ->
  ok.
add_doc_to_cache(Key, Doc) ->
  OldDoc = get_doc_from_cache(Key),
  ToSave = memory_store_driver:update_rev_if_needed(OldDoc, Doc),
  memory_store_driver:add_to_proc_mem(doc_cache, Key, ToSave).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets document from process memory.
%% @end
%%--------------------------------------------------------------------
-spec get_doc_from_cache(datastore:ext_key()) -> memory_store_driver:value() | undefined.
get_doc_from_cache(Key) ->
  memory_store_driver:get_from_proc_mem(doc_cache, Key).
