%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides all operations that are supported by a datastore
%%% document. Most of them requires documents batch for execution.
%%% It is also responsible for filling document with contextual
%%% data and resolving modification conflicts.
%%% NOTE! Functions provided by this module are thread safe. In order to achieve
%%% consistency and atomicity they should by called from serialization process
%%% e.g. {@link datastore_writer}.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_doc).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([create/4, save/4, update/4, update/5]).
-export([get/2, fetch/3, fetch_deleted/3, exists/2]).
-export([delete/3, delete/4]).
-export([get_links/4, get_links_trees/2]).

-type ctx() :: datastore:ctx().
-type key() :: undefined | binary().
-type value() :: term().
-type rev() :: binary().
-type seq() :: couchbase_changes:seq().
-type scope() :: binary().
-type mutator() :: binary().
-type version() :: datastore_versions:record_version().
-type doc(Value) :: #document{value :: Value}.
-type diff(Value) :: fun((Value) -> {ok, Value} | {error, term()}).
-type pred(Value) :: fun((Value) -> boolean()).
-type tree_id() :: datastore_links:tree_id().
-type link() :: datastore_links:link().
-type link_name() :: datastore_links:link_name().
-type batch() :: datastore_doc_batch:batch().

-export_type([doc/1]).
-export_type([key/0, value/0, rev/0, seq/0, scope/0, mutator/0, version/0]).
-export_type([diff/1, pred/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates datastore document.
%% @end
%%--------------------------------------------------------------------
-spec create(ctx(), key(), doc(value()), batch()) ->
    {{ok, doc(value())} | {error, term()}, batch()}.
create(Ctx, Key, Doc, Batch) ->
    case datastore_doc_batch:fetch(Ctx, Key, Batch) of
        {{ok, PrevDoc = #document{deleted = true}}, Batch2} ->
            Doc2 = fill(Ctx, Doc, PrevDoc),
            datastore_doc_batch:save(Ctx, Key, Doc2, Batch2);
        {{ok, _}, Batch2} ->
            {{error, already_exists}, Batch2};
        {{error, not_found}, Batch2} ->
            Doc2 = fill(Ctx, Doc),
            datastore_doc_batch:save(Ctx, Key, Doc2, Batch2);
        {{error, Reason}, Batch2} ->
            {{error, Reason}, Batch2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves datastore document.
%% @end
%%--------------------------------------------------------------------
-spec save(ctx(), key(), doc(value()), batch()) ->
    {{ok, doc(value())} | {error, term()}, batch()}.
save(Ctx = #{generated_key := true}, Key, Doc, Batch) ->
    Doc2 = fill(Ctx, Doc),
    datastore_doc_batch:save(Ctx, Key, Doc2, Batch);
save(Ctx, Key, Doc, Batch) ->
    case datastore_doc_batch:fetch(Ctx, Key, Batch) of
        {{ok, PrevDoc}, Batch2} ->
            case resolve_conflict(Ctx, Doc, PrevDoc) of
                {true, Doc2} ->
                    datastore_doc_batch:save(Ctx, Key, Doc2, Batch2);
                false ->
                    {{ok, Doc}, Batch2}
            end;
        {{error, not_found}, Batch2} ->
            case resolve_conflict(Ctx, Doc, #document{}) of
                {true, Doc2} ->
                    datastore_doc_batch:save(Ctx, Key, Doc2, Batch2);
                false ->
                    {{ok, Doc}, Batch2}
            end;
        {{error, Reason}, Batch2} ->
            {{error, Reason}, Batch2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates datastore document.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff(value()), batch()) ->
    {{ok, doc(value())} | {error, term()}, batch()}.
update(Ctx, Key, Diff, Batch) ->
    case datastore_doc_batch:fetch(Ctx, Key, Batch) of
        {{ok, #document{deleted = true}}, Batch2} ->
            {{error, not_found}, Batch2};
        {{ok, PrevDoc}, Batch2} ->
            case apply_diff(Diff, PrevDoc) of
                {ok, Doc} ->
                    Doc2 = fill(Ctx, Doc, PrevDoc),
                    datastore_doc_batch:save(Ctx, Key, Doc2, Batch2);
                {error, Reason} ->
                    {{error, Reason}, Batch2}
            end;
        {{error, Reason}, Batch2} ->
            {{error, Reason}, Batch2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates datastore document. If document doesn't exist creates default one.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff(value()), doc(value()), batch()) ->
    {{ok, doc(value())} | {error, term()}, batch()}.
update(Ctx, Key, Diff, Default, Batch) ->
    case datastore_doc_batch:fetch(Ctx, Key, Batch) of
        {{ok, PrevDoc = #document{deleted = true}}, Batch2} ->
            Doc = fill(Ctx, Default, PrevDoc),
            datastore_doc_batch:save(Ctx, Key, Doc, Batch2);
        {{ok, PrevDoc}, Batch2} ->
            case apply_diff(Diff, PrevDoc) of
                {ok, Doc} ->
                    Doc2 = fill(Ctx, Doc, PrevDoc),
                    datastore_doc_batch:save(Ctx, Key, Doc2, Batch2);
                {error, Reason} ->
                    {{error, Reason}, Batch2}
            end;
        {{error, not_found}, Batch2} ->
            Doc = fill(Ctx, Default),
            datastore_doc_batch:save(Ctx, Key, Doc, Batch2);
        {{error, Reason}, Batch2} ->
            {{error, Reason}, Batch2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns datastore document first using memory only store and fallbacking
%% to persistent store if missing.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), key()) -> {ok, doc(value())} | {error, term()}.
get(#{include_deleted := true} = Ctx, Key) ->
    case datastore_cache:get(Ctx, Key) of
        {ok, #document{value = undefined, deleted = true}} -> {error, not_found};
        {ok, Doc} -> {ok, Doc};
        {error, not_found} -> datastore_writer:fetch(Ctx, Key);
        {error, Reason} -> {error, Reason}
    end;
get(Ctx, Key) ->
    case datastore_cache:get(Ctx, Key) of
        {ok, #document{deleted = true}} -> {error, not_found};
        {ok, Doc} -> {ok, Doc};
        {error, not_found} -> datastore_writer:fetch(Ctx, Key);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks whether datastore document exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(ctx(), key()) -> {ok, boolean()} | {error, term()}.
exists(Ctx, Key) ->
    case get(Ctx, Key) of
        {ok, _Doc} -> {ok, true};
        {error, not_found} -> {ok, false};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns datastore document.
%% @end
%%--------------------------------------------------------------------
-spec fetch(ctx(), key(), undefined | batch()) ->
    {{ok, doc(value())} | {error, term()}, batch()}.
fetch(#{include_deleted := true} = Ctx, Key, Batch) ->
    case fetch_deleted(Ctx, Key, Batch) of
        {{ok, #document{value = undefined, deleted = true}}, Batch2} ->
            {{error, not_found}, Batch2};
        {Result, Batch2} ->
            {Result, Batch2}
    end;
fetch(Ctx, Key, Batch) ->
    case fetch_deleted(Ctx, Key, Batch) of
        {{ok, #document{deleted = true}}, Batch2} ->
            {{error, not_found}, Batch2};
        {Result, Batch2} ->
            {Result, Batch2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns datastore document even if it is marked as deleted.
%% @end
%%--------------------------------------------------------------------
-spec fetch_deleted(ctx(), key(), undefined | batch()) ->
    {{ok, doc(value())} | {error, term()}, batch()}.
fetch_deleted(Ctx, Key, Batch = undefined) ->
    {datastore_cache:get(Ctx, Key), Batch};
fetch_deleted(Ctx, Key, Batch) ->
    datastore_doc_batch:fetch(Ctx, Key, Batch).

%%--------------------------------------------------------------------
%% @doc
%% Removes datastore document.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), key(), batch()) -> {ok | {error, term()}, batch()}.
delete(Ctx, Key, Batch) ->
    delete(Ctx, Key, fun(_) -> true end, Batch).

%%--------------------------------------------------------------------
%% @doc
%% Removes datastore document if predicate is satisfied.
%% @end
%%--------------------------------------------------------------------
-spec delete(ctx(), key(), pred(value()), batch()) ->
    {ok | {error, term()}, batch()}.
delete(Ctx, Key, Pred, Batch) ->
    % TODO VFS-4144 - delete documents on memory_only models immediately
    case datastore_doc_batch:fetch(Ctx, Key, Batch) of
        {{ok, #document{deleted = true}}, Batch2} ->
            {ok, Batch2};
        {{ok, PrevDoc = #document{value = Value}}, Batch2} ->
            try Pred(Value) of
                true ->
                    Doc2 = fill(Ctx, PrevDoc, PrevDoc),
                    Doc3 = Doc2#document{deleted = true},
                    {_, Batch3} = datastore_doc_batch:save(Ctx, Key, Doc3, Batch2),
                    {ok, Batch3};
                false ->
                    {{error, {not_satisfied, Value}}, Batch2}
            catch
                _:Reason ->
                    {{error, {Reason, erlang:get_stacktrace()}}, Batch2}
            end;
        {{error, not_found}, Batch2} ->
            {ok, Batch2};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, Batch2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns datastore document links.
%% @end
%%--------------------------------------------------------------------
-spec get_links(ctx(), key(), tree_id(), [link_name()]) ->
    [{ok, link()} | {error, term()}].
get_links(Ctx, Key, TreeIds, LinkNames) ->
    {ok, ForestIt} = datastore_links_iter:init(Ctx, Key, TreeIds),
    Links = lists:map(fun(LinkName) ->
        {Result, _} = datastore_links:get(LinkName, ForestIt),
        {LinkName, Result}
    end, LinkNames),
    NotFoundLinkNames = lists:filtermap(fun
        ({LinkName, {error, not_found}}) -> {true, LinkName};
        (_) -> false
    end, Links),
    FetchedLinks = datastore_writer:fetch_links(
        Ctx, Key, TreeIds, NotFoundLinkNames
    ),
    FetchedLinks2 = lists:zip(NotFoundLinkNames, FetchedLinks),
    lists:map(fun
        ({_, {ok, MemoryLinks}}) ->
            {ok, MemoryLinks};
        ({LinkName, {error, not_found}}) ->
            {LinkName, Result} = lists:keyfind(LinkName, 1, FetchedLinks2),
            Result;
        ({_, {error, Reason}}) ->
            {error, Reason}
    end, Links).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of IDs of link trees that constitute datastore document links
%% forest.
%% @end
%%--------------------------------------------------------------------
-spec get_links_trees(ctx(), key()) -> {ok, [tree_id()]} | {error, term()}.
get_links_trees(Ctx, Key) ->
    case datastore_links:get_links_trees(Ctx, Key, undefined) of
        {{ok, TreeIds}, _} ->
            {ok, TreeIds};
        {{error, not_found}, _} ->
            datastore_writer:fetch_links_trees(Ctx, Key);
        {{error, Reason}, _} ->
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolve conflict using custom model resolution function.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(ctx(), doc(value()), doc(value())) ->
    {true, doc(value())} | false.
resolve_conflict(#{sync_change := true}, RemoteDoc, #document{revs = []}) ->
    {true, RemoteDoc};
resolve_conflict(#{sync_change := true}, #document{revs = []}, _LocalDoc) ->
    false;
resolve_conflict(Ctx = #{sync_change := true}, RemoteDoc, LocalDoc) ->
    Model = element(1, RemoteDoc#document.value),
    case datastore_model_default:resolve_conflict(
        Model, Ctx, RemoteDoc, LocalDoc
    ) of
        {true, Doc} ->
            {true, fill(Ctx, Doc, LocalDoc)};
        {false, Doc} ->
            {true, Doc};
        ignore ->
            false;
        default ->
            default_resolve_conflict(RemoteDoc, LocalDoc)
    end;
resolve_conflict(Ctx, RemoteDoc, LocalDoc) ->
    {true, fill(Ctx, RemoteDoc, LocalDoc)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves conflict based on revision.
%% @end
%%--------------------------------------------------------------------
-spec default_resolve_conflict(doc(value()), doc(value())) ->
    {true, doc(value())} | false.
default_resolve_conflict(RemoteDoc, LocalDoc) ->
    #document{revs = [RemoteRev | _]} = RemoteDoc,
    #document{revs = [LocalRev | _]} = LocalDoc,
    case datastore_utils:is_greater_rev(RemoteRev, LocalRev) of
        true -> {true, RemoteDoc};
        false -> false
    end.

%%--------------------------------------------------------------------
%% @private
%% @equiv fill(Ctx, Doc, #document{})
%% @end
%%--------------------------------------------------------------------
-spec fill(ctx(), doc(value())) -> doc(value()).
fill(Ctx, Doc) ->
    fill(Ctx, Doc, #document{}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fills document with mutator, scope, version and revision.
%% @end
%%--------------------------------------------------------------------
-spec fill(ctx(), doc(value()), doc(value())) -> doc(value()).
fill(Ctx, Doc, _PrevDoc = #document{revs = Revs}) ->
    Doc2 = Doc#document{deleted = false},
    Doc3 = set_mutator(Ctx, Doc2),
    Doc4 = set_scope(Ctx, Doc3),
    Doc5 = set_version(Doc4),
    case Revs of
        [] -> set_rev(Ctx, Doc5, undefined);
        [PrevRev | _] -> set_rev(Ctx, Doc5, PrevRev)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies diff function.
%% @end
%%--------------------------------------------------------------------
-spec apply_diff(diff(value()), doc(value())) ->
    {ok, doc(value())} | {error, term()}.
apply_diff(Diff, Doc = #document{value = Value}) ->
    try Diff(Value) of
        {ok, Value2} -> {ok, Doc#document{value = Value2}};
        {error, Reason} -> {error, Reason}
    catch
        _:Reason ->
            {error, {Reason, erlang:get_stacktrace()}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets document mutator.
%% @end
%%--------------------------------------------------------------------
-spec set_mutator(ctx(), doc(value())) -> doc(value()).
set_mutator(#{mutator := Mutator}, Doc = #document{mutators = [Mutator | _]}) ->
    Doc;
set_mutator(#{mutator := Mutator}, Doc = #document{mutators = Mutators}) ->
    Length = application:get_env(cluster_worker,
        datastore_doc_mutator_history_length, 1),
    Doc#document{mutators = lists:sublist([Mutator | Mutators], Length)};
set_mutator(_Ctx, Doc) ->
    Doc.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets document scope.
%% @end
%%--------------------------------------------------------------------
-spec set_scope(ctx(), doc(value())) -> doc(value()).
set_scope(#{scope := Scope}, Doc) ->
    Doc#document{scope = Scope};
set_scope(_Ctx, Doc) ->
    Doc.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets document version.
%% @end
%%--------------------------------------------------------------------
-spec set_version(doc(value())) -> doc(value()).
set_version(Doc = #document{value = Value}) when is_tuple(Value) ->
    Model = element(1, Value),
    Doc#document{version = datastore_model_default:get_record_version(Model)};
set_version(Doc) ->
    Doc.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets document revision.
%% @end
%%--------------------------------------------------------------------
-spec set_rev(ctx(), doc(value()), undefined | rev()) -> doc(value()).
set_rev(#{sync_enabled := true}, Doc = #document{revs = []}, undefined) ->
    Doc#document{revs = [datastore_utils:gen_rev(1)]};
set_rev(Ctx, Doc = #document{revs = [Rev | _]}, undefined) ->
    set_rev(Ctx, Doc, Rev);
set_rev(#{sync_enabled := true}, Doc = #document{revs = Revs}, PrevRev) ->
    Length = application:get_env(cluster_worker,
        datastore_doc_revision_history_length, 1),
    {Generation, _} = datastore_utils:parse_rev(PrevRev),
    Rev = datastore_utils:gen_rev(Generation + 1),
    Doc#document{revs = lists:sublist([Rev | Revs], Length)};
set_rev(_Ctx, Doc, _PrevRev) ->
    Doc.