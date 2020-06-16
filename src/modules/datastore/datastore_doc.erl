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
-export([get/3, fetch/3, fetch/4, fetch_deleted/3, exists/3]).
-export([delete/3, delete/4]).
-export([get_links/5, get_links_trees/3]).

-type ctx() :: datastore:ctx().
-type key() :: undefined | datastore_key:key().
-type value() :: term().
-type rev() :: datastore_rev:rev().
-type seq() :: couchbase_changes:seq().
-type timestamp() :: couchbase_changes:timestamp().
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
-export_type([key/0, value/0, rev/0, seq/0, timestamp/0, scope/0, mutator/0, version/0, batch/0]).
-export_type([diff/1, pred/1]).

%%%===================================================================
%%% Batch API
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
    datastore_doc_batch:create(Ctx, Key, Doc2, Batch);
save(Ctx, Key, Doc, Batch) ->
    case datastore_doc_batch:fetch(Ctx, Key, Batch) of
        {{ok, PrevDoc}, Batch2} ->
            case resolve_conflict(Ctx, Doc, PrevDoc) of
                {save, Doc2} ->
                    datastore_doc_batch:save(Ctx, Key, Doc2, Batch2);
                ignore ->
                    {{error, ignored}, Batch2}
            end;
        {{error, not_found}, Batch2} ->
            case resolve_conflict(Ctx, Doc, #document{}) of
                {save, Doc2} ->
                    datastore_doc_batch:save(Ctx, Key, Doc2, Batch2);
                ignore ->
                    {{error, ignored}, Batch2}
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
%% @equiv fetch(Ctx, Key, Batch, false)
%% @end
%%--------------------------------------------------------------------
-spec fetch(ctx(), key(), undefined | batch()) ->
    {{ok, doc(value())} | {error, term()}, batch()}.
fetch(Ctx, Key, Batch) ->
    fetch(Ctx, Key, Batch, false).

%%--------------------------------------------------------------------
%% @doc
%% Returns datastore document.
%% @end
%%--------------------------------------------------------------------
-spec fetch(ctx(), key(), undefined | batch(), boolean()) ->
    {{ok, doc(value())} | {error, term()}, batch()}.
fetch(#{include_deleted := true} = Ctx, Key, Batch, LinkFetch) ->
    case fetch_deleted(Ctx, Key, Batch, LinkFetch) of
        {{ok, #document{value = undefined, deleted = true}}, Batch2} ->
            {{error, not_found}, Batch2};
        {Result, Batch2} ->
            {Result, Batch2}
    end;
fetch(Ctx, Key, Batch, LinkFetch) ->
    case fetch_deleted(Ctx, Key, Batch, LinkFetch) of
        {{ok, #document{deleted = true}}, Batch2} ->
            {{error, not_found}, Batch2};
        {Result, Batch2} ->
            {Result, Batch2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv fetch_deleted(Ctx, Key, Batch, false).
%% @end
%%--------------------------------------------------------------------
-spec fetch_deleted(ctx(), key(), undefined | batch()) ->
    {{ok, doc(value())} | {error, term()}, batch()}.
fetch_deleted(Ctx, Key, Batch) ->
    fetch_deleted(Ctx, Key, Batch, false).

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

%%%===================================================================
%%% Direct access API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns datastore document first using memory only store and fallbacking
%% to persistent store if missing.
%% @end
%%--------------------------------------------------------------------
-spec get(node(), ctx(), key()) -> {ok, doc(value())} | {error, term()}.
get(FetchNode, #{include_deleted := true} = Ctx, Key) ->
    case datastore_cache:get(Ctx, Key) of
        {ok, #document{value = undefined, deleted = true}} -> {error, not_found};
        {ok, Doc} -> {ok, Doc};
        {error, not_found} -> fetch_missing(FetchNode, Ctx, Key);
        {error, Reason2} -> {error, Reason2}
    end;
get(FetchNode, Ctx, Key) ->
    case datastore_cache:get(Ctx, Key) of
        {ok, #document{deleted = true}} -> {error, not_found};
        {ok, Doc} -> {ok, Doc};
        {error, not_found} -> fetch_missing(FetchNode, Ctx, Key);
        {error, Reason2} -> {error, Reason2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks whether datastore document exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(node(), ctx(), key()) -> {ok, boolean()} | {error, term()}.
exists(FetchNode, Ctx, Key) ->
    case get(FetchNode, Ctx, Key) of
        {ok, _Doc} -> {ok, true};
        {error, not_found} -> {ok, false};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns datastore document links.
%% @end
%%--------------------------------------------------------------------
-spec get_links(node(), ctx(), key(), tree_id(), [link_name()]) ->
    [{ok, link()} | {error, term()}].
get_links(FetchNode, Ctx, Key, TreeIds, LinkNames) ->
    try
        MemoryCtx = case {Ctx, node()} of
            {#{disc_driver := undefined}, FetchNode} -> Ctx;
            _ -> Ctx#{disc_driver => undefined, remote_driver => undefined, throw_not_found => true}
        end,
        {ok, ForestIt} = datastore_links_iter:init(MemoryCtx, Key, TreeIds),
        lists:map(fun(LinkName) ->
            {Result, _} = datastore_links:get(LinkName, ForestIt),
            Result
        end, LinkNames)
    catch
        _:_ ->
            datastore_router:execute_on_node(FetchNode,
                datastore_writer, fetch_links, [Ctx, Key, TreeIds, LinkNames])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of IDs of link trees that constitute datastore document links
%% forest.
%% @end
%%--------------------------------------------------------------------
-spec get_links_trees(node(), ctx(), key()) -> {ok, [tree_id()]} | {error, term()}.
get_links_trees(FetchNode, Ctx, Key) ->
    try
        MemoryCtx = case {Ctx, node()} of
            {#{disc_driver := undefined}, FetchNode} -> Ctx;
            _ -> Ctx#{disc_driver => undefined, remote_driver => undefined, throw_not_found => true}
        end,
        case datastore_links:get_links_trees(MemoryCtx, Key, undefined) of
            {{ok, TreeIds}, _} ->
                {ok, TreeIds};
            {{error, Reason}, _} ->
                {error, Reason}
        end
    catch
        _:_ ->
            datastore_router:execute_on_node(FetchNode, datastore_writer, fetch_links_trees, [Ctx, Key])
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to fetch missing document via tp process if needed.
%% @end
%%--------------------------------------------------------------------
-spec fetch_missing(node(), ctx(), key()) -> {ok, doc(value())} | {error, term()}.
fetch_missing(FetchNode, Ctx, Key) ->
    case (maps:get(disc_driver, Ctx, undefined) =/= undefined) orelse (node() =/= FetchNode) of
        true ->
            datastore_router:execute_on_node(FetchNode, datastore_writer, fetch, [Ctx, Key]);
        _ ->
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns datastore document even if it is marked as deleted.
%% @end
%%--------------------------------------------------------------------
-spec fetch_deleted(ctx(), key(), undefined | batch(), boolean()) ->
    {{ok, doc(value())} | {error, term()}, batch()}.
% This case is used for fetching link documents outside tp process
% Errors should be thrown to prevent further processing
fetch_deleted(#{throw_not_found := true} = Ctx, Key, Batch, _) ->
    case datastore_cache:get(Ctx, Key, false) of
        {error, not_found} ->
            % Throw tuple to prevent catching by bp_tree
            throw({fetch_error, not_found});
        Result ->
            {Result, Batch}
    end;
fetch_deleted(Ctx, Key, Batch = undefined, false) ->
    {datastore_cache:get(Ctx, Key, true), Batch};
fetch_deleted(Ctx, Key, Batch = undefined, true) ->
    case datastore_cache:get(Ctx, Key, true) of
        {error, not_found} -> {datastore_cache:get_remote(Ctx, Key), Batch};
        Other -> {Other, Batch}
    end;
fetch_deleted(Ctx, Key, Batch, _) ->
    datastore_doc_batch:fetch(Ctx, Key, Batch).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolve conflict using custom model resolution function.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(ctx(), doc(value()), doc(value())) ->
    {save, doc(value())} | ignore.
resolve_conflict(#{sync_change := true}, RemoteDoc, #document{revs = []}) ->
    {save, RemoteDoc};
resolve_conflict(#{sync_change := true}, #document{revs = []}, _LocalDoc) ->
    ignore;
resolve_conflict(Ctx = #{sync_change := true}, RemoteDoc, LocalDoc) ->
    Model = element(1, RemoteDoc#document.value),
    case datastore_model_default:resolve_conflict(
        Model, Ctx, RemoteDoc, LocalDoc
    ) of
        {true, Doc} ->
            {save, fill(Ctx, Doc, LocalDoc)};
        {false, Doc} ->
            {save, Doc};
        ignore ->
            ignore;
        default ->
            default_resolve_conflict(RemoteDoc, LocalDoc)
    end;
resolve_conflict(Ctx, RemoteDoc, LocalDoc) ->
    {save, fill(Ctx, RemoteDoc, LocalDoc)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves conflict based on revision.
%% @end
%%--------------------------------------------------------------------
-spec default_resolve_conflict(doc(value()), doc(value())) ->
    {save, doc(value())} | ignore.
default_resolve_conflict(RemoteDoc, LocalDoc) ->
    #document{revs = [RemoteRev | _]} = RemoteDoc,
    #document{revs = [LocalRev | _]} = LocalDoc,
    case datastore_rev:is_greater(RemoteRev, LocalRev) of
        true ->
            {save, RemoteDoc};
        false ->
            ignore
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
    Doc#document{revs = [datastore_rev:new(1)]};
set_rev(Ctx, Doc = #document{revs = [Rev | _]}, undefined) ->
    set_rev(Ctx, Doc, Rev);
set_rev(#{sync_enabled := true}, Doc = #document{revs = Revs}, PrevRev) ->
    Length = application:get_env(cluster_worker,
        datastore_doc_revision_history_length, 1),
    {Generation, _} = datastore_rev:parse(PrevRev),
    Rev = datastore_rev:new(Generation + 1),
    Doc#document{revs = lists:sublist([Rev | Revs], Length)};
set_rev(_Ctx, Doc, _PrevRev) ->
    Doc.