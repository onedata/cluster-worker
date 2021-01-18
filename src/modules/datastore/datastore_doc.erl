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

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_errors.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([create/4, save/4, save_remote/5, update/4, update/5]).
-export([get/3, fetch/3, fetch/4, fetch_deleted/3, exists/3]).
-export([delete/3, delete/4]).
-export([get_links/5, get_links_trees/3]).
-export([model/1]).

-type ctx() :: datastore:ctx().
-type key() :: undefined | datastore_key:key().
-type value() :: tuple() |  % record defining model - typical value of document
                 binary() | % binary is used by datastore_writer to test couchbase connection
                 undefined. % undefined is used by datastore_cache when storing information
                            % about not existing documents (to prevent calls to couchbase)
-type rev() :: datastore_rev:rev().
-type seq() :: couchbase_changes:seq().
-type remote_seq() :: seq().
% Type describing remote sequence overridden by remote write. The sequence can be
% undefined in case of first remote write performed by particular remote cluster.
-type overridden_seq() :: remote_seq() | undefined.
-type remote_mutation_info() :: #remote_mutation_info{}.
-type timestamp() :: couchbase_changes:timestamp().
-type scope() :: binary().
-type mutator() :: binary().
-type version() :: datastore_versions:record_version().
-type doc(Value) :: #document{value :: Value}.
-type doc() :: doc(value()).
-type diff(Value) :: fun((Value) -> {ok, Value} | {error, term()}).
-type pred(Value) :: fun((Value) -> boolean()).
-type tree_id() :: datastore_links:tree_id().
-type link() :: datastore_links:link().
-type link_name() :: datastore_links:link_name().
-type batch() :: datastore_doc_batch:batch().
% Map storing sequence numbers of remote providers seen during last handling of save_remote/5 function
% (remote document appears with document's seq field value set by remote cluster but couchbase_driver
% will change this field during save to local database so additional map is required to store information
% about sequence numbers set by other clusters).
-type remote_sequences() :: #{mutator() => datastore_doc:remote_seq()}.

-export_type([doc/1]).
-export_type([key/0, value/0, rev/0, seq/0, remote_seq/0, overridden_seq/0,
    remote_mutation_info/0, timestamp/0, scope/0, mutator/0, version/0, batch/0]).
-export_type([diff/1, pred/1]).
-export_type([remote_sequences/0]).

%%%===================================================================
%%% Batch API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates datastore document.
%% @end
%%--------------------------------------------------------------------
-spec create(ctx(), key(), doc(), batch()) ->
    {{ok, doc()} | {error, term()}, batch()}.
create(Ctx, Key, Doc, Batch) ->
    case datastore_doc_batch:fetch(Ctx, Key, Batch) of
        {{ok, PrevDoc = #document{deleted = true}}, Batch2} ->
            Doc2 = fill(Ctx, Doc, PrevDoc),
            datastore_doc_batch:save(Ctx, Key, Doc2, Batch2);
        {{ok, _}, Batch2} ->
            {{error, ?ALREADY_EXISTS}, Batch2};
        {{error, ?NOT_FOUND}, Batch2} ->
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
-spec save(ctx(), key(), doc(), batch()) ->
    {{ok, doc()} | {error, term()}, batch()}.
save(Ctx = #{generated_key := true}, Key, Doc, Batch) ->
    Doc2 = fill(Ctx, Doc),
    datastore_doc_batch:create(Ctx, Key, Doc2, Batch);
save(Ctx, Key, Doc, Batch) ->
    case datastore_doc_batch:fetch(Ctx, Key, Batch) of
        {{ok, PrevDoc}, Batch2} ->
            Doc2 = fill(Ctx, Doc, PrevDoc),
            datastore_doc_batch:save(Ctx, Key, Doc2, Batch2);
        {{error, ?NOT_FOUND}, Batch2} ->
            Doc2 = fill(Ctx, Doc),
            datastore_doc_batch:save(Ctx, Key, Doc2, Batch2);
        {{error, Reason}, Batch2} ->
            {{error, Reason}, Batch2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves datastore document created by other cluster.
%% @end
%%--------------------------------------------------------------------
-spec save_remote(ctx(), key(), doc(), mutator(), batch()) ->
    {{ok, doc(), remote_mutation_info()} | {error, term()}, batch()}.
save_remote(Ctx, Key, Doc, RemoteMutator, Batch) ->
    case datastore_doc_batch:fetch(Ctx, Key, Batch) of
        {{ok, PrevLocalDoc}, Batch2} ->
            case resolve_conflict(Ctx, Doc, PrevLocalDoc, RemoteMutator) of
                {save, ReconciledDoc} ->
                    FinalDoc = update_remote_sequences(ReconciledDoc, Doc, PrevLocalDoc, RemoteMutator),
                    {{ok, SavedDoc}, Batch3} = datastore_doc_batch:save(Ctx, Key, FinalDoc, Batch2),
                    FinalAns = verify_if_remote_doc_existed_and_extend_save_remote_result(
                        SavedDoc, Doc, PrevLocalDoc, RemoteMutator),
                    {FinalAns, Batch3};
                ignore ->
                    FinalAns = verify_if_remote_doc_existed_and_extend_save_remote_result(
                        ?IGNORED, Doc, PrevLocalDoc, RemoteMutator),
                    {FinalAns, Batch2}
            end;
        {{error, ?NOT_FOUND}, Batch2} ->
            EmptyDoc = #document{},
            RemoteMutationInfo = create_remote_mutation_info(Doc, EmptyDoc, RemoteMutator),
            FinalDoc = update_remote_sequences(Doc, Doc, EmptyDoc, RemoteMutator),
            {{ok, SavedDoc}, Batch3} = datastore_doc_batch:save(Ctx, Key, FinalDoc, Batch2),
            {{ok, SavedDoc, RemoteMutationInfo}, Batch3};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, Batch2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates datastore document.
%% @end
%%--------------------------------------------------------------------
-spec update(ctx(), key(), diff(value()), batch()) ->
    {{ok, doc()} | {error, term()}, batch()}.
update(Ctx, Key, Diff, Batch) ->
    case datastore_doc_batch:fetch(Ctx, Key, Batch) of
        {{ok, #document{deleted = true}}, Batch2} ->
            {{error, ?NOT_FOUND}, Batch2};
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
-spec update(ctx(), key(), diff(value()), doc(), batch()) ->
    {{ok, doc()} | {error, term()}, batch()}.
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
        {{error, ?NOT_FOUND}, Batch2} ->
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
    {{ok, doc()} | {error, term()}, batch()}.
fetch(Ctx, Key, Batch) ->
    fetch(Ctx, Key, Batch, false).

%%--------------------------------------------------------------------
%% @doc
%% Returns datastore document.
%% @end
%%--------------------------------------------------------------------
-spec fetch(ctx(), key(), undefined | batch(), boolean()) ->
    {{ok, doc()} | {error, term()}, batch()}.
fetch(#{include_deleted := true} = Ctx, Key, Batch, LinkFetch) ->
    case fetch_deleted(Ctx, Key, Batch, LinkFetch) of
        {{ok, #document{value = undefined, deleted = true}}, Batch2} ->
            {{error, ?NOT_FOUND}, Batch2};
        {Result, Batch2} ->
            {Result, Batch2}
    end;
fetch(Ctx, Key, Batch, LinkFetch) ->
    case fetch_deleted(Ctx, Key, Batch, LinkFetch) of
        {{ok, #document{deleted = true}}, Batch2} ->
            {{error, ?NOT_FOUND}, Batch2};
        {Result, Batch2} ->
            {Result, Batch2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv fetch_deleted(Ctx, Key, Batch, false).
%% @end
%%--------------------------------------------------------------------
-spec fetch_deleted(ctx(), key(), undefined | batch()) ->
    {{ok, doc()} | {error, term()}, batch()}.
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
                    {{error, {?PREDICATE_NOT_SATISFIED, Value}}, Batch2}
            catch
                _:Reason ->
                    {{error, {Reason, erlang:get_stacktrace()}}, Batch2}
            end;
        {{error, ?NOT_FOUND}, Batch2} ->
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
-spec get(node(), ctx(), key()) -> {ok, doc()} | {error, term()}.
get(FetchNode, #{include_deleted := true} = Ctx, Key) ->
    case datastore_cache:get(Ctx, Key) of
        {ok, #document{value = undefined, deleted = true}} -> {error, ?NOT_FOUND};
        {ok, Doc} -> {ok, Doc};
        {error, ?NOT_FOUND} -> fetch_missing(FetchNode, Ctx, Key);
        {error, Reason2} -> {error, Reason2}
    end;
get(FetchNode, Ctx, Key) ->
    case datastore_cache:get(Ctx, Key) of
        {ok, #document{deleted = true}} -> {error, ?NOT_FOUND};
        {ok, Doc} -> {ok, Doc};
        {error, ?NOT_FOUND} -> fetch_missing(FetchNode, Ctx, Key);
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
        {error, ?NOT_FOUND} -> {ok, false};
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

-spec model(value()) -> datastore_model:model().
model(Value) when is_tuple(Value) ->
    element(1, Value);
model(Value) ->
    % Although value() type is `tuple() | binary() | undefined`, binary() and undefined values
    % are used internally by datastore and cannot appear in documents handled by datastore_doc.
    throw({wrong_doc_value_type, Value}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tries to fetch missing document via tp process if needed.
%% @end
%%--------------------------------------------------------------------
-spec fetch_missing(node(), ctx(), key()) -> {ok, doc()} | {error, term()}.
fetch_missing(FetchNode, Ctx, Key) ->
    case (maps:get(disc_driver, Ctx, undefined) =/= undefined) orelse (node() =/= FetchNode) of
        true ->
            datastore_router:execute_on_node(FetchNode, datastore_writer, fetch, [Ctx, Key]);
        _ ->
            {error, ?NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns datastore document even if it is marked as deleted.
%% @end
%%--------------------------------------------------------------------
-spec fetch_deleted(ctx(), key(), undefined | batch(), boolean()) ->
    {{ok, doc()} | {error, term()}, batch()}.
% This case is used for fetching link documents outside tp process
% Errors should be thrown to prevent further processing
fetch_deleted(#{throw_not_found := true} = Ctx, Key, Batch, _) ->
    case datastore_cache:get(Ctx, Key, false) of
        {error, ?NOT_FOUND} ->
            % Throw tuple to prevent catching by bp_tree
            throw({fetch_error, ?NOT_FOUND});
        Result ->
            {Result, Batch}
    end;
fetch_deleted(Ctx, Key, Batch = undefined, false) ->
    {datastore_cache:get(Ctx, Key, true), Batch};
fetch_deleted(Ctx, Key, Batch = undefined, true) ->
    case datastore_cache:get(Ctx, Key, true) of
        {error, ?NOT_FOUND} -> {datastore_cache:get_remote(Ctx, Key), Batch};
        Other -> {Other, Batch}
    end;
fetch_deleted(Ctx, Key, Batch, _) ->
    datastore_doc_batch:fetch(Ctx, Key, Batch).

-spec create_remote_mutation_info(doc(), doc(), mutator()) -> remote_mutation_info().
create_remote_mutation_info(#document{key = Key, value = Value, seq = RemoteSeq} = _RemoteDoc,
    #document{remote_sequences = LocalRemoteSequences} = _LocalDoc,
    RemoteMutator
) ->
    #remote_mutation_info{
        key = Key,
        model = model(Value),
        new_seq = RemoteSeq,
        overridden_seq = maps:get(RemoteMutator, LocalRemoteSequences, undefined)
    }.

-spec update_remote_sequences(doc(), doc(), doc(), mutator()) -> doc().
update_remote_sequences(FinalDoc,
    #document{seq = RemoteSeq} = _RemoteDoc,
    #document{remote_sequences = LocalRemoteSequences} = _LocalDoc, RemoteMutator) ->
    FinalDoc#document{remote_sequences = LocalRemoteSequences#{
        RemoteMutator => RemoteSeq
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function verifies remote mutations and detects if revision or remote sequence has appeared before.
%% In such a case it returns remote_doc_already_exists error. Otherwise, it extends correct answer 
%% with remote_mutation_info or propagates information that document has been ignored.
%% Revision or remote sequence can appear more than once in case of previous error of upper layer or if
%% datastore_remote_driver read document before.
%%--------------------------------------------------------------------
-spec verify_if_remote_doc_existed_and_extend_save_remote_result(doc() | ?IGNORED, doc(), doc(), mutator()) ->
    {ok, doc(), remote_mutation_info()} | {error, ?IGNORED | {?REMOTE_DOC_ALREADY_EXISTS, remote_mutation_info()}}.
verify_if_remote_doc_existed_and_extend_save_remote_result(#document{} = _SaveRemoteResult,
    #document{revs = [Rev | _]} = RemoteDoc,
    #document{revs = [Rev | _]} = LocalDoc,
    RemoteMutator
) ->
    RemoteMutationInfo = create_remote_mutation_info(RemoteDoc, LocalDoc, RemoteMutator),
    {error, {?REMOTE_DOC_ALREADY_EXISTS, RemoteMutationInfo#remote_mutation_info{overridden_seq = undefined}}};
verify_if_remote_doc_existed_and_extend_save_remote_result(SaveRemoteResult, RemoteDoc, LocalDoc, RemoteMutator) ->
    RemoteMutationInfo = create_remote_mutation_info(RemoteDoc, LocalDoc, RemoteMutator),
    case {RemoteMutationInfo, SaveRemoteResult} of
        {#remote_mutation_info{new_seq = Seq, overridden_seq = Seq}, _} ->
            {error, {?REMOTE_DOC_ALREADY_EXISTS, RemoteMutationInfo#remote_mutation_info{overridden_seq = undefined}}};
        {_, #document{}} ->
            {ok, SaveRemoteResult, RemoteMutationInfo};
        {_, ?IGNORED} ->
            {error, ?IGNORED}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolve conflict using custom model resolution function.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(ctx(), doc(), doc(), mutator()) ->
    {save, doc()} | ignore.
resolve_conflict(_Ctx, RemoteDoc, #document{revs = []}, _RemoteMutator) ->
    {save, RemoteDoc};
resolve_conflict(_Ctx, #document{revs = []}, _LocalDoc, _RemoteMutator) ->
    ignore;
resolve_conflict(_Ctx,
    #document{seq = RemoteSeq, revs = [Rev | _]} = _RemoteDoc,
    #document{remote_sequences = LocalRemoteSequences, revs = [Rev | _]} = LocalDoc,
    RemoteMutator
) ->
    case maps:get(RemoteMutator, LocalRemoteSequences, 0) of
        SmallerSeq when SmallerSeq < RemoteSeq ->
            % Remote document has already been saved but its sequence in local database is smaller.
            % It is possible when document has been previously saved by datastore_remote_driver and
            % datastore_remote_driver has read this document before couchbase driver on remote cluster
            % had updated its sequence number. Thus, remote_sequences must be fixed - it will be done
            % saving local document once more (save updates remote sequences).
            %% NOTE: wrong sequence number in local document is possible only for link documents as only 
            %% they can be read by datastore_remote_driver. Such links documents are always written using 
            %% save_remote function.
            {save, LocalDoc};
        _ ->
            ignore
    end;
resolve_conflict(Ctx, RemoteDoc, LocalDoc, _RemoteMutator) ->
    Model = model(RemoteDoc#document.value),
    case datastore_model_default:resolve_conflict(
        Model, Ctx, RemoteDoc, LocalDoc
    ) of
        {true, Doc} ->
            {save, fill(Ctx, Doc, LocalDoc)};
        {false, Doc} ->
            {save, set_remote_sequences(Doc, LocalDoc)};
        ignore ->
            ignore;
        default ->
            default_resolve_conflict(RemoteDoc, LocalDoc)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Resolves conflict based on revision.
%% @end
%%--------------------------------------------------------------------
-spec default_resolve_conflict(doc(), doc()) ->
    {save, doc()} | ignore.
default_resolve_conflict(RemoteDoc, LocalDoc) ->
    #document{revs = [RemoteRev | _]} = RemoteDoc,
    #document{revs = [LocalRev | _]} = LocalDoc,
    case datastore_rev:is_greater(RemoteRev, LocalRev) of
        true ->
            {save, set_remote_sequences(RemoteDoc, LocalDoc)};
        false ->
            ignore
    end.

%%--------------------------------------------------------------------
%% @private
%% @equiv fill(Ctx, Doc, #document{})
%% @end
%%--------------------------------------------------------------------
-spec fill(ctx(), doc()) -> doc().
fill(Ctx, Doc) ->
    fill(Ctx, Doc, #document{}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fills document with mutator, scope, version and revision.
%% @end
%%--------------------------------------------------------------------
-spec fill(ctx(), doc(), doc()) -> doc().
fill(Ctx, Doc, PrevDoc) ->
    % TODO VFS-7076 Test deleted field management
    Doc1 = Doc#document{deleted = false},
    Doc2 = set_remote_sequences(Doc1, PrevDoc),
    Doc3 = set_mutator(Ctx, Doc2),
    Doc4 = set_scope(Ctx, Doc3),
    Doc5 = set_version(Doc4),
    set_rev(Ctx, Doc5, PrevDoc).

%% @private
-spec set_remote_sequences(doc(), doc()) -> doc().
set_remote_sequences(Doc, _PrevDoc = #document{remote_sequences = RemoteSequences}) ->
    Doc#document{remote_sequences = RemoteSequences}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Applies diff function.
%% @end
%%--------------------------------------------------------------------
-spec apply_diff(diff(value()), doc()) ->
    {ok, doc()} | {error, term()}.
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
-spec set_mutator(ctx(), doc()) -> doc().
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
-spec set_scope(ctx(), doc()) -> doc().
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
-spec set_version(doc()) -> doc().
set_version(Doc = #document{value = Value}) when is_tuple(Value) ->
    Model = model(Value),
    Doc#document{version = datastore_model_default:get_record_version(Model)};
set_version(Doc) ->
    Doc.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sets document revision.
%% @end
%%--------------------------------------------------------------------
-spec set_rev(ctx(), doc(), doc() | rev() | undefined) -> doc().
set_rev(Ctx, Doc, _PrevDoc = #document{revs = []}) ->
    set_rev(Ctx, Doc, undefined);
set_rev(Ctx, Doc, _PrevDoc = #document{revs = [PrevRev | _]}) ->
    set_rev(Ctx, Doc, PrevRev);
set_rev(#{sync_enabled := true}, Doc = #document{revs = []}, undefined) ->
    Doc#document{revs = [datastore_rev:new(1)]};
set_rev(#{sync_enabled := true}, Doc = #document{revs = [Rev | _]}, undefined) ->
    set_new_rev(Doc, [Rev]);
set_rev(#{sync_enabled := true}, Doc = #document{revs = []}, PrevRev) ->
    set_new_rev(Doc, [PrevRev]);
set_rev(#{sync_enabled := true}, Doc = #document{revs = [Rev | _]}, PrevRev) ->
    set_new_rev(Doc, [Rev, PrevRev]);
set_rev(_Ctx, Doc, _PrevRev) ->
    Doc.

%% @private
-spec set_new_rev(doc(), [rev()]) -> doc().
set_new_rev(Doc, Revs) ->
    Length = application:get_env(cluster_worker,
        datastore_doc_revision_history_length, 1),
    Generation = lists:max(lists:map(fun(Rev) ->
        {RevGen, _} = datastore_rev:parse(Rev),
        RevGen
    end, Revs)),
    Rev = datastore_rev:new(Generation + 1),
    Doc#document{revs = lists:sublist([Rev | Revs], Length)}.