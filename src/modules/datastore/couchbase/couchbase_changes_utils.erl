%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module utility functions used to get changes from couchbase.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_changes_utils).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_docs/5, get_upper_seq_num/3]).

% If INCLUDE_OVERRIDDEN is true, document is sent every time when sequence connected with document appear in stream.
% If false, document is ignored if sequence is not newest sequence connected with doc.
% Thus, true value may result in sending same version of document multiple times but documents will appear faster.
-define(INCLUDE_OVERRIDDEN, cluster_worker:get_env(include_overridden_seqs_in_changes, true)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns list of documents associated with the changes. Skips documents that
%% has changed in the database since the changes generation (they will be
%% included in the future changes).
%% @end
%%--------------------------------------------------------------------
-spec get_docs([couchbase_changes:change()], couchbase_config:bucket(), datastore_doc:mutator(),
    couchbase_changes:seq(), boolean()) -> [datastore:doc() | {ignored, datastore:doc()}].
get_docs(Changes, Bucket, FilterMutator, MaxSeqNum, IncludeIgnored) ->
    KeyRevisionsAndSequences = lists:filtermap(fun(Change) ->
        Key = maps:get(<<"id">>, Change),
        Value = maps:get(<<"value">>, Change),
        [_, Seq] = maps:get(<<"key">>, Change),
        Rev = maps:get(<<"_rev">>, Value),
        case maps:get(<<"_mutator">>, Value) of
            FilterMutator -> false;
            _ -> {true, {Key, {Rev, Seq}}}
        end
    end, Changes),
    Ctx = #{bucket => Bucket},
    IncludeOverridden = ?INCLUDE_OVERRIDDEN,
    {Keys, RevisionsAnsSequences} = lists:unzip(KeyRevisionsAndSequences),
    lists:filtermap(fun
        ({_Key, {ok, _, #document{ignore_in_changes = true}}, _Rev}) when not IncludeIgnored ->
            false;
        ({_Key, {ok, _, #document{ignore_in_changes = true} = Doc}, _Rev}) ->
            {true, {ignored, Doc}};
        ({_Key, {ok, _, #document{revs = [Rev | _], seq = Seq} = Doc}, {Rev, Seq}}) when Seq =< MaxSeqNum ->
            {true, Doc};
        ({_Key, {ok, _, #document{seq = DocSeq} = Doc}, {_Rev, Seq}}) when Seq =< MaxSeqNum andalso Seq < DocSeq andalso IncludeOverridden ->
            % Use newer doc with old revision - otherwise constant modifications can prevent returning of doc
            {true, Doc#document{seq = Seq}};
        ({_Key, {ok, _, #document{}}, _Rev}) ->
            false;
        ({Key, {error, not_found}, Rev}) ->
            ?debug("Document ~p not found in changes stream in revision ~p", [Key, Rev]),
            false;
        ({Key, Error, Rev}) ->
            ?error("Document ~p (revision ~p) get error ~p", [Key, Rev, Error]),
            throw({get_error, Key})
    end, lists:zip3(Keys, couchbase_driver:get(Ctx, Keys), RevisionsAnsSequences)).

%%--------------------------------------------------------------------
%% @doc
%% Returns largest sequence number connected with changes request.
%% If returned batch is full - it is calculated from batch. Otherwise,
%% last requested number is returned as lower numbers of changes in batch
%% than requested means that there are no more changes up to requested number.
%% @end
%%--------------------------------------------------------------------
-spec get_upper_seq_num([couchbase_changes:change()], non_neg_integer(),
    couchbase_changes:until()) -> non_neg_integer().
get_upper_seq_num(Changes, BatchSize, LastRequested) ->
    case length(Changes) of
        BatchSize ->
            lists:foldl(fun(Change, Acc) ->
                [_, Seq] = maps:get(<<"key">>, Change),
                max(Seq, Acc)
            end, 0, Changes);
        _ ->
            LastRequested
    end.