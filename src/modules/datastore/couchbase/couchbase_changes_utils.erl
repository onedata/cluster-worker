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
-export([get_docs/4, get_upper_seq_num/3]).

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
    couchbase_changes:seq()) -> [datastore:doc()].
get_docs(Changes, Bucket, FilterMutator, MaxSeqNum) ->
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
    {Keys, RevisionsAnsSequences} = lists:unzip(KeyRevisionsAndSequences),
    lists:filtermap(fun
        ({{ok, _, #document{revs = [Rev | _], seq = Seq} = Doc}, {Rev, Seq}}) when Seq =< MaxSeqNum ->
            {true, Doc};
        ({{ok, _, #document{}}, _Rev}) ->
            false;
        ({{error, not_found}, Rev}) ->
            ?debug("Document not found in changes stream in revision ~p", [Rev]),
            false
    end, lists:zip(couchbase_driver:get(Ctx, Keys), RevisionsAnsSequences)).

%%--------------------------------------------------------------------
%% @doc
%% Returns largest sequence number from list of changes.
%% @end
%%--------------------------------------------------------------------
-spec get_upper_seq_num([couchbase_changes:change()], non_neg_integer(),
    couchbase_changes:until()) -> non_neg_integer().
get_upper_seq_num(Changes, BatchSize, LastRequested) ->
    case length(Changes) of
        BatchSize ->
            LastSeq = lists:foldl(fun(Change, Acc) ->
                [_, Seq] = maps:get(<<"key">>, Change),
                case Seq > Acc of
                    true -> Seq;
                    false -> Acc
                end
            end, 0, Changes),
            LastSeq;
        _ ->
            LastRequested
    end.