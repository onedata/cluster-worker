%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides CouchBase document management functions.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_doc).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").

%% API
-export([set_mutator/2, set_next_seq/3, set_next_rev/2]).

-type hash() :: datastore_utils2:hex().
-export_type([hash/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Stores mutator in a document.
%% @end
%%--------------------------------------------------------------------
-spec set_mutator(couchbase_driver:ctx(), datastore:doc()) -> datastore:doc().
set_mutator(#{mutator := Mutator}, #document2{mutator = Mutators} = Doc) ->
    Length = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_mutator_history_length, 20),
    Doc#document2{mutator = lists:sublist([Mutator | Mutators], Length)};
set_mutator(_Ctx, Doc) ->
    Doc.

%%--------------------------------------------------------------------
%% @doc
%% Creates and stores next sequence number in a document.
%% @end
%%--------------------------------------------------------------------
-spec set_next_seq(cberl:connection(), couchbase_driver:ctx(),
    datastore:doc()) -> datastore:doc().
set_next_seq(_Connection, #{no_seq := true}, Doc) ->
    Doc;
set_next_seq(Connection, Ctx, #document2{key = Key, scope = Scope} = Doc) ->
    SeqKey = couchbase_changes:get_seq_key(Scope),
    {ok, Seq} = couchbase_crud:update_counter(Connection, SeqKey, 1, 1),
    ChangeKey = couchbase_changes:get_change_key(Scope, Seq),
    [{ChangeKey, ok}] = couchbase_crud:save(Connection, [{Ctx, ChangeKey, Key}]),
    Doc#document2{seq = Seq}.

%%--------------------------------------------------------------------
%% @doc
%% Creates and stores next revision hash in a document.
%% Returns updated document and its EJSON encoded counterpart.
%% @end
%%--------------------------------------------------------------------
-spec set_next_rev(couchbase_driver:ctx(), datastore:doc()) ->
    {datastore:doc(), datastore_json2:ejson()}.
set_next_rev(#{no_rev := true}, Doc) ->
    {Doc, datastore_json2:encode(Doc)};
set_next_rev(_Ctx, #document2{rev = Revs} = Doc) ->
    {Props} = EJson = datastore_json2:encode(Doc),
    Rev = create_rev(EJson),
    Length = application:get_env(?CLUSTER_WORKER_APP_NAME,
        couchbase_revision_history_length, 20),
    Revs2 = lists:sublist([Rev | Revs], Length),

    Doc2 = Doc#document2{rev = Revs2},
    Props2 = lists:keystore(<<"_rev">>, 1, Props, {<<"_rev">>, Revs2}),
    {Doc2, {Props2}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates revision hash for a document.
%% @end
%%--------------------------------------------------------------------
-spec create_rev(datastore_json2:ejson()) -> hash().
create_rev({Props} = EJson) ->
    {Gen, Hash} = parse_last_rev(EJson),
    Props2 = lists:filter(fun
        ({<<"_deleted">>, _}) -> true;
        ({<<"_record">>, _}) -> true;
        ({<<"_version">>, _}) -> true;
        ({<<"_", _/binary>>, _}) -> false;
        ({_, _}) -> true
    end, Props),

    Ctx = crypto:hash_init(md5),
    Ctx2 = crypto:hash_update(Ctx, integer_to_binary(size(Hash))),
    Ctx3 = crypto:hash_update(Ctx2, Hash),
    Ctx4 = crypto:hash_update(Ctx3, jiffy:encode({Props2})),
    Digest = crypto:hash_final(Ctx4),

    Gen2 = Gen + 1,
    Hash2 = datastore_utils2:hex(Digest),
    <<(integer_to_binary(Gen2))/binary, "-", Hash2/binary>>.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns generation and hash of the last document revision.
%% @end
%%--------------------------------------------------------------------
-spec parse_last_rev(datastore_json2:ejson()) -> {non_neg_integer(), hash()}.
parse_last_rev({Props}) ->
    case lists:keyfind(<<"_rev">>, 1, Props) of
        {<<"_rev">>, []} ->
            {0, <<>>};
        {<<"_rev">>, [Rev | _]} ->
            [Gen, Hash] = binary:split(Rev, <<"-">>),
            {binary_to_integer(Gen), Hash}
    end.