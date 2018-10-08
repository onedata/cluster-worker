%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides datastore utility functions.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_utils).
-author("Krzysztof Trzepla").

%% API
-export([gen_key/0, gen_key/2, gen_rev/1, parse_rev/1, is_greater_rev/2]).
-export([hex/1, gen_hex/1]).

-type hex() :: binary().
-type key() :: datastore:key().
-type rev() :: datastore_doc:rev().

-export_type([hex/0]).

-define(KEY_LENGTH,
    application:get_env(cluster_worker, datastore_doc_key_length, 16)).
-define(REV_LENGTH,
    application:get_env(cluster_worker, datastore_doc_rev_length, 16)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Generates random datastore key.
%% @end
%%--------------------------------------------------------------------
-spec gen_key() -> key().
gen_key() ->
    gen_hex(?KEY_LENGTH).

%%--------------------------------------------------------------------
%% @doc
%% Generates datastore key based on provided seed and other datastore key.
%% @end
%%--------------------------------------------------------------------
-spec gen_key(binary(), key()) -> key().
gen_key(Seed, Key) when is_binary(Seed) ->
    Ctx = crypto:hash_init(md5),
    Ctx2 = crypto:hash_update(Ctx, Seed),
    Ctx3 = crypto:hash_update(Ctx2, Key),
    hex(crypto:hash_final(Ctx3)).

%%--------------------------------------------------------------------
%% @doc
%% Generates revision with provided generation and random hash.
%% @end
%%--------------------------------------------------------------------
-spec gen_rev(pos_integer()) -> rev().
gen_rev(Generation) ->
    Hash = gen_hex(?REV_LENGTH),
    <<(integer_to_binary(Generation))/binary, "-", Hash/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Returns generation and hash of provided revision.
%% @end
%%--------------------------------------------------------------------
-spec parse_rev(rev()) -> {pos_integer(), hex()}.
parse_rev(Rev) ->
    [Generation, Hash] = binary:split(Rev, <<"-">>),
    {binary_to_integer(Generation), Hash}.

%%--------------------------------------------------------------------
%% @doc
%% Returns 'true' if Rev1 is greater than Rev2, otherwise 'false'.
%% @end
%%--------------------------------------------------------------------
-spec is_greater_rev(rev(), rev()) -> boolean().
is_greater_rev(Rev1, Rev2) ->
    {Gen1, Hash1} = datastore_utils:parse_rev(Rev1),
    {Gen2, Hash2} = datastore_utils:parse_rev(Rev2),
    case {Gen1 > Gen2, Gen1 < Gen2, Hash1 > Hash2, Hash1 < Hash2} of
        {true, false, _, _} -> true;
        {false, true, _, _} -> false;
        {false, false, true, false} -> true;
        {false, false, false, true} -> false;
        % TODO VFS-4145 - change to false when remote driver flushes documents
        {false, false, false, false} -> true
    end.

%%--------------------------------------------------------------------
%% @doc
%% Converts binary digest to a binary hex string.
%% @end
%%--------------------------------------------------------------------
-spec hex(binary()) -> hex().
% TODO - VFS-4904 - very slow
hex(Digest) ->
    Hex = {$0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $a, $b, $c, $d, $e, $f},
    <<
        <<(element(B bsr 4 + 1, Hex)), (element(B band 16#0F + 1, Hex))>> ||
        <<B:8>> <= Digest
    >>.

%%--------------------------------------------------------------------
%% @doc
%% Generates random binary hex string of given size.
%% @end
%%--------------------------------------------------------------------
-spec gen_hex(non_neg_integer()) -> hex().
gen_hex(Size) ->
    hex(crypto:strong_rand_bytes(Size)).