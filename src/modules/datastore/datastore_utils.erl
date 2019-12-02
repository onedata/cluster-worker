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
-export([gen_key/0, gen_key/1, gen_key/2, gen_rev/1, parse_rev/1, is_greater_rev/2]).
-export([set_expiry/2]).

-type key() :: datastore:key().
-type rev() :: datastore_doc:rev().
-type ctx() :: datastore:ctx().

-define(KEY_LENGTH,
    application:get_env(cluster_worker, datastore_doc_key_length, 16)).
-define(REV_LENGTH,
    application:get_env(cluster_worker, datastore_doc_rev_length, 16)).

% Test macro - uncomment to generate keys always connected with local node
%%-define(gen_local_keys, 1).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% %% Generates random key.
%% @end
%%--------------------------------------------------------------------
-spec gen_key() -> key().
-ifndef(gen_local_keys).
gen_key() ->
    HashPart = consistent_hashing:gen_hashing_key(),
    gen_key(HashPart).
-endif.
-ifdef(gen_local_keys).
gen_key() ->
    gen_key_node(node()).

%%--------------------------------------------------------------------
%% @doc
%% Function generating key connected with particular node (for tests only).
%% @end
%%--------------------------------------------------------------------
-spec gen_key_node(node()) -> key().
gen_key_node(TargetNode) ->
    Key = consistent_hashing:gen_hashing_key(),
    case consistent_hashing:get_node(Key) of
        TargetNode ->
            gen_key(Key);
        _ ->
            gen_key_node(TargetNode)
    end.
-endif.

%%--------------------------------------------------------------------
%% @doc
%% Generates random key with hash part from arg.
%% @end
%%--------------------------------------------------------------------
-spec gen_key(binary()) -> key().
gen_key(HashPart) ->
    RandPart = str_utils:rand_hex(?KEY_LENGTH),
    consistent_hashing:create_label(HashPart, RandPart).

%%--------------------------------------------------------------------
%% @doc
%% Generates datastore key based on provided seed and other datastore key.
%% @end
%%--------------------------------------------------------------------
-spec gen_key(binary(), key()) -> key().
gen_key(Seed, Key) when is_binary(Seed) ->
    case consistent_hashing:has_hash_part(Key) of
        true ->
            <<Key/binary, Seed/binary>>;
        _ ->
            Ctx = crypto:hash_init(md5),
            Ctx2 = crypto:hash_update(Ctx, Seed),
            Ctx3 = crypto:hash_update(Ctx2, Key),
            hex_utils:hex(crypto:hash_final(Ctx3))
    end.

%%--------------------------------------------------------------------
%% @doc
%% Generates revision with provided generation and random hash.
%% @end
%%--------------------------------------------------------------------
-spec gen_rev(pos_integer()) -> rev().
gen_rev(Generation) ->
    Hash = str_utils:rand_hex(?REV_LENGTH),
    <<(integer_to_binary(Generation))/binary, "-", Hash/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Returns generation and hash of provided revision.
%% @end
%%--------------------------------------------------------------------
-spec parse_rev(rev()) -> {pos_integer(), binary()}.
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
%% Sets expiry field in context.
%% @end
%%--------------------------------------------------------------------
-spec set_expiry(ctx() | couchbase_driver:ctx(), non_neg_integer()) ->
    ctx() | couchbase_driver:ctx().
set_expiry(Ctx, Expiry) when Expiry =< 2592000 ->
    Ctx#{expiry => Expiry};
set_expiry(Ctx, Expiry) ->
    os:timestamp(),
    Ctx#{expiry => erlang:system_time(second) + Expiry}.