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
-module(datastore_utils2).
-author("Krzysztof Trzepla").

-define(KEY_LEN, 16).

%% API
-export([gen_key/0, gen_key/2, hex/1]).

-type hex() :: binary().

-export_type([hex/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Generates random datastore key.
%% @end
%%--------------------------------------------------------------------
-spec gen_key() -> datastore:key().
gen_key() ->
    hex(crypto:strong_rand_bytes(?KEY_LEN)).

%%--------------------------------------------------------------------
%% @doc
%% Generates datastore key based on provided seed and other datastore key.
%% @end
%%--------------------------------------------------------------------
-spec gen_key(binary(), datastore:key()) -> datastore:key().
gen_key(Seed, Key) when is_binary(Seed) ->
    Ctx = crypto:hash_init(md5),
    Ctx2 = crypto:hash_update(Ctx, Seed),
    Ctx3 = crypto:hash_update(Ctx2, Key),
    hex(crypto:hash_final(Ctx3)).

%%--------------------------------------------------------------------
%% @doc
%% Converts binary digest to a binary hex string.
%% @end
%%--------------------------------------------------------------------
-spec hex(binary()) -> hex().
hex(Digest) ->
    Hex = {$0, $1, $2, $3, $4, $5, $6, $7, $8, $9, $a, $b, $c, $d, $e, $f},
    <<
        <<(element(B bsr 4 + 1, Hex)), (element(B band 16#0F + 1, Hex))>> ||
        <<B:8>> <= Digest
    >>.