%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles datastore key creation. All keys should be
%%% created using this module to ensure consistent and safe format.
%%%
%%% Datastore keys are alphanumeric strings (without special characters) with
%%% the following characteristics:
%%%
%%%   * Versions before 19.02.1 - 16 random bytes, hex encoded (32 characters),
%%%     identical to an MD5 hash.
%%%
%%%   * Since 19.02.1 - the same as for previous versions, but with an additional
%%%     suffix - chash label - that serves as a label for consistent hashing:
%%%         <<BasicKey,Separator,CHashLabel>>
%%%
%%% The chash label is used to route keys - pick a cluster node responsible
%%% for given key. Presence of the chash label is recommended, but not mandatory
%%% - in such case legacy key routing applies.
%%% The chash label can be random (if no specific key routing is needed), or
%%% inherited from another key by creating an *adjacent key*.
%%%
%%% ADJACENT KEYS are always handled on the same cluster node thanks to the fact
%%% that they share the same chash label. It is not possible to create an
%%% adjacent key to a legacy key.
%%%
%%% For backward compatibility and in order to retain the system data from older
%%% versions, some of the keys are still constructed using the legacy procedure.
%%% This is especially true for the digest-based ids that were generated by
%%% hashing arbitrary binaries or erlang terms to obtain a consistent, one-way
%%% mapping - retained as the gen_legacy_key/2 function, which is used as a
%%% fallback when a legacy key is provided to key construction functions. This
%%% means that there will always be two types of keys coexisting in the system.
%%% The API offers a new function new_from_digest/1 that includes a chash label,
%%% which should be used if legacy generation is not necessary.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_key).
-author("Lukasz Opiola").

-include_lib("ctool/include/hashing/consistent_hashing.hrl").

-define(KEY_BYTES, application:get_env(cluster_worker, datastore_doc_key_length, 16)).
-define(CHASH_LABEL_SEPARATOR, "ch").
-define(CHASH_LABEL_SEPARATOR_SIZE, 2).
-define(CHASH_LABEL_BYTES, 2).
-define(CHASH_LABEL_CHARS, 4). % Each byte is encoded by two hex characters

% Alphanumeric string without special characters or whitespaces
-type key() :: binary().
% Label used for key routing - the same label is guaranteed to be always handled
% on the same cluster node
-type chash_label() :: <<_:32>>. % ?CHASH_LABEL_CHARS * 8 bits
% Terms that will be digested to create a key - the same terms guarantee to
% yield the same key. A list of binaries is preferred, other terms are first
% transformed to binaries.
-type digest_components() :: term() | [binary() | term()].

-export_type([key/0]).

%% API
-export([new/0, new_from_digest/1]).
-export([new_adjacent_to/1, build_adjacent/2, adjacent_from_digest/2]).
-export([any_responsible_node/1, primary_responsible_node/1, get_chash_seed/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns a new, random datastore key with a random chash label.
%% @end
%%--------------------------------------------------------------------
-spec new() -> key().
new() ->
    BasicKey = str_utils:rand_hex(?KEY_BYTES),
    CHashLabel = str_utils:rand_hex(?CHASH_LABEL_BYTES),
    concatenate_chash_label(BasicKey, CHashLabel).


%%--------------------------------------------------------------------
%% @doc
%% Returns the datastore key obtained from digesting arbitrary terms.
%% A list of binaries is preferred, other terms are first transformed to binary.
%% @end
%%--------------------------------------------------------------------
-spec new_from_digest(digest_components()) -> key().
new_from_digest(DigestComponents) ->
    BasicKey = digest(DigestComponents),
    % Take the chash label from the middle of the basic key
    <<_:10/binary, CHashLabel:?CHASH_LABEL_CHARS/binary, _/binary>> = BasicKey,
    concatenate_chash_label(BasicKey, CHashLabel).


%%--------------------------------------------------------------------
%% @doc
%% Creates a new, random key that is adjacent to the Original key.
%% NOTE: if a legacy Original key is given, adjacency is not supported.
%% @end
%%--------------------------------------------------------------------
-spec new_adjacent_to(Original :: key()) -> key().
new_adjacent_to(Original) when size(Original) > 0 ->
    BasicKey = str_utils:rand_hex(?KEY_BYTES),
    case to_basic_key_and_chash_label(Original) of
        {_, undefined} -> gen_legacy_key(BasicKey, Original);
        {_, CHashLabel} -> concatenate_chash_label(BasicKey, CHashLabel)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Builds a key that is adjacent to the Original key using given Extension.
%% Extension must be a valid key (typically a constant or another key).
%% NOTE: if a legacy Original key is given, adjacency is not supported.
%% NOTE: Empty Original key is not recommended, but accepted as it occurs in legacy keys.
%% In such case, the legacy key generation procedure applies.
%% @end
%%--------------------------------------------------------------------
-spec build_adjacent(Extension :: key(), Original :: key()) -> key().
build_adjacent(Extension, Original) when is_binary(Extension) andalso is_binary(Original) ->
    case to_basic_key_and_chash_label(Original) of
        {_, undefined} ->
            gen_legacy_key(Extension, Original);
        {BasicKey, CHashLabel} when size(Extension) > 0 ->
            % As the extension is usually a constant or has limited pool of
            % values, it is placed in the middle of resulting key to avoid keys
            % with identical prefixes
            concatenate_chash_label(<<BasicKey/binary, Extension/binary>>, CHashLabel)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Creates a key from digest that is adjacent to the Original key.
%% A list of binaries is preferred, other terms are first transformed to binary.
%% NOTE: if a legacy Original key is given, adjacency is not supported.
%% @end
%%--------------------------------------------------------------------
-spec adjacent_from_digest(digest_components(), Original :: key()) -> key().
adjacent_from_digest(DigestComponents, Original) when not is_list(DigestComponents) ->
    adjacent_from_digest([DigestComponents], Original);
adjacent_from_digest(DigestComponents, Original) when size(Original) > 0 ->
    % Original key is included in the digest, otherwise two different Original
    % keys with the same chash label would yield the same key for same DigestComponents
    BasicKey = digest([Original | DigestComponents]),
    case to_basic_key_and_chash_label(Original) of
        {_, undefined} -> gen_legacy_key(BasicKey, Original);
        {_, CHashLabel} -> concatenate_chash_label(BasicKey, CHashLabel)
    end.


%%--------------------------------------------------------------------
%% @doc
%% Returns a single node responsible for handling given datastore key.
%% If responsible node is down, returns first possible node.
%% @end
%%--------------------------------------------------------------------
-spec any_responsible_node(key()) -> node().
any_responsible_node(Key) ->
    CHashSeed = get_chash_seed(Key),
    #node_routing_info{assigned_nodes = Nodes, failed_nodes = FailedNodes} =
        consistent_hashing:get_routing_info(CHashSeed),
    case Nodes -- FailedNodes of
        [Node | _] -> Node;
        [] -> throw(all_responsible_nodes_failed)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns a single node responsible for handling given datastore key.
%% @end
%%--------------------------------------------------------------------
-spec primary_responsible_node(key()) -> node().
primary_responsible_node(Key) ->
    CHashSeed = get_chash_seed(Key),
    #node_routing_info{assigned_nodes = [Node | _]} = consistent_hashing:get_routing_info(CHashSeed),
    Node.


-spec get_chash_seed(key()) -> key() | chash_label().
get_chash_seed(Key) ->
    case to_basic_key_and_chash_label(Key) of
        {BasicKey, undefined} ->
            % Legacy key - use the whole key for routing
            BasicKey;
        {_, CHashLabel} ->
            % Key with a chash label - use the label for routing
            CHashLabel
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Generates a datastore key based on provided seed and other datastore key
%% according to the procedure that was used in versions pre 19.02.1.
%% NOTE: Should only be used in code that requires retaining the legacy ids.
%% In other cases, new_from_digest/1 should be used.
%% @end
%%--------------------------------------------------------------------
-spec gen_legacy_key(binary(), key()) -> key().
gen_legacy_key(Seed, Key) ->
    Ctx = crypto:hash_init(md5),
    Ctx2 = crypto:hash_update(Ctx, Seed),
    Ctx3 = crypto:hash_update(Ctx2, Key),
    hex_utils:hex(crypto:hash_final(Ctx3)).


%% @private
-spec digest(digest_components()) -> binary().
digest(Term) when not is_list(Term) ->
    digest([Term]);
digest(DigestComponents) when length(DigestComponents) > 0 ->
    FinalCtx = lists:foldl(fun
        (Bin, Ctx) when is_binary(Bin) -> crypto:hash_update(Ctx, Bin);
        (Term, Ctx) -> crypto:hash_update(Ctx, term_to_binary(Term))
    end, crypto:hash_init(md5), DigestComponents),
    hex_utils:hex(crypto:hash_final(FinalCtx)).


%% @private
-spec concatenate_chash_label(key(), chash_label()) -> key().
concatenate_chash_label(BasicKey, CHashLabel) ->
    <<BasicKey/binary, ?CHASH_LABEL_SEPARATOR, CHashLabel/binary>>.


%% @private
-spec to_basic_key_and_chash_label(key()) -> {key(), undefined | chash_label()}.
to_basic_key_and_chash_label(Key) ->
    BasicKeyLength = byte_size(Key) - ?CHASH_LABEL_SEPARATOR_SIZE - ?CHASH_LABEL_CHARS,
    case Key of
        <<BasicKey:BasicKeyLength/binary, ?CHASH_LABEL_SEPARATOR, CHashLabel:4/binary>> ->
            {BasicKey, CHashLabel};
        _ ->
            {Key, undefined}
    end.