%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Eunit tests for the datastore_key module.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_key_tests).
-author("Lukasz Opiola").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(LEGACY_KEY_CHARS, 32).
-define(RAND_LEGACY_KEY, str_utils:rand_hex(?LEGACY_KEY_CHARS div 2)).
-define(EMPTY_LEGACY_KEY, <<>>).
-define(MOCK_CLUSTER_NODES_COUNT, 100).

%%%===================================================================
%%% Setup and teardown
%%%===================================================================

datastore_key_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            {"new key", fun new_key/0},
            {"new key from digest", fun new_key_from_digest/0},
            {"new key adjacent to a random_key", fun new_key_adjacent_to_a_random_key/0},
            {"new key adjacent to a digest key", fun new_key_adjacent_to_a_digest_key/0},
            {"new key adjacent to a legacy key", fun new_key_adjacent_to_a_legacy_key/0},
            {"build key adjacent to a random key", fun build_key_adjacent_to_a_random_key/0},
            {"build key adjacent to a digest key", fun build_key_adjacent_to_a_digest_key/0},
            {"build key adjacent to a legacy key", fun build_key_adjacent_to_a_legacy_key/0},
            {"build key adjacent to an empty legacy key", fun build_key_adjacent_to_an_empty_legacy_key/0},
            {"key from digest adjacent to a random key", fun key_from_digest_adjacent_to_a_random_key/0},
            {"key from digest adjacent to a digest key", fun key_from_digest_adjacent_to_a_digest_key/0},
            {"key from digest adjacent to a legacy key", fun key_from_digest_adjacent_to_a_legacy_key/0},
            {"keys adjacent to keys with the same chash differ", fun keys_adjacent_to_keys_with_the_same_chash_differ/0},
            {"gen legacy key", fun gen_legacy_key/0}
        ]
    }.

setup() ->
    meck:new(consistent_hashing, []),
    meck:expect(consistent_hashing, get_assigned_node, fun(Key) ->
        % Mock consistent hashing by simply returning node with number equal to
        % modulo of the key's decimal representation
        KeyInt = list_to_integer(binary_to_list(Key), 16),
        list_to_atom(str_utils:format("node~B@cluster.example.com", [KeyInt rem ?MOCK_CLUSTER_NODES_COUNT]))
    end).

teardown(_) ->
    ?assert(meck:validate(consistent_hashing)),
    ok = meck:unload(consistent_hashing).

%%%===================================================================
%%% Tests
%%%===================================================================

new_key() ->
    Keys = lists:map(fun(_) ->
        datastore_key:new()
    end, lists:seq(1, 20)),
    assert_keys_are_different(Keys),
    assert_keys_are_same_length(Keys).


new_key_from_digest() ->
    Keys = lists:map(fun datastore_key:new_from_digest/1, [
        a,
        {tuple, value},
        17,
        [a, b, c],
        [<<"binary">>],
        [<<"list">>, <<"of">>, <<"binaries">>],
        [a, b, <<"binary">>, {record, val}]
    ]),
    assert_keys_are_different(Keys),
    assert_keys_are_same_length(Keys),

    DigestComponents = [<<"the">>, same, components, {should_yield, the_same}, "key"],
    ?assertEqual(
        datastore_key:new_from_digest(DigestComponents),
        datastore_key:new_from_digest(DigestComponents)
    ).


new_key_adjacent_to_a_random_key() ->
    lists:foreach(fun new_key_adjacent_to_a_random_key/1, lists:seq(1, 100)).
new_key_adjacent_to_a_random_key(_Repeat) ->
    RandomKey = datastore_key:new(),
    new_adjacent_key_base(all_keys_from_the_same_predecessor, RandomKey, adjacent),
    new_adjacent_key_base(each_key_recursively_from_the_previous, RandomKey, adjacent).


new_key_adjacent_to_a_digest_key() ->
    lists:foreach(fun new_key_adjacent_to_a_digest_key/1, lists:seq(1, 100)).
new_key_adjacent_to_a_digest_key(_Repeat) ->
    DigestKey = datastore_key:new_from_digest([a, b, str_utils:rand_hex(10)]),
    new_adjacent_key_base(all_keys_from_the_same_predecessor, DigestKey, adjacent),
    new_adjacent_key_base(each_key_recursively_from_the_previous, DigestKey, adjacent).


new_key_adjacent_to_a_legacy_key() ->
    lists:foreach(fun new_key_adjacent_to_a_legacy_key/1, lists:seq(1, 100)).
new_key_adjacent_to_a_legacy_key(_Repeat) ->
    LegacyKey = ?RAND_LEGACY_KEY,
    % Adjacency (routing to the same node) is not supported for legacy keys,
    % but key generation should work anyway
    new_adjacent_key_base(all_keys_from_the_same_predecessor, LegacyKey, not_adjacent),
    new_adjacent_key_base(each_key_recursively_from_the_previous, LegacyKey, not_adjacent).


new_adjacent_key_base(KeyCreationPattern, OriginalKey, ExpectedAdjacency) ->
    {NewKeys, _} = lists:mapfoldl(fun(_, PreviousKey) ->
        AdjacentKey = datastore_key:new_adjacent_to(PreviousKey),
        case KeyCreationPattern of
            all_keys_from_the_same_predecessor -> {AdjacentKey, OriginalKey};
            each_key_recursively_from_the_previous -> {AdjacentKey, AdjacentKey}
        end
    end, OriginalKey, lists:seq(1, 10)),
    AllKeys = [OriginalKey | NewKeys],
    assert_keys_are_different(AllKeys),
    assert_keys_are_same_length(AllKeys),
    case ExpectedAdjacency of
        adjacent -> assert_keys_are_adjacent(AllKeys);
        not_adjacent -> ok
    end.


build_key_adjacent_to_a_random_key() ->
    lists:foreach(fun build_key_adjacent_to_a_random_key/1, lists:seq(1, 100)).
build_key_adjacent_to_a_random_key(_Repeat) ->
    RandomKey = datastore_key:new(),
    build_adjacent_key_base(all_keys_from_the_same_predecessor, RandomKey, adjacent),
    build_adjacent_key_base(each_key_recursively_from_the_previous, RandomKey, adjacent).


build_key_adjacent_to_a_digest_key() ->
    lists:foreach(fun build_key_adjacent_to_a_digest_key/1, lists:seq(1, 100)).
build_key_adjacent_to_a_digest_key(_Repeat) ->
    DigestKey = datastore_key:new_from_digest([a, b, str_utils:rand_hex(10)]),
    build_adjacent_key_base(all_keys_from_the_same_predecessor, DigestKey, adjacent),
    build_adjacent_key_base(each_key_recursively_from_the_previous, DigestKey, adjacent).


build_key_adjacent_to_a_legacy_key() ->
    lists:foreach(fun build_key_adjacent_to_a_legacy_key/1, lists:seq(1, 100)).
build_key_adjacent_to_a_legacy_key(_Repeat) ->
    LegacyKey = ?RAND_LEGACY_KEY,
    % Adjacency (routing to the same node) is not supported for legacy keys,
    % but key generation should work anyway
    build_adjacent_key_base(all_keys_from_the_same_predecessor, LegacyKey, not_adjacent),
    build_adjacent_key_base(each_key_recursively_from_the_previous, LegacyKey, not_adjacent).


%% Empty Original key is not recommended, but accepted as it occurs in legacy keys.
build_key_adjacent_to_an_empty_legacy_key() ->
    lists:foreach(fun build_key_adjacent_to_an_empty_legacy_key/1, lists:seq(1, 100)).
build_key_adjacent_to_an_empty_legacy_key(_Repeat) ->
    LegacyKey = ?EMPTY_LEGACY_KEY,
    % Adjacency (routing to the same node) is not supported for legacy keys,
    % but key generation should work anyway
    build_adjacent_key_base(all_keys_from_the_same_predecessor, LegacyKey, not_adjacent),
    build_adjacent_key_base(each_key_recursively_from_the_previous, LegacyKey, not_adjacent).


build_adjacent_key_base(KeyCreationPattern, OriginalKey, ExpectedAdjacency) ->
    ExtensionExamples = [
        <<"custom">>,
        datastore_key:new(),
        datastore_key:new_from_digest(["another", key, {1, 2, 3}]),
        ?RAND_LEGACY_KEY,
        datastore_key:new_adjacent_to(datastore_key:new()),
        datastore_key:new_adjacent_to(datastore_key:new_from_digest(["dig", <<"Est">>])),
        datastore_key:build_adjacent(<<"a">>, datastore_key:new()),
        datastore_key:build_adjacent(<<"custom">>, datastore_key:new_from_digest(["dig", <<"Est">>]))
    ],
    {NewKeys, _} = lists:mapfoldl(fun(Extension, PreviousKey) ->
        AdjacentKey = datastore_key:build_adjacent(Extension, PreviousKey),
        % The same input parameters should always return the same key
        ?assertEqual(AdjacentKey, datastore_key:build_adjacent(Extension, PreviousKey)),
        case KeyCreationPattern of
            all_keys_from_the_same_predecessor -> {AdjacentKey, OriginalKey};
            each_key_recursively_from_the_previous -> {AdjacentKey, AdjacentKey}
        end
    end, OriginalKey, ExtensionExamples),
    AllKeys = [OriginalKey | NewKeys],
    assert_keys_are_different(AllKeys),
    case ExpectedAdjacency of
        adjacent -> assert_keys_are_adjacent(AllKeys);
        not_adjacent -> ok
    end.


key_from_digest_adjacent_to_a_random_key() ->
    lists:foreach(fun key_from_digest_adjacent_to_a_random_key/1, lists:seq(1, 100)).
key_from_digest_adjacent_to_a_random_key(_Repeat) ->
    RandomKey = datastore_key:new(),
    adjacent_key_from_digest_base(all_keys_from_the_same_predecessor, RandomKey, adjacent),
    adjacent_key_from_digest_base(each_key_recursively_from_the_previous, RandomKey, adjacent).


key_from_digest_adjacent_to_a_digest_key() ->
    lists:foreach(fun key_from_digest_adjacent_to_a_digest_key/1, lists:seq(1, 100)).
key_from_digest_adjacent_to_a_digest_key(_Repeat) ->
    DigestKey = datastore_key:new_from_digest({def, [a, b, str_utils:rand_hex(10)]}),
    adjacent_key_from_digest_base(all_keys_from_the_same_predecessor, DigestKey, adjacent),
    adjacent_key_from_digest_base(each_key_recursively_from_the_previous, DigestKey, adjacent).


key_from_digest_adjacent_to_a_legacy_key() ->
    lists:foreach(fun key_from_digest_adjacent_to_a_legacy_key/1, lists:seq(1, 100)).
key_from_digest_adjacent_to_a_legacy_key(_Repeat) ->
    LegacyKey = ?RAND_LEGACY_KEY,
    % Adjacency (routing to the same node) is not supported for legacy keys,
    % but key generation should work anyway
    adjacent_key_from_digest_base(all_keys_from_the_same_predecessor, LegacyKey, not_adjacent),
    adjacent_key_from_digest_base(each_key_recursively_from_the_previous, LegacyKey, not_adjacent).


adjacent_key_from_digest_base(KeyCreationPattern, OriginalKey, ExpectedAdjacency) ->
    DigestComponentsExamples = [
        a,
        {tuple, value},
        17,
        [a, b, c],
        [<<"binary">>],
        [<<"list">>, <<"of">>, <<"binaries">>],
        [a, b, <<"binary">>, {record, val}]
    ],
    {NewKeys, _} = lists:mapfoldl(fun(DigestComponents, PreviousKey) ->
        AdjacentKey = datastore_key:adjacent_from_digest(DigestComponents, PreviousKey),
        % The same input parameters should always return the same key
        ?assertEqual(AdjacentKey, datastore_key:adjacent_from_digest(DigestComponents, PreviousKey)),
        case KeyCreationPattern of
            all_keys_from_the_same_predecessor -> {AdjacentKey, OriginalKey};
            each_key_recursively_from_the_previous -> {AdjacentKey, AdjacentKey}
        end
    end, OriginalKey, DigestComponentsExamples),
    AllKeys = [OriginalKey | NewKeys],
    assert_keys_are_different(AllKeys),
    assert_keys_are_same_length(AllKeys),
    case ExpectedAdjacency of
        adjacent -> assert_keys_are_adjacent(AllKeys);
        not_adjacent -> ok
    end.


keys_adjacent_to_keys_with_the_same_chash_differ() ->
    KeyA = datastore_key:new(),
    KeyB = datastore_key:new_adjacent_to(KeyA),
    KeyC = datastore_key:new_adjacent_to(KeyA),
    assert_keys_are_different([KeyA, KeyB, KeyC]),
    assert_keys_are_adjacent([KeyA, KeyB, KeyC]),
    % Keys A, B, C have the same chash label, but are different. Make sure that
    % adjacent keys derived from them are different.
    NewKeys = [
        datastore_key:build_adjacent(<<"ext">>, KeyA),
        datastore_key:build_adjacent(<<"ext">>, KeyB),
        datastore_key:build_adjacent(<<"ext">>, KeyC),

        datastore_key:adjacent_from_digest(["another", key, {1, 2, 3}], KeyA),
        datastore_key:adjacent_from_digest(["another", key, {1, 2, 3}], KeyB),
        datastore_key:adjacent_from_digest(["another", key, {1, 2, 3}], KeyC)
    ],
    assert_keys_are_different(NewKeys),
    assert_keys_are_adjacent(NewKeys).


% datastore_key:build_adjacent/2 should fall back to gen_legacy_key if the
% Original key (the second argument) is a legacy key (does not include a chash label).
gen_legacy_key() ->
    InputExamples = [
        {<<"">>, <<"123">>},
        {<<"">>, ?RAND_LEGACY_KEY},
        {<<"user">>, ?RAND_LEGACY_KEY},
        {?RAND_LEGACY_KEY, ?RAND_LEGACY_KEY},
        {datastore_key:new(), ?RAND_LEGACY_KEY},
        {datastore_key:build_adjacent(<<"a">>, datastore_key:new()), ?RAND_LEGACY_KEY}
    ],
    Keys = lists:map(fun({Seed, Key}) ->
        LegacyKey = datastore_key:build_adjacent(Seed, Key),
        % The same input parameters should always return the same key
        ?assertEqual(LegacyKey, datastore_key:build_adjacent(Seed, Key)),
        LegacyKey
    end, InputExamples),
    assert_keys_are_different(Keys),
    assert_keys_are_same_length(Keys),
    ?assertEqual(?LEGACY_KEY_CHARS, byte_size(hd(Keys))),

    % Make sure legacy key the mappings are retained
    lists:foreach(fun({Seed, Key, ExpectedResultKey}) ->
        ?assertEqual(ExpectedResultKey, datastore_key:build_adjacent(Seed, Key))
    end, legacy_key_specific_examples()).


% Examples of keys created using the legacy procedure from previous versions of the system
legacy_key_specific_examples() -> [
    {<<"">>, <<"myIdP:123456789@idp.com">>, <<"2073dfa18a7e64bee5c492b71e9ee5c1">>},
    {<<"seed">>, <<"myIdP:123456789@idp.com">>, <<"2298e9f441be7fc5a236271311a52c2d">>},

    {<<"">>, <<"sso_org:abcdefghij">>, <<"c85747a0cb9a377891a0505f218d910d">>},
    {<<"seed">>, <<"sso_org:abcdefghij">>, <<"2415c8ce5ae9d80012044aa0f07bbc98">>},

    {<<"">>, <<"vo:my-organization/tm:support-unit">>, <<"115594c131271df8df8510e5ac7d8861">>},
    {<<"seed">>, <<"vo:my-organization/tm:support-unit">>, <<"8699d7c1b769da53dba0dc280b255db6">>},

    {<<"">>, <<"ut:company/tm:users/rl:admins">>, <<"55765121f92ced7a57eefb23caad9e22">>},
    {<<"seed">>, <<"ut:company/tm:users/rl:admins">>, <<"a6ca3d2fd3565adb19a236198fe83d97">>}
].


%%%===================================================================
%%% Helper function
%%%===================================================================

assert_keys_are_different(Keys) ->
    foreach_pair(fun(KeyAlpha, KeyBeta) ->
        ?assertNotEqual(KeyAlpha, KeyBeta)
    end, Keys).


assert_keys_are_same_length(Keys) ->
    foreach_pair(fun(KeyAlpha, KeyBeta) ->
        ?assertEqual(byte_size(KeyAlpha), byte_size(KeyBeta))
    end, Keys).


% adjacent <-> are routed to the same node
assert_keys_are_adjacent(Keys) ->
    foreach_pair(fun(KeyAlpha, KeyBeta) ->
        ?assertEqual(datastore_key:responsible_node(KeyAlpha), datastore_key:responsible_node(KeyBeta))
    end, Keys).


foreach_pair(Fun, Elements) ->
    lists:foldl(fun(Current, Unpaired) ->
        UnpairedWithoutCurrent = lists:delete(Current, Unpaired),
        lists:foreach(fun(Other) ->
            Fun(Current, Other)
        end, UnpairedWithoutCurrent),
        UnpairedWithoutCurrent
    end, Elements, Elements).


-endif.