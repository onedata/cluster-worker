%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Eunit tests for the infinite_log module.
%%% @end
%%%-------------------------------------------------------------------
-module(infinite_log_tests).
-author("Lukasz Opiola").

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/logging.hrl").
-include("modules/datastore/infinite_log.hrl").

-define(range(From, To), lists:seq(From, To, signum(To - From))).
-define(rand(Limit), rand:uniform(Limit)).
-define(testList(ExpectedIndices, Direction, StartFrom),
    ?testList(ExpectedIndices, Direction, StartFrom, #{})
).
-define(testList(ExpectedIndices, Direction, StartFrom, OtherOpts),
    ?assert(list_indices_and_verify(ExpectedIndices, Direction, StartFrom, OtherOpts))
).

%%%===================================================================
%%% Test setup
%%%===================================================================

% appending to log is tested during listing tests
-define(TEST_CASES, [
    {"create_and_destroy", fun create_and_destroy/2},
    {"set_ttl", fun set_ttl/2},

    {"list", fun list/2},
    {"list_from_id", fun list_from_id/2},
    {"list_from_timestamp", fun list_from_timestamp/2},
    {"list_log_with_the_same_timestamps", fun list_log_with_the_same_timestamps/2},
    {"list_log_with_clustered_timestamps", fun list_log_with_clustered_timestamps/2},
    {"list_log_with_irregular_timestamps", fun list_log_with_irregular_timestamps/2},
    {"list_log_with_one_element", fun list_log_with_one_element/2},
    {"list_log_with_one_full_node", fun list_log_with_one_full_node/2},

    {"size_based_pruning", fun size_based_pruning/2},
    {"size_based_pruning_with_low_threshold", fun size_based_pruning_with_low_threshold/2},
    {"age_based_pruning", fun age_based_pruning/2},
    {"age_based_pruning_with_ttl_set", fun age_based_pruning_with_ttl_set/2},

    {"list_inexistent_log", fun list_inexistent_log/2},
    {"list_empty_log", fun list_empty_log/2},
    {"list_log_with_missing_nodes", fun list_log_with_missing_nodes/2},

    {"append_with_time_warps", fun append_with_time_warps/2},
    {"append_too_large_content", fun append_too_large_content/2}
]).

-define(ELEMENTS_PER_NODE_VARIANTS, [
    1, 2, 3, 5, 11, 99, 301, 1000, 1099
]).

-define(DATASTORE_CTX, #{disc_driver_ctx => #{}}).
-define(DATASTORE_BATCH, datastore_batch).


inf_log_test_() ->
    {foreach,
        fun() ->
            clock_freezer_mock:setup_locally([
                infinite_log_sentinel, node_cache, couchbase_driver, ?MODULE
            ]),
            node_cache:init(),
            mock_datastore_doc()
        end,
        fun(_) ->
            clock_freezer_mock:teardown_locally(),
            node_cache:destroy(),
            meck:unload(datastore_doc)
        end,
        lists:flatmap(fun({Name, Fun}) ->
            lists:map(fun(MaxEntriesPerNode) ->
                {timeout, 120, {str_utils:format("~s [~B]", [Name, MaxEntriesPerNode]), fun() ->
                    LogId = create_log_for_test(#{
                        max_entries_per_node => MaxEntriesPerNode
                    }),
                    Fun(LogId, MaxEntriesPerNode)
                end}}
            end, ?ELEMENTS_PER_NODE_VARIANTS)
        end, ?TEST_CASES)
    }.

%%%===================================================================
%%% Tests
%%%===================================================================

create_and_destroy(LogId, MaxEntriesPerNode) ->
    % the log is created in test setup
    EntryCount = 1000,

    ?assert(sentinel_exists(LogId)),
    foreach_archival_node_number(EntryCount, MaxEntriesPerNode, fun(NodeNumber) ->
        ?assertNot(node_exists(LogId, NodeNumber))
    end),

    append(#{count => EntryCount, first_at => 0, interval => 1}),
    ?assert(sentinel_exists(LogId)),
    foreach_archival_node_number(EntryCount, MaxEntriesPerNode, fun(NodeNumber) ->
        ?assert(node_exists(LogId, NodeNumber))
    end),

    ?assertEqual(ok, call_destroy(LogId)),
    ?assertNot(sentinel_exists(LogId)),
    foreach_archival_node_number(EntryCount, MaxEntriesPerNode, fun(NodeNumber) ->
        ?assertNot(node_exists(LogId, NodeNumber))
    end),

    ?assertEqual({error, not_found}, call_list(LogId, #{direction => ?FORWARD}, allow_updates)),
    ?assertEqual({error, not_found}, call_list(LogId, #{direction => ?BACKWARD}, allow_updates)),
    ?assertEqual({error, not_found}, call_append(LogId, <<"log">>)),
    ?assertEqual(ok, call_destroy(LogId)).


set_ttl(LogId, MaxEntriesPerNode) ->
    % the log is created in test setup
    EntryCount = 5000,
    append(#{count => EntryCount, first_at => 0, interval => 1}),

    Ttl = rand:uniform(100000000),
    ?assertEqual(ok, call_set_ttl(LogId, Ttl)),

    clock_freezer_mock:simulate_seconds_passing(Ttl - 1),
    ?assert(sentinel_exists(LogId)),
    foreach_archival_node_number(EntryCount, MaxEntriesPerNode, fun(NodeNumber) ->
        ?assert(node_exists(LogId, NodeNumber))
    end),

    % newly appended logs should also be subject to the previously set TTL
    NewLogsCount = 783,
    append(#{count => NewLogsCount, interval => 0}),

    % when the TTL passes, all log data should be deleted
    clock_freezer_mock:simulate_seconds_passing(1),
    ?assertNot(sentinel_exists(LogId)),
    foreach_archival_node_number(EntryCount + NewLogsCount, MaxEntriesPerNode, fun(NodeNumber) ->
        ?assertNot(node_exists(LogId, NodeNumber))
    end),

    ?assertEqual({error, not_found}, call_list(LogId, #{direction => ?FORWARD}, allow_updates)),
    ?assertEqual({error, not_found}, call_list(LogId, #{direction => ?BACKWARD}, allow_updates)),
    ?assertEqual({error, not_found}, call_append(LogId, <<"log">>)),
    ?assertEqual({error, not_found}, call_set_ttl(LogId, Ttl)).


list(_, _) ->
    append(#{count => 5000}),

    ?testList([4999], ?BACKWARD, undefined, #{offset => 0, limit => 1}),
    ?testList([0], ?FORWARD, undefined, #{offset => 0, limit => 1}),
    ?testList([4999], ?BACKWARD, undefined, #{offset => -1, limit => 1}),
    ?testList([0], ?FORWARD, undefined, #{offset => -1, limit => 1}),

    ?testList(?range(4999, 4980), ?BACKWARD, undefined, #{offset => 0, limit => 20}),
    ?testList(?range(0, 19), ?FORWARD, undefined, #{offset => 0, limit => 20}),
    ?testList(?range(4999, 4980), ?BACKWARD, undefined, #{offset => -19, limit => 20}),
    ?testList(?range(0, 19), ?FORWARD, undefined, #{offset => -19, limit => 20}),

    % listing limit is capped at 1000
    ?testList(?range(4999, 4000), ?BACKWARD, undefined, #{offset => 0, limit => 5000}),
    ?testList(?range(0, 999), ?FORWARD, undefined, #{offset => 0, limit => 5000}),
    ?testList(?range(2999, 2000), ?BACKWARD, undefined, #{offset => 2000, limit => 5000}),
    ?testList(?range(2000, 2999), ?FORWARD, undefined, #{offset => 2000, limit => 5000}),

    ?testList([0], ?BACKWARD, undefined, #{offset => 4999, limit => 1}),
    ?testList([4999], ?FORWARD, undefined, #{offset => 4999, limit => 1}),
    ?testList([0], ?BACKWARD, undefined, #{offset => 4999, limit => 56}),
    ?testList([4999], ?FORWARD, undefined, #{offset => 4999, limit => 56}),

    ?testList(?range(4979, 4965), ?BACKWARD, undefined, #{offset => 20, limit => 15}),
    ?testList(?range(20, 34), ?FORWARD, undefined, #{offset => 20, limit => 15}),
    % listing limit is capped at 1000
    ?testList(?range(4979, 3980), ?BACKWARD, undefined, #{offset => 20, limit => 3000}),
    ?testList(?range(20, 1019), ?FORWARD, undefined, #{offset => 20, limit => 3000}),
    ?testList(?range(4979, 3980), ?BACKWARD, undefined, #{offset => 20, limit => 80000000}),
    ?testList(?range(20, 1019), ?FORWARD, undefined, #{offset => 20, limit => 80000000}).


list_from_id(_, _) ->
    append(#{count => 5000}),

    ?testList([0], ?BACKWARD, {index, 0}, #{offset => 0, limit => 1}),
    ?testList([0], ?FORWARD, {index, 0}, #{offset => 0, limit => 1}),
    ?testList([0], ?BACKWARD, {index, 0}, #{offset => 0, limit => 8}),
    ?testList(?range(0, 7), ?FORWARD, {index, 0}, #{offset => 0, limit => 8}),

    ?testList([1], ?BACKWARD, {index, 0}, #{offset => -1, limit => 1}),
    ?testList([0], ?FORWARD, {index, 0}, #{offset => -1, limit => 1}),
    ?testList(?range(1, 0), ?BACKWARD, {index, 0}, #{offset => -1, limit => 27}),
    ?testList(?range(0, 26), ?FORWARD, {index, 0}, #{offset => -1, limit => 27}),

    ?testList([], ?BACKWARD, {index, 0}, #{offset => 1, limit => 1}),
    ?testList([1], ?FORWARD, {index, 0}, #{offset => 1, limit => 1}),
    ?testList([], ?BACKWARD, {index, 0}, #{offset => 1, limit => 240}),
    ?testList(?range(1, 240), ?FORWARD, {index, 0}, #{offset => 1, limit => 240}),

    ?testList([4999], ?BACKWARD, {index, 4999}, #{offset => 0, limit => 1}),
    ?testList([4999], ?FORWARD, {index, 4999}, #{offset => 0, limit => 1}),
    ?testList(?range(4999, 4987), ?BACKWARD, {index, 4999}, #{offset => 0, limit => 13}),
    ?testList([4999], ?FORWARD, {index, 4999}, #{offset => 0, limit => 13}),

    ?testList([4999], ?BACKWARD, {index, 4999}, #{offset => -1, limit => 1}),
    ?testList([4998], ?FORWARD, {index, 4999}, #{offset => -1, limit => 1}),
    ?testList(?range(4999, 4995), ?BACKWARD, {index, 4999}, #{offset => -1, limit => 5}),
    ?testList(?range(4998, 4999), ?FORWARD, {index, 4999}, #{offset => -1, limit => 5}),

    ?testList([4998], ?BACKWARD, {index, 4999}, #{offset => 1, limit => 1}),
    ?testList([], ?FORWARD, {index, 4999}, #{offset => 1, limit => 1}),
    % listing limit is capped at 1000
    ?testList(?range(4998, 3999), ?BACKWARD, {index, 4999}, #{offset => 1, limit => 4800}),
    ?testList([], ?FORWARD, {index, 4999}, #{offset => 1, limit => 4800}),

    ?testList(?range(2400, 2101), ?BACKWARD, {index, 2400}, #{offset => 0, limit => 300}),
    ?testList(?range(2400, 2699), ?FORWARD, {index, 2400}, #{offset => 0, limit => 300}),
    ?testList(?range(100, 0), ?BACKWARD, {index, 2400}, #{offset => 2300, limit => 300}),
    ?testList(?range(4700, 4999), ?FORWARD, {index, 2400}, #{offset => 2300, limit => 300}),
    ?testList(?range(3000, 2701), ?BACKWARD, {index, 2400}, #{offset => -600, limit => 300}),
    ?testList(?range(1800, 2099), ?FORWARD, {index, 2400}, #{offset => -600, limit => 300}),
    ?testList(?range(4999, 4700), ?BACKWARD, {index, 2400}, #{offset => -2600, limit => 300}),
    ?testList(?range(0, 299), ?FORWARD, {index, 2400}, #{offset => -2600, limit => 300}),

    ?testList([], ?BACKWARD, {index, -7}, #{offset => 0, limit => 1}),
    ?testList([0], ?FORWARD, {index, -7}, #{offset => 0, limit => 1}),

    ?testList(?range(3, 0), ?BACKWARD, {index, -7}, #{offset => -4, limit => 49}),
    ?testList(?range(0, 48), ?FORWARD, {index, -7}, #{offset => -4, limit => 49}),

    ?testList([], ?BACKWARD, {index, -7}, #{offset => 9, limit => 506}),
    ?testList(?range(9, 514), ?FORWARD, {index, -7}, #{offset => 9, limit => 506}),

    ?testList([4999], ?BACKWARD, {index, 9823655123764823}, #{offset => 0, limit => 1}),
    ?testList([], ?FORWARD, {index, 9823655123764823}, #{offset => 0, limit => 1}),

    ?testList(?range(4999, 4930), ?BACKWARD, {index, 9823655123764823}, #{offset => -60, limit => 70}),
    ?testList(?range(4940, 4999), ?FORWARD, {index, 9823655123764823}, #{offset => -60, limit => 70}),

    % listing limit is capped at 1000
    ?testList(?range(4985, 3986), ?BACKWARD, {index, 9823655123764823}, #{offset => 14, limit => 7000}),
    ?testList([], ?FORWARD, {index, 9823655123764823}, #{offset => 14, limit => 7000}).


list_from_timestamp(_, _) ->
    append(#{count => 1000, first_at => 0, interval => 1}),

    ?testList([], ?BACKWARD, {timestamp, -1}, #{offset => 0, limit => 1}),
    ?testList([], ?FORWARD, {timestamp, 1000}, #{offset => 0, limit => 1}),
    ?testList([], ?BACKWARD, {timestamp, -1}, #{offset => 10, limit => 1}),
    ?testList([], ?FORWARD, {timestamp, 1000}, #{offset => 10, limit => 1}),
    ?testList([9], ?BACKWARD, {timestamp, -1}, #{offset => -10, limit => 1}),
    ?testList([990], ?FORWARD, {timestamp, 1000}, #{offset => -10, limit => 1}),

    ?testList([999], ?BACKWARD, {timestamp, 1000}, #{offset => 0, limit => 1}),
    ?testList([0], ?FORWARD, {timestamp, -1}, #{offset => 0, limit => 1}),
    ?testList([319], ?BACKWARD, {timestamp, 1000}, #{offset => 680, limit => 1}),
    ?testList([680], ?FORWARD, {timestamp, -1}, #{offset => 680, limit => 1}),

    ?testList([999], ?BACKWARD, {timestamp, 999}, #{offset => 0, limit => 1}),
    ?testList([0], ?FORWARD, {timestamp, 0}, #{offset => 0, limit => 1}),
    ?testList([0], ?BACKWARD, {timestamp, 999}, #{offset => 999, limit => 1}),
    ?testList([999], ?FORWARD, {timestamp, 0}, #{offset => 999, limit => 1}),

    ?testList([0], ?BACKWARD, {timestamp, 0}, #{offset => 0, limit => 1}),
    ?testList([999], ?FORWARD, {timestamp, 999}, #{offset => 0, limit => 1}),
    ?testList([], ?BACKWARD, {timestamp, 0}, #{offset => 1, limit => 1}),
    ?testList([], ?FORWARD, {timestamp, 999}, #{offset => 1, limit => 1}),

    ?testList([500, 499], ?BACKWARD, {timestamp, 500}, #{offset => 0, limit => 2}),
    ?testList([500, 501], ?FORWARD, {timestamp, 500}, #{offset => 0, limit => 2}),
    ?testList([543, 542], ?BACKWARD, {timestamp, 500}, #{offset => -43, limit => 2}),
    ?testList([457, 458], ?FORWARD, {timestamp, 500}, #{offset => -43, limit => 2}),

    ?testList(?range(500, 401), ?BACKWARD, {timestamp, 500}, #{offset => 0, limit => 100}),
    ?testList(?range(500, 599), ?FORWARD, {timestamp, 500}, #{offset => 0, limit => 100}),
    ?testList(?range(34, 0), ?BACKWARD, {timestamp, 500}, #{offset => 466, limit => 100}),
    ?testList(?range(966, 999), ?FORWARD, {timestamp, 500}, #{offset => 466, limit => 100}).


list_log_with_the_same_timestamps(_, _) ->
    Timestamp = ?rand(9999999999),
    append(#{count => 100, first_at => Timestamp, interval => 0}),

    ?testList([99], ?BACKWARD, {timestamp, Timestamp}, #{offset => 0, limit => 1}),
    ?testList([0], ?FORWARD, {timestamp, Timestamp}, #{offset => 0, limit => 1}),
    ?testList([82], ?BACKWARD, {timestamp, Timestamp}, #{offset => 17, limit => 1}),
    ?testList([17], ?FORWARD, {timestamp, Timestamp}, #{offset => 17, limit => 1}),
    ?testList([99], ?BACKWARD, {timestamp, Timestamp}, #{offset => -9, limit => 1}),
    ?testList([0], ?FORWARD, {timestamp, Timestamp}, #{offset => -9, limit => 1}),

    ?testList(?range(99, 65), ?BACKWARD, {timestamp, Timestamp}, #{offset => 0, limit => 35}),
    ?testList(?range(0, 34), ?FORWARD, {timestamp, Timestamp}, #{offset => 0, limit => 35}),
    ?testList(?range(88, 54), ?BACKWARD, {timestamp, Timestamp}, #{offset => 11, limit => 35}),
    ?testList(?range(11, 45), ?FORWARD, {timestamp, Timestamp}, #{offset => 11, limit => 35}),
    ?testList(?range(99, 65), ?BACKWARD, {timestamp, Timestamp}, #{offset => -30, limit => 35}),
    ?testList(?range(0, 34), ?FORWARD, {timestamp, Timestamp}, #{offset => -30, limit => 35}).


list_log_with_clustered_timestamps(_, _) ->
    append(#{count => 100, first_at => 0, interval => 0}),
    append(#{count => 100, first_at => 12345, interval => 0}),
    append(#{count => 100, first_at => 24690, interval => 0}),

    ?testList(?range(99, 95), ?BACKWARD, {timestamp, 0}, #{offset => 0, limit => 5}),
    ?testList(?range(0, 4), ?FORWARD, {timestamp, 0}, #{offset => 0, limit => 5}),
    ?testList(?range(179, 175), ?BACKWARD, {timestamp, 0}, #{offset => -80, limit => 5}),
    ?testList(?range(0, 4), ?FORWARD, {timestamp, 0}, #{offset => -80, limit => 5}),

    ?testList([199], ?BACKWARD, {timestamp, 12345}, #{offset => 0, limit => 1}),
    ?testList([100], ?FORWARD, {timestamp, 12345}, #{offset => 0, limit => 1}),
    ?testList([233], ?BACKWARD, {timestamp, 12345}, #{offset => -34, limit => 1}),
    ?testList([66], ?FORWARD, {timestamp, 12345}, #{offset => -34, limit => 1}),

    ?testList(?range(199, 80), ?BACKWARD, {timestamp, 12345}, #{offset => 0, limit => 120}),
    ?testList(?range(100, 219), ?FORWARD, {timestamp, 12345}, #{offset => 0, limit => 120}),
    ?testList(?range(184, 65), ?BACKWARD, {timestamp, 12345}, #{offset => 15, limit => 120}),
    ?testList(?range(115, 234), ?FORWARD, {timestamp, 12345}, #{offset => 15, limit => 120}),

    ?testList(?range(299, 250), ?BACKWARD, {timestamp, 24690}, #{offset => 0, limit => 50}),
    ?testList(?range(200, 249), ?FORWARD, {timestamp, 24690}, #{offset => 0, limit => 50}),
    ?testList(?range(19, 0), ?BACKWARD, {timestamp, 24690}, #{offset => 280, limit => 50}),
    ?testList([], ?FORWARD, {timestamp, 24690}, #{offset => 280, limit => 50}),

    % listing limit is capped at 1000
    ?testList(?range(99, 0), ?BACKWARD, {timestamp, 0}, #{offset => 0, limit => 4000}),
    ?testList(?range(0, 299), ?FORWARD, {timestamp, 0}, #{offset => 0, limit => 4000}),
    ?testList(?range(179, 0), ?BACKWARD, {timestamp, 0}, #{offset => -80, limit => 4000}),
    ?testList(?range(0, 299), ?FORWARD, {timestamp, 0}, #{offset => -80, limit => 4000}).


list_log_with_irregular_timestamps(_, _) ->
    T0 = 0,
    Intervals = [0, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000],
    lists:foreach(fun(Interval) ->
        append(#{count => 1, first_at => T0 + Interval})
    end, Intervals),

    ?testList([0], ?BACKWARD, {timestamp, T0}, #{offset => 0, limit => 1}),
    ?testList([0], ?FORWARD, {timestamp, T0}, #{offset => 0, limit => 1}),
    ?testList([], ?BACKWARD, {timestamp, T0}, #{offset => 6, limit => 1}),
    ?testList([6], ?FORWARD, {timestamp, T0}, #{offset => 6, limit => 1}),

    ?testList([11], ?BACKWARD, {timestamp, T0 + 100000000000}, #{offset => 0, limit => 1}),
    ?testList([11], ?FORWARD, {timestamp, T0 + 100000000000}, #{offset => 0, limit => 1}),
    ?testList([8], ?BACKWARD, {timestamp, T0 + 100000000000}, #{offset => 3, limit => 1}),
    ?testList([], ?FORWARD, {timestamp, T0 + 100000000000}, #{offset => 3, limit => 1}),

    ?testList(?range(11, 7), ?BACKWARD, {timestamp, T0 + 100000000000}, #{offset => 0, limit => 5}),
    ?testList([11], ?FORWARD, {timestamp, T0 + 100000000000}, #{offset => 0, limit => 5}),
    ?testList([0], ?BACKWARD, {timestamp, T0 + 100000000000}, #{offset => 11, limit => 5}),
    ?testList([], ?FORWARD, {timestamp, T0 + 100000000000}, #{offset => 11, limit => 5}),

    ?testList([0], ?BACKWARD, {timestamp, T0}, #{offset => 0, limit => 5}),
    ?testList(?range(0, 4), ?FORWARD, {timestamp, T0}, #{offset => 0, limit => 5}),
    ?testList(?range(2, 0), ?BACKWARD, {timestamp, T0}, #{offset => -2, limit => 5}),
    ?testList(?range(0, 4), ?FORWARD, {timestamp, T0}, #{offset => -2, limit => 5}),

    ?testList(?range(6, 2), ?BACKWARD, {timestamp, T0 + 1000000}, #{offset => 0, limit => 5}),
    ?testList(?range(6, 10), ?FORWARD, {timestamp, T0 + 1000000}, #{offset => 0, limit => 5}),
    ?testList(?range(10, 6), ?BACKWARD, {timestamp, T0 + 1000000}, #{offset => -4, limit => 5}),
    ?testList(?range(2, 6), ?FORWARD, {timestamp, T0 + 1000000}, #{offset => -4, limit => 5}),

    ?testList(?range(6, 2), ?BACKWARD, {timestamp, T0 + 1000001}, #{offset => 0, limit => 5}),
    ?testList(?range(7, 11), ?FORWARD, {timestamp, T0 + 1000001}, #{offset => 0, limit => 5}),
    ?testList(?range(3, 0), ?BACKWARD, {timestamp, T0 + 1000001}, #{offset => 3, limit => 5}),
    ?testList(?range(10, 11), ?FORWARD, {timestamp, T0 + 1000001}, #{offset => 3, limit => 5}).


list_log_with_one_element(_, _) ->
    append(#{count => 1, first_at => 9999}),

    ?testList([0], ?BACKWARD, undefined, #{offset => 0, limit => 0}),
    ?testList([0], ?FORWARD, undefined, #{offset => 0, limit => 0}),

    ?testList([0], ?BACKWARD, undefined, #{offset => 0, limit => -15}),
    ?testList([0], ?FORWARD, undefined, #{offset => 0, limit => -1}),

    ?testList([0], ?BACKWARD, undefined, #{offset => 0, limit => 1}),
    ?testList([0], ?FORWARD, undefined, #{offset => 0, limit => 1}),

    ?testList([0], ?BACKWARD, undefined, #{offset => 0, limit => ?rand(1000)}),
    ?testList([0], ?FORWARD, undefined, #{offset => 0, limit => ?rand(1000)}),

    ?testList([], ?BACKWARD, undefined, #{offset => 1, limit => ?rand(1000)}),
    ?testList([], ?FORWARD, undefined, #{offset => 1, limit => ?rand(1000)}),

    ?testList([0], ?BACKWARD, undefined, #{offset => -1, limit => ?rand(1000)}),
    ?testList([0], ?FORWARD, undefined, #{offset => -1, limit => ?rand(1000)}),

    ?testList([0], ?BACKWARD, {index, 0}, #{offset => 0, limit => ?rand(1000)}),
    ?testList([0], ?FORWARD, {index, 0}, #{offset => 0, limit => ?rand(1000)}),
    ?testList([], ?BACKWARD, {index, 0}, #{offset => 1, limit => ?rand(1000)}),
    ?testList([], ?FORWARD, {index, 0}, #{offset => 1, limit => ?rand(1000)}),
    ?testList([0], ?BACKWARD, {index, 0}, #{offset => -1, limit => ?rand(1000)}),
    ?testList([0], ?FORWARD, {index, 0}, #{offset => -1, limit => ?rand(1000)}),

    ?testList([0], ?BACKWARD, {index, 4}, #{offset => 0, limit => ?rand(1000)}),
    ?testList([], ?FORWARD, {index, 4}, #{offset => 0, limit => ?rand(1000)}),
    ?testList([], ?BACKWARD, {index, 4}, #{offset => 2, limit => ?rand(1000)}),
    ?testList([], ?FORWARD, {index, 4}, #{offset => 2, limit => ?rand(1000)}),
    ?testList([0], ?BACKWARD, {index, 4}, #{offset => -8, limit => ?rand(1000)}),
    ?testList([0], ?FORWARD, {index, 4}, #{offset => -8, limit => ?rand(1000)}),

    ?testList([], ?BACKWARD, {index, -8}, #{offset => 0, limit => ?rand(1000)}),
    ?testList([0], ?FORWARD, {index, -8}, #{offset => 0, limit => ?rand(1000)}),
    ?testList([], ?BACKWARD, {index, -8}, #{offset => 3, limit => ?rand(1000)}),
    ?testList([], ?FORWARD, {index, -8}, #{offset => 3, limit => ?rand(1000)}),
    ?testList([0], ?BACKWARD, {index, -8}, #{offset => -2, limit => ?rand(1000)}),
    ?testList([0], ?FORWARD, {index, -8}, #{offset => -2, limit => ?rand(1000)}),

    ?testList([0], ?BACKWARD, {timestamp, 9999}, #{offset => 0, limit => ?rand(1000)}),
    ?testList([0], ?FORWARD, {timestamp, 9999}, #{offset => 0, limit => ?rand(1000)}),
    ?testList([], ?BACKWARD, {timestamp, 9999}, #{offset => 56, limit => ?rand(1000)}),
    ?testList([], ?FORWARD, {timestamp, 9999}, #{offset => 56, limit => ?rand(1000)}),
    ?testList([0], ?BACKWARD, {timestamp, 9999}, #{offset => -100, limit => ?rand(1000)}),
    ?testList([0], ?FORWARD, {timestamp, 9999}, #{offset => -100, limit => ?rand(1000)}),

    ?testList([0], ?BACKWARD, {timestamp, 15683}, #{offset => 0, limit => ?rand(1000)}),
    ?testList([], ?FORWARD, {timestamp, 15683}, #{offset => 0, limit => ?rand(1000)}),
    ?testList([], ?BACKWARD, {timestamp, 15683}, #{offset => 9, limit => ?rand(1000)}),
    ?testList([], ?FORWARD, {timestamp, 15683}, #{offset => 9, limit => ?rand(1000)}),
    ?testList([0], ?BACKWARD, {timestamp, 15683}, #{offset => -895, limit => ?rand(1000)}),
    ?testList([0], ?FORWARD, {timestamp, 15683}, #{offset => -895, limit => ?rand(1000)}),

    ?testList([], ?BACKWARD, {timestamp, 0}, #{offset => 0, limit => ?rand(1000)}),
    ?testList([0], ?FORWARD, {timestamp, 0}, #{offset => 0, limit => ?rand(1000)}),
    ?testList([], ?BACKWARD, {timestamp, 0}, #{offset => 33, limit => ?rand(1000)}),
    ?testList([], ?FORWARD, {timestamp, 0}, #{offset => 33, limit => ?rand(1000)}),
    ?testList([0], ?BACKWARD, {timestamp, 0}, #{offset => -70, limit => ?rand(1000)}),
    ?testList([0], ?FORWARD, {timestamp, 0}, #{offset => -70, limit => ?rand(1000)}).


list_log_with_one_full_node(_, MaxEntriesPerNode) ->
    append(#{count => MaxEntriesPerNode, first_at => 0, interval => 1}),

    EffLimit = min(1000, MaxEntriesPerNode),
    MaxEntryIndex = MaxEntriesPerNode - 1,
    ExpBackwardResult = ?range(MaxEntryIndex, max(0, MaxEntryIndex - EffLimit + 1)),

    ?testList([MaxEntryIndex], ?BACKWARD, undefined, #{limit => 1}),
    ?testList([0], ?FORWARD, undefined, #{limit => 1}),

    ?testList(ExpBackwardResult, ?BACKWARD, undefined, #{limit => MaxEntriesPerNode}),
    ?testList(?range(0, EffLimit - 1), ?FORWARD, undefined, #{limit => MaxEntriesPerNode}),

    ?testList(ExpBackwardResult, ?BACKWARD, {index, MaxEntryIndex}, #{limit => MaxEntriesPerNode}),
    ?testList(?range(0, EffLimit - 1), ?FORWARD, {index, 0}, #{limit => MaxEntriesPerNode}),

    ?testList([0], ?BACKWARD, {index, 0}, #{limit => MaxEntriesPerNode}),
    ?testList([MaxEntryIndex], ?FORWARD, {index, MaxEntryIndex}, #{limit => MaxEntriesPerNode}),

    ?testList(ExpBackwardResult, ?BACKWARD, {timestamp, MaxEntryIndex}, #{limit => MaxEntriesPerNode}),
    ?testList(?range(0, EffLimit - 1), ?FORWARD, {timestamp, 0}, #{limit => MaxEntriesPerNode}),

    ?testList([0], ?BACKWARD, {timestamp, 0}, #{limit => MaxEntriesPerNode}),
    ?testList([MaxEntryIndex], ?FORWARD, {timestamp, MaxEntryIndex}, #{limit => MaxEntriesPerNode}).


size_based_pruning(LogId, MaxEntriesPerNode) ->
    Threshold = MaxEntriesPerNode,
    create_log_for_test(#{
        max_entries_per_node => MaxEntriesPerNode,
        size_pruning_threshold => Threshold
    }),

    append(#{count => Threshold, first_at => 0, interval => 1}),
    ?testList([0], ?FORWARD, undefined, #{limit => 1}),
    ?testList([Threshold - 1], ?BACKWARD, undefined, #{limit => 1}),

    % size based pruning has the tolerance of max_entries_per_node - the size must be
    % exceeded by at least one full archival node
    append(#{count => MaxEntriesPerNode, interval => 1}),
    ?testList([0], ?FORWARD, undefined, #{limit => 1}),
    ?testList([Threshold + MaxEntriesPerNode - 1], ?BACKWARD, undefined, #{limit => 1}),

    append(#{count => 1, interval => 1}),
    ?testList([MaxEntriesPerNode], ?FORWARD, undefined, #{limit => 1}),
    ?testList([Threshold + MaxEntriesPerNode], ?BACKWARD, undefined, #{limit => 1}),
    ?assertNot(nodes_up_to_number_exist(LogId, 0)),

    append(#{count => Threshold * 3, interval => 1}),
    OldestIdx = 3 * Threshold + MaxEntriesPerNode,
    NewestIdx = 4 * Threshold + MaxEntriesPerNode,
    ?testList([OldestIdx], ?FORWARD, undefined, #{limit => 1}),
    ?testList([NewestIdx], ?BACKWARD, undefined, #{limit => 1}),
    ?assertNot(nodes_up_to_number_exist(LogId, 3)),

    ?testList([OldestIdx], ?FORWARD, {index, 0}, #{limit => 1}),
    ?testList([NewestIdx], ?BACKWARD, {index, NewestIdx}, #{limit => 1}),
    case MaxEntriesPerNode >= 3 of
        true ->
            ?testList([OldestIdx + 3], ?FORWARD, {index, 0}, #{limit => 1, offset => 3}),
            ?testList([NewestIdx - 3], ?BACKWARD, {index, NewestIdx}, #{limit => 1, offset => 3});
        false ->
            ok
    end,
    ?testList([OldestIdx], ?FORWARD, {index, 0}, #{limit => 1}),
    ?testList([NewestIdx], ?BACKWARD, {index, NewestIdx}, #{limit => 1}),

    ?testList([OldestIdx], ?FORWARD, {timestamp, 0}, #{limit => 1}),
    ?testList([NewestIdx], ?BACKWARD, {timestamp, NewestIdx}, #{limit => 1}),
    ?testList([OldestIdx], ?FORWARD, {timestamp, 0}, #{limit => 1, offset => -5}),
    ?testList([NewestIdx], ?BACKWARD, {timestamp, NewestIdx}, #{limit => 1, offset => -5}),
    ?testList([OldestIdx], ?FORWARD, {timestamp, 0}, #{limit => 1}),
    ?testList([NewestIdx], ?BACKWARD, {timestamp, NewestIdx}, #{limit => 1}).


size_based_pruning_with_low_threshold(LogId, MaxEntriesPerNode) ->
    create_log_for_test(#{
        max_entries_per_node => MaxEntriesPerNode,
        size_pruning_threshold => 0
    }),
    append(#{count => 2 * MaxEntriesPerNode}),
    ?testList([0], ?FORWARD, undefined, #{limit => 1}),
    ?testList([2 * MaxEntriesPerNode - 1], ?BACKWARD, undefined, #{limit => 1}),
    % size pruning is triggered when an archival node is saved
    append(#{count => 1}),
    ?testList([MaxEntriesPerNode], ?FORWARD, undefined, #{limit => 1}),
    ?testList([2 * MaxEntriesPerNode], ?BACKWARD, undefined, #{limit => 1}),
    ?assertNot(nodes_up_to_number_exist(LogId, 0)),

    create_log_for_test(#{
        max_entries_per_node => MaxEntriesPerNode,
        size_pruning_threshold => MaxEntriesPerNode
    }),
    append(#{count => 3 * MaxEntriesPerNode}),
    ?testList([MaxEntriesPerNode], ?FORWARD, undefined, #{limit => 1}),
    ?testList([3 * MaxEntriesPerNode - 1], ?BACKWARD, undefined, #{limit => 1}),
    ?assertNot(nodes_up_to_number_exist(LogId, 0)),
    append(#{count => 1}),
    ?testList([2 * MaxEntriesPerNode], ?FORWARD, undefined, #{limit => 1}),
    ?testList([3 * MaxEntriesPerNode], ?BACKWARD, undefined, #{limit => 1}),
    ?assertNot(nodes_up_to_number_exist(LogId, 1)).


age_based_pruning(LogId, MaxEntriesPerNode) ->
    Threshold = MaxEntriesPerNode,
    create_log_for_test(#{
        max_entries_per_node => MaxEntriesPerNode,
        age_pruning_threshold => Threshold
    }),

    append(#{count => MaxEntriesPerNode, first_at => 0, interval => 1000}),
    ?testList([0], ?FORWARD, undefined, #{limit => 1}),
    ?testList([MaxEntriesPerNode - 1], ?BACKWARD, undefined, #{limit => 1}),

    append(#{count => MaxEntriesPerNode, interval => 1000}),
    ?testList([MaxEntriesPerNode], ?FORWARD, undefined, #{limit => 1, required_access => allow_updates}),
    ?testList([2 * MaxEntriesPerNode - 1], ?BACKWARD, undefined, #{limit => 1}),

    append(#{count => 3 * MaxEntriesPerNode, interval => 0}),
    case MaxEntriesPerNode of
        1 ->
            % in case of max_entries_per_node=1, the interval between entries is equal to the
            % age pruning threshold and the entries are pruned immediately as the new ones come
            % (so that pruning during listing is not needed)
            ?testList([2 * MaxEntriesPerNode], ?FORWARD, undefined, #{limit => 1});
        _ ->
            ?testList([MaxEntriesPerNode], ?FORWARD, undefined, #{limit => 1})
    end,
    ?testList([5 * MaxEntriesPerNode - 1], ?BACKWARD, undefined, #{limit => 1}),

    % when age pruning is enabled, nodes should expire by themselves with corresponding ttl
    clock_freezer_mock:simulate_seconds_passing(Threshold),
    ?assertNot(nodes_up_to_number_exist(LogId, 3)),

    % nodes should be pruned even if no update operations are performed,
    % but excluding the newest node
    ?testList([4 * MaxEntriesPerNode], ?FORWARD, undefined, #{limit => 1, required_access => allow_updates}),
    ?testList([5 * MaxEntriesPerNode - 1], ?BACKWARD, undefined, #{limit => 1}),

    append(#{count => 1, interval => 0}),
    ?testList([5 * MaxEntriesPerNode], ?FORWARD, undefined, #{limit => 1, required_access => allow_updates}),
    ?testList([5 * MaxEntriesPerNode], ?BACKWARD, undefined, #{limit => 1}),
    ?assertNot(nodes_up_to_number_exist(LogId, 4)).


age_based_pruning_with_ttl_set(_, MaxEntriesPerNode) ->
    Threshold = 4 * rand:uniform(100000),
    LogId = create_log_for_test(#{
        max_entries_per_node => MaxEntriesPerNode,
        age_pruning_threshold => Threshold
    }),

    append(#{count => MaxEntriesPerNode, first_at => Threshold div 4 * 0 * 1000, interval => 0}),
    append(#{count => MaxEntriesPerNode, first_at => Threshold div 4 * 1 * 1000, interval => 0}),
    append(#{count => MaxEntriesPerNode, first_at => Threshold div 4 * 2 * 1000, interval => 0}),
    append(#{count => MaxEntriesPerNode, first_at => Threshold div 4 * 3 * 1000, interval => 0}),

    ?assertEqual(ok, call_set_ttl(LogId, Threshold div 4 * 3)),

    ?testList([0], ?FORWARD, undefined, #{limit => 1}),
    ?testList([4 * MaxEntriesPerNode - 1], ?BACKWARD, undefined, #{limit => 1}),

    clock_freezer_mock:simulate_seconds_passing(Threshold div 4),
    ?testList([MaxEntriesPerNode], ?FORWARD, undefined, #{limit => 1, required_access => allow_updates}),
    ?testList([4 * MaxEntriesPerNode - 1], ?BACKWARD, undefined, #{limit => 1}),
    ?assertNot(nodes_up_to_number_exist(LogId, 0)),

    clock_freezer_mock:simulate_seconds_passing(Threshold div 4),
    ?testList([2 * MaxEntriesPerNode], ?FORWARD, undefined, #{limit => 1, required_access => allow_updates}),
    ?testList([4 * MaxEntriesPerNode - 1], ?BACKWARD, undefined, #{limit => 1}),
    ?assertNot(nodes_up_to_number_exist(LogId, 1)),

    % when the TTL passes, all log data should be deleted, regardless of the
    % age-based pruning threshold
    clock_freezer_mock:simulate_seconds_passing(Threshold div 4),
    foreach_archival_node_number(4 * MaxEntriesPerNode, MaxEntriesPerNode, fun(NodeNumber) ->
        ?assertNot(node_exists(LogId, NodeNumber))
    end),

    ?assertEqual({error, not_found}, call_list(LogId, #{direction => ?FORWARD}, allow_updates)),
    ?assertEqual({error, not_found}, call_list(LogId, #{direction => ?BACKWARD}, allow_updates)),
    ?assertEqual({error, not_found}, call_append(LogId, <<"log">>)),
    ?assertEqual({error, not_found}, call_set_ttl(LogId, Threshold)).


list_inexistent_log(_, _) ->
    InexistentLogId = str_utils:rand_hex(16),
    ?assertEqual({error, not_found}, call_list(InexistentLogId, #{
        direction => lists_utils:random_element([?FORWARD, ?BACKWARD]),
        start_from => lists_utils:random_element([undefined, {index, ?rand(1000)}, {timestamp, ?rand(1000)}]),
        offset => ?rand(1000) - 500,
        limit => ?rand(1000)
    }, allow_updates)).


list_empty_log(_, _) ->
    lists:foreach(fun(_) ->
        ?testList([], ?BACKWARD, undefined, #{offset => 500 - ?rand(1000), limit => ?rand(1000)}),
        ?testList([], ?FORWARD, undefined, #{offset => 500 - ?rand(1000), limit => ?rand(1000)}),
        ?testList([], ?BACKWARD, {index, ?rand(1000)}, #{offset => 500 - ?rand(1000), limit => ?rand(1000)}),
        ?testList([], ?FORWARD, {index, ?rand(1000)}, #{offset => 500 - ?rand(1000), limit => ?rand(1000)}),
        ?testList([], ?BACKWARD, {timestamp, ?rand(1000)}, #{offset => 500 - ?rand(1000), limit => ?rand(1000)}),
        ?testList([], ?FORWARD, {timestamp, ?rand(1000)}, #{offset => 500 - ?rand(1000), limit => ?rand(1000)})
    end, ?range(1, 100)).


list_log_with_missing_nodes(LogId, MaxEntriesPerNode) ->
    % make sure that there is at least one archival node
    EntryCount = max(1000, MaxEntriesPerNode + 1),
    append(#{count => EntryCount}),

    % simulate a situation when some nodes are missing - this is possible for
    % example when the listing intertwines with documents' expiring - in such
    % case the code such not crash, but return an error
    MaxNodeNumber = (EntryCount - 1) div MaxEntriesPerNode,
    ArchivalNodeNumbers = lists:seq(0, MaxNodeNumber - 1),
    MissingNodeNumbers = lists_utils:random_sublist(ArchivalNodeNumbers, 1, all),
    lists:foreach(fun(NodeNumber) ->
        ?assertEqual(ok, delete_node(LogId, NodeNumber))
    end, MissingNodeNumbers),
    ?assertEqual({error, internal_server_error}, call_list(LogId, #{
        direction => lists_utils:random_element([?FORWARD, ?BACKWARD]),
        limit => EntryCount
    }, allow_updates)),

    % if the sentinel is gone, the whole log is considered to be inexistent
    ?assertEqual(ok, delete_sentinel(LogId)),
    ?assertEqual({error, not_found}, call_list(LogId, #{
        direction => lists_utils:random_element([?FORWARD, ?BACKWARD]),
        limit => EntryCount
    }, allow_updates)).


append_with_time_warps(LogId, _) ->
    % in case of backward time warps, the infinite log should artificially
    % keep the entries monotonic - consecutive entry cannot be older than the previous
    clock_freezer_mock:set_current_time_millis(1000),
    call_append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(950),
    call_append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(953),
    call_append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(993),
    call_append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(1000),
    call_append(LogId, str_utils:rand_hex(100)),

    {ok, {done, ListResultsPrim}} = call_list(LogId, #{direction => ?FORWARD}, allow_updates),
    ?assertEqual(
        [1000, 1000, 1000, 1000, 1000],
        extract_timestamps(ListResultsPrim)
    ),

    % forward time warp should cause the new entries to get later timestamps
    clock_freezer_mock:set_current_time_millis(1300),
    call_append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(1306),
    call_append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(1296),
    call_append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(1296),
    call_append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(1318),
    call_append(LogId, str_utils:rand_hex(100)),

    {ok, {done, ListResultsBis}} = call_list(LogId, #{direction => ?FORWARD}, allow_updates),
    ?assertEqual(
        [1000, 1000, 1000, 1000, 1000, 1300, 1306, 1306, 1306, 1318],
        extract_timestamps(ListResultsBis)
    ).


append_too_large_content(LogId, MaxEntriesPerNode) ->
    TooLargeSize = 20000000 div MaxEntriesPerNode,
    % rand hex returns two output bytes for every input byte
    Content = str_utils:rand_hex(TooLargeSize div 2),
    ?assertEqual({error, log_content_too_large}, call_append(LogId, Content)).

%%=====================================================================
%% Helper functions
%%=====================================================================

create_log_for_test(LogOpts) ->
    LogId = datastore_key:new(),
    ?assertEqual(ok, call_create(LogId, LogOpts)),
    store_current_log_id(LogId),
    LogId.


append(Spec) ->
    LogId = get_current_log_id(),
    EntryCount = maps:get(count, Spec, 1),
    case maps:find(first_at, Spec) of
        {ok, StartingTimestamp} ->
            clock_freezer_mock:set_current_time_millis(StartingTimestamp);
        error ->
            ok
    end,
    Interval = maps:get(interval, Spec, random),
    lists:foreach(fun(_) ->
        Content = str_utils:rand_hex(?rand(500)),
        EntryNumber = get_entry_count(LogId),
        Timestamp = clock_freezer_mock:current_time_millis(),
        store_entry(LogId, EntryNumber, {Timestamp, Content}),
        call_append(LogId, Content),
        store_entry_count(LogId, EntryNumber + 1),
        clock_freezer_mock:simulate_millis_passing(case Interval of
            random -> ?rand(10000);
            _ -> Interval
        end)
    end, ?range(1, EntryCount)).


% to be called within an ?assert() macro to pinpoint the failing line
list_indices_and_verify(ExpectedIndices, Direction, StartFrom, OtherOpts) ->
    try
        Offset = maps:get(offset, OtherOpts, 0),
        Limit = maps:get(limit, OtherOpts, 1000),
        LogId = get_current_log_id(),
        ListOpts = #{
            direction => Direction,
            start_from => StartFrom,
            offset => Offset,
            limit => Limit
        },

        RequiredAccess = maps:get(required_access, OtherOpts, readonly),
        Result = case RequiredAccess of
            readonly ->
                RandomAccessMode = lists_utils:random_element([readonly, allow_updates]),
                call_list(LogId, ListOpts, RandomAccessMode);
            allow_updates ->
                ?assertEqual({error, update_required}, call_list(LogId, ListOpts, readonly)),
                call_list(LogId, ListOpts, allow_updates)
        end,
        ?assertMatch({ok, {_, _}}, Result),
        {ok, {ProgressMarker, Batch}} = Result,

        ListedIndices = lists:map(fun({EntryIndex, {Timestamp, Content}}) ->
            ?assertEqual({Timestamp, Content}, get_entry(LogId, EntryIndex)),
            EntryIndex
        end, Batch),
        ?assertEqual(ExpectedIndices, ListedIndices),

        ExpProgressMarker = case length(ExpectedIndices) of
            0 ->
                done;
            _ ->
                LastEntryIndex = case Direction of
                    ?FORWARD ->
                        get_entry_count(LogId) - 1;
                    ?BACKWARD ->
                        {ok, {_, [{FirstIndex, _}]}} = call_list(LogId, #{
                            direction => ?FORWARD,
                            limit => 1
                        }, allow_updates),
                        FirstIndex
                end,
                case lists:last(ExpectedIndices) of
                    LastEntryIndex -> done;
                    _ -> more
                end
        end,
        ?assertEqual(ExpProgressMarker, ProgressMarker),

        true
    catch _:_ ->
        false
    end.


extract_timestamps(ListingResult) ->
    [Timestamp || {_Id, {Timestamp, _Content}} <- ListingResult].


store_current_log_id(Id) ->
    node_cache:put(current_log_id, Id).


get_current_log_id() ->
    node_cache:get(current_log_id).


store_entry_count(Id, Count) ->
    node_cache:put({entry_count, Id}, Count).


get_entry_count(Id) ->
    node_cache:get({entry_count, Id}, 0).


store_entry(Id, EntryIndex, Entry) ->
    node_cache:put({entry, Id, EntryIndex}, Entry).


get_entry(Id, EntryIndex) ->
    node_cache:get({entry, Id, EntryIndex}).


sentinel_exists(LogId) ->
    case acquire_sentinel(LogId, skip_pruning, readonly) of
        {ok, _} -> true;
        {error, not_found} -> false
    end.


node_exists(LogId, NodeNumber) ->
    case get_node(LogId, NodeNumber) of
        {ok, _} -> true;
        {error, not_found} -> false
    end.


nodes_up_to_number_exist(LogId, 0) ->
    node_exists(LogId, 0);
nodes_up_to_number_exist(LogId, MaxNodeNumber) ->
    FirstNodeExists = nodes_up_to_number_exist(LogId, 0),
    OtherNodesExist = lists:all(fun(NodeNumber) ->
        node_exists(LogId, NodeNumber)
    end, lists:seq(1, MaxNodeNumber)),
    ?assertEqual(FirstNodeExists, OtherNodesExist),
    OtherNodesExist.


foreach_archival_node_number(EntryCount, MaxEntriesPerNode, Callback) ->
    MaxNodeNumber = (EntryCount - 1) div MaxEntriesPerNode,
    % do not run the callback for sentinel (which has MaxNodeNumber)
    lists:foreach(Callback, lists:seq(0, MaxNodeNumber - 1)).


signum(X) when X < 0 -> -1;
signum(X) when X == 0 -> 0;
signum(X) when X > 0 -> 1.


mock_datastore_doc() ->
    meck:new(datastore_doc, [passthrough]),
    meck:expect(datastore_doc, fetch,
        fun(_Ctx, Id, Batch) ->
            {node_cache:get({?MODULE, Id}, {error, not_found}), Batch}
        end
    ),
    meck:expect(datastore_doc, save,
        fun(Ctx, Id, Document, Batch) ->
            Ttl = kv_utils:get([disc_driver_ctx, expiry], Ctx, infinity),
            MockTtl = case is_integer(Ttl) andalso Ttl > 2592000 of
                true -> Ttl - global_clock:timestamp_seconds();
                false -> Ttl
            end,
            node_cache:put({?MODULE, Id}, {ok, Document}, MockTtl),
            {{ok, Document}, Batch}
        end
    ),
    meck:expect(datastore_doc, delete,
        fun(_Ctx, Id, Batch) ->
            node_cache:clear({?MODULE, Id}),
            {ok, Batch}
        end
    ),
    ok.

%%=====================================================================
%% Convenience functions
%%=====================================================================

call_create(LogId, Opts) ->
    {Res, _} = infinite_log:create(?DATASTORE_CTX, LogId, Opts, ?DATASTORE_BATCH),
    Res.

call_destroy(LogId) ->
    {Res, _} = infinite_log:destroy(?DATASTORE_CTX, LogId, ?DATASTORE_BATCH),
    Res.

call_append(LogId, Content) ->
    {Res, _} = infinite_log:append(?DATASTORE_CTX, LogId, Content, ?DATASTORE_BATCH),
    Res.

call_list(LogId, Opts, AccessMode) ->
    {Res, _} = infinite_log:list(?DATASTORE_CTX, LogId, Opts, AccessMode, ?DATASTORE_BATCH),
    Res.

call_set_ttl(LogId, Ttl) ->
    {Res, _} = infinite_log:set_ttl(?DATASTORE_CTX, LogId, Ttl, ?DATASTORE_BATCH),
    Res.

delete_node(LogId, NodeNumber) ->
    {Res, _} = infinite_log_node:delete(?DATASTORE_CTX, LogId, NodeNumber, ?DATASTORE_BATCH),
    Res.

delete_sentinel(LogId) ->
    {Res, _} = infinite_log_sentinel:delete(?DATASTORE_CTX, LogId, ?DATASTORE_BATCH),
    Res.

acquire_sentinel(LogId, skip_pruning, readonly) ->
    {Res, _} = infinite_log_sentinel:acquire(?DATASTORE_CTX, LogId, skip_pruning, readonly, ?DATASTORE_BATCH),
    Res.

get_node(LogId, NodeNumber) -> 
    {Res, _} = infinite_log_node:get(?DATASTORE_CTX, LogId, NodeNumber, ?DATASTORE_BATCH),
    Res.

-endif.
