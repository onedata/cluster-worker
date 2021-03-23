%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2019 ACK CYFRONET AGH
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

-define(BACKWARD, backward_from_newest).
-define(FORWARD, forward_from_oldest).

-define(range(From, To), lists:seq(From, To, signum(To - From))).
-define(rand(Limit), rand:uniform(Limit)).
-define(testList(Result, Direction, StartFrom, OtherOpts), ?assertEqual(
    Result, list_ids_and_verify(Direction, StartFrom, OtherOpts))
).

%%%===================================================================
%%% Tests
%%%===================================================================

% appending to log is tested during listing tests
-define(TEST_CASES, [
    {"create_and_destroy", fun create_and_destroy/2},
    {"list_inexistent_log", fun list_inexistent_log/2},
    {"list_empty_log", fun list_empty_log/2},
    {"list", fun list/2},
    {"list_from_id", fun list_from_id/2},
    {"list_from_timestamp", fun list_from_timestamp/2},
    {"list_log_with_the_same_timestamps", fun list_log_with_the_same_timestamps/2},
    {"list_log_with_clustered_timestamps", fun list_log_with_clustered_timestamps/2},
    {"list_log_with_irregular_timestamps", fun list_log_with_irregular_timestamps/2},
    {"list_log_with_one_element", fun list_log_with_one_element/2},
    {"append_with_time_warps", fun append_with_time_warps/2},
    {"append_too_large_content", fun append_too_large_content/2}
]).

-define(ELEMENTS_PER_NODE_VARIANTS, [
    1, 2, 3, 5, 11, 99, 301, 1000, 1999
]).

inf_log_test_() ->
    {foreach,
        fun() ->
            clock_freezer_mock:setup_locally([infinite_log]),
            node_cache:init()
        end,
        fun(_) ->
            clock_freezer_mock:teardown_locally(),
            node_cache:destroy()
        end,
        lists:flatmap(fun({Name, Fun}) ->
            lists:map(fun(MaxEntriesPerNode) ->
                {str_utils:format("~s [~B]", [Name, MaxEntriesPerNode]), fun() ->
                    LogId = datastore_key:new(),
                    ?assertEqual(ok, infinite_log:create(LogId, MaxEntriesPerNode)),
                    store_current_log_id(LogId),
                    Fun(LogId, MaxEntriesPerNode)
                end}
            end, ?ELEMENTS_PER_NODE_VARIANTS)
        end, ?TEST_CASES)
    }.


create_and_destroy(LogId, MaxEntriesPerNode) ->
    % the log is created in test setup
    EntryCount = 1000,
    MaxNodeNumber = (EntryCount - 1) div MaxEntriesPerNode,

    ForeachNodeNumber = fun(Callback) ->
        NodeNumbers = case MaxNodeNumber of
            0 -> [];
            _ -> lists:seq(0, MaxNodeNumber - 1)
        end,
        lists:foreach(Callback, NodeNumbers)
    end,

    ?assert(sentinel_exists(LogId)),
    ForeachNodeNumber(fun(NodeNumber) ->
        ?assertNot(node_exists(LogId, NodeNumber))
    end),

    append(#{count => EntryCount, first_at => 0, interval => 1}),
    ?assert(sentinel_exists(LogId)),
    ForeachNodeNumber(fun(NodeNumber) ->
        ?assert(node_exists(LogId, NodeNumber))
    end),

    ?assertEqual(ok, infinite_log:destroy(LogId)),
    ?assertNot(sentinel_exists(LogId)),
    ForeachNodeNumber(fun(NodeNumber) ->
        ?assertNot(node_exists(LogId, NodeNumber))
    end),

    ?assertEqual({error, not_found}, infinite_log:list(LogId, #{
        direction => lists_utils:random_element([?FORWARD, ?BACKWARD])
    })),
    ?assertEqual({error, not_found}, infinite_log:append(LogId, <<"log">>)),
    ?assertEqual(ok, infinite_log:destroy(LogId)).


list_inexistent_log(_, _) ->
    InexistentLogId = str_utils:rand_hex(16),
    ?assertEqual({error, not_found}, infinite_log:list(InexistentLogId, #{
        direction => lists_utils:random_element([?FORWARD, ?BACKWARD]),
        start_from => lists_utils:random_element([undefined, {id, ?rand(1000)}, {timestamp, ?rand(1000)}]),
        offset => ?rand(1000) - 500,
        limit => ?rand(1000)
    })).


list_empty_log(_, _) ->
    lists:foreach(fun(_) ->
        ?testList([], ?BACKWARD, undefined, #{off => 500 - ?rand(1000), lim => ?rand(1000)}),
        ?testList([], ?FORWARD, undefined, #{off => 500 - ?rand(1000), lim => ?rand(1000)}),
        ?testList([], ?BACKWARD, {id, ?rand(1000)}, #{off => 500 - ?rand(1000), lim => ?rand(1000)}),
        ?testList([], ?FORWARD, {id, ?rand(1000)}, #{off => 500 - ?rand(1000), lim => ?rand(1000)}),
        ?testList([], ?BACKWARD, {timestamp, ?rand(1000)}, #{off => 500 - ?rand(1000), lim => ?rand(1000)}),
        ?testList([], ?FORWARD, {timestamp, ?rand(1000)}, #{off => 500 - ?rand(1000), lim => ?rand(1000)})
    end, lists:seq(1, 100)).


list(_, _) ->
    append(#{count => 5000}),

    ?testList([4999], ?BACKWARD, undefined, #{off => 0, lim => 1}),
    ?testList([0], ?FORWARD, undefined, #{off => 0, lim => 1}),
    ?testList([4999], ?BACKWARD, undefined, #{off => -1, lim => 1}),
    ?testList([0], ?FORWARD, undefined, #{off => -1, lim => 1}),

    ?testList(?range(4999, 4980), ?BACKWARD, undefined, #{off => 0, lim => 20}),
    ?testList(?range(0, 19), ?FORWARD, undefined, #{off => 0, lim => 20}),
    ?testList(?range(4999, 4980), ?BACKWARD, undefined, #{off => -19, lim => 20}),
    ?testList(?range(0, 19), ?FORWARD, undefined, #{off => -19, lim => 20}),

    % listing limit is capped at 1000
    ?testList(?range(4999, 4000), ?BACKWARD, undefined, #{off => 0, lim => 5000}),
    ?testList(?range(0, 999), ?FORWARD, undefined, #{off => 0, lim => 5000}),
    ?testList(?range(2999, 2000), ?BACKWARD, undefined, #{off => 2000, lim => 5000}),
    ?testList(?range(2000, 2999), ?FORWARD, undefined, #{off => 2000, lim => 5000}),

    ?testList([0], ?BACKWARD, undefined, #{off => 4999, lim => 1}),
    ?testList([4999], ?FORWARD, undefined, #{off => 4999, lim => 1}),
    ?testList([0], ?BACKWARD, undefined, #{off => 4999, lim => 56}),
    ?testList([4999], ?FORWARD, undefined, #{off => 4999, lim => 56}),

    ?testList(?range(4979, 4965), ?BACKWARD, undefined, #{off => 20, lim => 15}),
    ?testList(?range(20, 34), ?FORWARD, undefined, #{off => 20, lim => 15}),
    % listing limit is capped at 1000
    ?testList(?range(4979, 3980), ?BACKWARD, undefined, #{off => 20, lim => 3000}),
    ?testList(?range(20, 1019), ?FORWARD, undefined, #{off => 20, lim => 3000}),
    ?testList(?range(4979, 3980), ?BACKWARD, undefined, #{off => 20, lim => 80000000}),
    ?testList(?range(20, 1019), ?FORWARD, undefined, #{off => 20, lim => 80000000}).


list_from_id(_, _) ->
    append(#{count => 5000}),

    ?testList([0], ?BACKWARD, {id, 0}, #{off => 0, lim => 1}),
    ?testList([0], ?FORWARD, {id, 0}, #{off => 0, lim => 1}),
    ?testList([0], ?BACKWARD, {id, 0}, #{off => 0, lim => 8}),
    ?testList(?range(0, 7), ?FORWARD, {id, 0}, #{off => 0, lim => 8}),

    ?testList([1], ?BACKWARD, {id, 0}, #{off => -1, lim => 1}),
    ?testList([0], ?FORWARD, {id, 0}, #{off => -1, lim => 1}),
    ?testList(?range(1, 0), ?BACKWARD, {id, 0}, #{off => -1, lim => 27}),
    ?testList(?range(0, 26), ?FORWARD, {id, 0}, #{off => -1, lim => 27}),

    ?testList([], ?BACKWARD, {id, 0}, #{off => 1, lim => 1}),
    ?testList([1], ?FORWARD, {id, 0}, #{off => 1, lim => 1}),
    ?testList([], ?BACKWARD, {id, 0}, #{off => 1, lim => 240}),
    ?testList(?range(1, 240), ?FORWARD, {id, 0}, #{off => 1, lim => 240}),

    ?testList([4999], ?BACKWARD, {id, 4999}, #{off => 0, lim => 1}),
    ?testList([4999], ?FORWARD, {id, 4999}, #{off => 0, lim => 1}),
    ?testList(?range(4999, 4987), ?BACKWARD, {id, 4999}, #{off => 0, lim => 13}),
    ?testList([4999], ?FORWARD, {id, 4999}, #{off => 0, lim => 13}),

    ?testList([4999], ?BACKWARD, {id, 4999}, #{off => -1, lim => 1}),
    ?testList([4998], ?FORWARD, {id, 4999}, #{off => -1, lim => 1}),
    ?testList(?range(4999, 4995), ?BACKWARD, {id, 4999}, #{off => -1, lim => 5}),
    ?testList(?range(4998, 4999), ?FORWARD, {id, 4999}, #{off => -1, lim => 5}),

    ?testList([4998], ?BACKWARD, {id, 4999}, #{off => 1, lim => 1}),
    ?testList([], ?FORWARD, {id, 4999}, #{off => 1, lim => 1}),
    % listing limit is capped at 1000
    ?testList(?range(4998, 3999), ?BACKWARD, {id, 4999}, #{off => 1, lim => 4800}),
    ?testList([], ?FORWARD, {id, 4999}, #{off => 1, lim => 4800}),

    ?testList(?range(2400, 2101), ?BACKWARD, {id, 2400}, #{off => 0, lim => 300}),
    ?testList(?range(2400, 2699), ?FORWARD, {id, 2400}, #{off => 0, lim => 300}),
    ?testList(?range(100, 0), ?BACKWARD, {id, 2400}, #{off => 2300, lim => 300}),
    ?testList(?range(4700, 4999), ?FORWARD, {id, 2400}, #{off => 2300, lim => 300}),
    ?testList(?range(3000, 2701), ?BACKWARD, {id, 2400}, #{off => -600, lim => 300}),
    ?testList(?range(1800, 2099), ?FORWARD, {id, 2400}, #{off => -600, lim => 300}),
    ?testList(?range(4999, 4700), ?BACKWARD, {id, 2400}, #{off => -2600, lim => 300}),
    ?testList(?range(0, 299), ?FORWARD, {id, 2400}, #{off => -2600, lim => 300}),

    ?testList([], ?BACKWARD, {id, -7}, #{off => 0, lim => 1}),
    ?testList([0], ?FORWARD, {id, -7}, #{off => 0, lim => 1}),

    ?testList(?range(3, 0), ?BACKWARD, {id, -7}, #{off => -4, lim => 49}),
    ?testList(?range(0, 48), ?FORWARD, {id, -7}, #{off => -4, lim => 49}),

    ?testList([], ?BACKWARD, {id, -7}, #{off => 9, lim => 506}),
    ?testList(?range(9, 514), ?FORWARD, {id, -7}, #{off => 9, lim => 506}),

    ?testList([4999], ?BACKWARD, {id, 9823655123764823}, #{off => 0, lim => 1}),
    ?testList([], ?FORWARD, {id, 9823655123764823}, #{off => 0, lim => 1}),

    ?testList(?range(4999, 4930), ?BACKWARD, {id, 9823655123764823}, #{off => -60, lim => 70}),
    ?testList(?range(4940, 4999), ?FORWARD, {id, 9823655123764823}, #{off => -60, lim => 70}),

    % listing limit is capped at 1000
    ?testList(?range(4985, 3986), ?BACKWARD, {id, 9823655123764823}, #{off => 14, lim => 7000}),
    ?testList([], ?FORWARD, {id, 9823655123764823}, #{off => 14, lim => 7000}).


list_from_timestamp(_, _) ->
    append(#{count => 1000, first_at => 0, interval => 1}),

    ?testList([], ?BACKWARD, {timestamp, -1}, #{off => 0, lim => 1}),
    ?testList([], ?FORWARD, {timestamp, 1000}, #{off => 0, lim => 1}),
    ?testList([], ?BACKWARD, {timestamp, -1}, #{off => 10, lim => 1}),
    ?testList([], ?FORWARD, {timestamp, 1000}, #{off => 10, lim => 1}),
    ?testList([9], ?BACKWARD, {timestamp, -1}, #{off => -10, lim => 1}),
    ?testList([990], ?FORWARD, {timestamp, 1000}, #{off => -10, lim => 1}),

    ?testList([999], ?BACKWARD, {timestamp, 1000}, #{off => 0, lim => 1}),
    ?testList([0], ?FORWARD, {timestamp, -1}, #{off => 0, lim => 1}),
    ?testList([319], ?BACKWARD, {timestamp, 1000}, #{off => 680, lim => 1}),
    ?testList([680], ?FORWARD, {timestamp, -1}, #{off => 680, lim => 1}),

    ?testList([999], ?BACKWARD, {timestamp, 999}, #{off => 0, lim => 1}),
    ?testList([0], ?FORWARD, {timestamp, 0}, #{off => 0, lim => 1}),
    ?testList([0], ?BACKWARD, {timestamp, 999}, #{off => 999, lim => 1}),
    ?testList([999], ?FORWARD, {timestamp, 0}, #{off => 999, lim => 1}),

    ?testList([0], ?BACKWARD, {timestamp, 0}, #{off => 0, lim => 1}),
    ?testList([999], ?FORWARD, {timestamp, 999}, #{off => 0, lim => 1}),
    ?testList([], ?BACKWARD, {timestamp, 0}, #{off => 1, lim => 1}),
    ?testList([], ?FORWARD, {timestamp, 999}, #{off => 1, lim => 1}),

    ?testList([500, 499], ?BACKWARD, {timestamp, 500}, #{off => 0, lim => 2}),
    ?testList([500, 501], ?FORWARD, {timestamp, 500}, #{off => 0, lim => 2}),
    ?testList([543, 542], ?BACKWARD, {timestamp, 500}, #{off => -43, lim => 2}),
    ?testList([457, 458], ?FORWARD, {timestamp, 500}, #{off => -43, lim => 2}),

    ?testList(?range(500, 401), ?BACKWARD, {timestamp, 500}, #{off => 0, lim => 100}),
    ?testList(?range(500, 599), ?FORWARD, {timestamp, 500}, #{off => 0, lim => 100}),
    ?testList(?range(34, 0), ?BACKWARD, {timestamp, 500}, #{off => 466, lim => 100}),
    ?testList(?range(966, 999), ?FORWARD, {timestamp, 500}, #{off => 466, lim => 100}).


list_log_with_the_same_timestamps(_, _) ->
    Timestamp = ?rand(9999999999),
    append(#{count => 100, first_at => Timestamp, interval => 0}),

    ?testList([99], ?BACKWARD, {timestamp, Timestamp}, #{off => 0, lim => 1}),
    ?testList([0], ?FORWARD, {timestamp, Timestamp}, #{off => 0, lim => 1}),
    ?testList([82], ?BACKWARD, {timestamp, Timestamp}, #{off => 17, lim => 1}),
    ?testList([17], ?FORWARD, {timestamp, Timestamp}, #{off => 17, lim => 1}),
    ?testList([99], ?BACKWARD, {timestamp, Timestamp}, #{off => -9, lim => 1}),
    ?testList([0], ?FORWARD, {timestamp, Timestamp}, #{off => -9, lim => 1}),

    ?testList(?range(99, 65), ?BACKWARD, {timestamp, Timestamp}, #{off => 0, lim => 35}),
    ?testList(?range(0, 34), ?FORWARD, {timestamp, Timestamp}, #{off => 0, lim => 35}),
    ?testList(?range(88, 54), ?BACKWARD, {timestamp, Timestamp}, #{off => 11, lim => 35}),
    ?testList(?range(11, 45), ?FORWARD, {timestamp, Timestamp}, #{off => 11, lim => 35}),
    ?testList(?range(99, 65), ?BACKWARD, {timestamp, Timestamp}, #{off => -30, lim => 35}),
    ?testList(?range(0, 34), ?FORWARD, {timestamp, Timestamp}, #{off => -30, lim => 35}).


list_log_with_clustered_timestamps(_, _) ->
    append(#{count => 100, first_at => 0, interval => 0}),
    append(#{count => 100, first_at => 12345, interval => 0}),
    append(#{count => 100, first_at => 24690, interval => 0}),

    ?testList(?range(99, 95), ?BACKWARD, {timestamp, 0}, #{off => 0, lim => 5}),
    ?testList(?range(0, 4), ?FORWARD, {timestamp, 0}, #{off => 0, lim => 5}),
    ?testList(?range(179, 175), ?BACKWARD, {timestamp, 0}, #{off => -80, lim => 5}),
    ?testList(?range(0, 4), ?FORWARD, {timestamp, 0}, #{off => -80, lim => 5}),

    ?testList([199], ?BACKWARD, {timestamp, 12345}, #{off => 0, lim => 1}),
    ?testList([100], ?FORWARD, {timestamp, 12345}, #{off => 0, lim => 1}),
    ?testList([233], ?BACKWARD, {timestamp, 12345}, #{off => -34, lim => 1}),
    ?testList([66], ?FORWARD, {timestamp, 12345}, #{off => -34, lim => 1}),

    ?testList(?range(199, 80), ?BACKWARD, {timestamp, 12345}, #{off => 0, lim => 120}),
    ?testList(?range(100, 219), ?FORWARD, {timestamp, 12345}, #{off => 0, lim => 120}),
    ?testList(?range(184, 65), ?BACKWARD, {timestamp, 12345}, #{off => 15, lim => 120}),
    ?testList(?range(115, 234), ?FORWARD, {timestamp, 12345}, #{off => 15, lim => 120}),

    ?testList(?range(299, 250), ?BACKWARD, {timestamp, 24690}, #{off => 0, lim => 50}),
    ?testList(?range(200, 249), ?FORWARD, {timestamp, 24690}, #{off => 0, lim => 50}),
    ?testList(?range(19, 0), ?BACKWARD, {timestamp, 24690}, #{off => 280, lim => 50}),
    ?testList([], ?FORWARD, {timestamp, 24690}, #{off => 280, lim => 50}),

    % listing limit is capped at 1000
    ?testList(?range(99, 0), ?BACKWARD, {timestamp, 0}, #{off => 0, lim => 4000}),
    ?testList(?range(0, 299), ?FORWARD, {timestamp, 0}, #{off => 0, lim => 4000}),
    ?testList(?range(179, 0), ?BACKWARD, {timestamp, 0}, #{off => -80, lim => 4000}),
    ?testList(?range(0, 299), ?FORWARD, {timestamp, 0}, #{off => -80, lim => 4000}).


list_log_with_irregular_timestamps(_, _) ->
    T0 = 0,
    Intervals = [0, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000],
    lists:foreach(fun(Interval) ->
        append(#{count => 1, first_at => T0 + Interval})
    end, Intervals),

    ?testList([0], ?BACKWARD, {timestamp, T0}, #{off => 0, lim => 1}),
    ?testList([0], ?FORWARD, {timestamp, T0}, #{off => 0, lim => 1}),
    ?testList([], ?BACKWARD, {timestamp, T0}, #{off => 6, lim => 1}),
    ?testList([6], ?FORWARD, {timestamp, T0}, #{off => 6, lim => 1}),

    ?testList([11], ?BACKWARD, {timestamp, T0 + 100000000000}, #{off => 0, lim => 1}),
    ?testList([11], ?FORWARD, {timestamp, T0 + 100000000000}, #{off => 0, lim => 1}),
    ?testList([8], ?BACKWARD, {timestamp, T0 + 100000000000}, #{off => 3, lim => 1}),
    ?testList([], ?FORWARD, {timestamp, T0 + 100000000000}, #{off => 3, lim => 1}),

    ?testList(?range(11, 7), ?BACKWARD, {timestamp, T0 + 100000000000}, #{off => 0, lim => 5}),
    ?testList([11], ?FORWARD, {timestamp, T0 + 100000000000}, #{off => 0, lim => 5}),
    ?testList([0], ?BACKWARD, {timestamp, T0 + 100000000000}, #{off => 11, lim => 5}),
    ?testList([], ?FORWARD, {timestamp, T0 + 100000000000}, #{off => 11, lim => 5}),

    ?testList([0], ?BACKWARD, {timestamp, T0}, #{off => 0, lim => 5}),
    ?testList(?range(0, 4), ?FORWARD, {timestamp, T0}, #{off => 0, lim => 5}),
    ?testList(?range(2, 0), ?BACKWARD, {timestamp, T0}, #{off => -2, lim => 5}),
    ?testList(?range(0, 4), ?FORWARD, {timestamp, T0}, #{off => -2, lim => 5}),

    ?testList(?range(6, 2), ?BACKWARD, {timestamp, T0 + 1000000}, #{off => 0, lim => 5}),
    ?testList(?range(6, 10), ?FORWARD, {timestamp, T0 + 1000000}, #{off => 0, lim => 5}),
    ?testList(?range(10, 6), ?BACKWARD, {timestamp, T0 + 1000000}, #{off => -4, lim => 5}),
    ?testList(?range(2, 6), ?FORWARD, {timestamp, T0 + 1000000}, #{off => -4, lim => 5}),

    ?testList(?range(6, 2), ?BACKWARD, {timestamp, T0 + 1000001}, #{off => 0, lim => 5}),
    ?testList(?range(7, 11), ?FORWARD, {timestamp, T0 + 1000001}, #{off => 0, lim => 5}),
    ?testList(?range(3, 0), ?BACKWARD, {timestamp, T0 + 1000001}, #{off => 3, lim => 5}),
    ?testList(?range(10, 11), ?FORWARD, {timestamp, T0 + 1000001}, #{off => 3, lim => 5}).


list_log_with_one_element(_, _) ->
    append(#{count => 1, first_at => 9999}),

    ?testList([0], ?BACKWARD, undefined, #{off => 0, lim => 0}),
    ?testList([0], ?FORWARD, undefined, #{off => 0, lim => 0}),

    ?testList([0], ?BACKWARD, undefined, #{off => 0, lim => -15}),
    ?testList([0], ?FORWARD, undefined, #{off => 0, lim => -1}),

    ?testList([0], ?BACKWARD, undefined, #{off => 0, lim => 1}),
    ?testList([0], ?FORWARD, undefined, #{off => 0, lim => 1}),

    ?testList([0], ?BACKWARD, undefined, #{off => 0, lim => ?rand(1000)}),
    ?testList([0], ?FORWARD, undefined, #{off => 0, lim => ?rand(1000)}),

    ?testList([], ?BACKWARD, undefined, #{off => 1, lim => ?rand(1000)}),
    ?testList([], ?FORWARD, undefined, #{off => 1, lim => ?rand(1000)}),

    ?testList([0], ?BACKWARD, undefined, #{off => -1, lim => ?rand(1000)}),
    ?testList([0], ?FORWARD, undefined, #{off => -1, lim => ?rand(1000)}),

    ?testList([0], ?BACKWARD, {id, 0}, #{off => 0, lim => ?rand(1000)}),
    ?testList([0], ?FORWARD, {id, 0}, #{off => 0, lim => ?rand(1000)}),
    ?testList([], ?BACKWARD, {id, 0}, #{off => 1, lim => ?rand(1000)}),
    ?testList([], ?FORWARD, {id, 0}, #{off => 1, lim => ?rand(1000)}),
    ?testList([0], ?BACKWARD, {id, 0}, #{off => -1, lim => ?rand(1000)}),
    ?testList([0], ?FORWARD, {id, 0}, #{off => -1, lim => ?rand(1000)}),

    ?testList([0], ?BACKWARD, {id, 4}, #{off => 0, lim => ?rand(1000)}),
    ?testList([], ?FORWARD, {id, 4}, #{off => 0, lim => ?rand(1000)}),
    ?testList([], ?BACKWARD, {id, 4}, #{off => 2, lim => ?rand(1000)}),
    ?testList([], ?FORWARD, {id, 4}, #{off => 2, lim => ?rand(1000)}),
    ?testList([0], ?BACKWARD, {id, 4}, #{off => -8, lim => ?rand(1000)}),
    ?testList([0], ?FORWARD, {id, 4}, #{off => -8, lim => ?rand(1000)}),

    ?testList([], ?BACKWARD, {id, -8}, #{off => 0, lim => ?rand(1000)}),
    ?testList([0], ?FORWARD, {id, -8}, #{off => 0, lim => ?rand(1000)}),
    ?testList([], ?BACKWARD, {id, -8}, #{off => 3, lim => ?rand(1000)}),
    ?testList([], ?FORWARD, {id, -8}, #{off => 3, lim => ?rand(1000)}),
    ?testList([0], ?BACKWARD, {id, -8}, #{off => -2, lim => ?rand(1000)}),
    ?testList([0], ?FORWARD, {id, -8}, #{off => -2, lim => ?rand(1000)}),

    ?testList([0], ?BACKWARD, {timestamp, 9999}, #{off => 0, lim => ?rand(1000)}),
    ?testList([0], ?FORWARD, {timestamp, 9999}, #{off => 0, lim => ?rand(1000)}),
    ?testList([], ?BACKWARD, {timestamp, 9999}, #{off => 56, lim => ?rand(1000)}),
    ?testList([], ?FORWARD, {timestamp, 9999}, #{off => 56, lim => ?rand(1000)}),
    ?testList([0], ?BACKWARD, {timestamp, 9999}, #{off => -100, lim => ?rand(1000)}),
    ?testList([0], ?FORWARD, {timestamp, 9999}, #{off => -100, lim => ?rand(1000)}),

    ?testList([0], ?BACKWARD, {timestamp, 15683}, #{off => 0, lim => ?rand(1000)}),
    ?testList([], ?FORWARD, {timestamp, 15683}, #{off => 0, lim => ?rand(1000)}),
    ?testList([], ?BACKWARD, {timestamp, 15683}, #{off => 9, lim => ?rand(1000)}),
    ?testList([], ?FORWARD, {timestamp, 15683}, #{off => 9, lim => ?rand(1000)}),
    ?testList([0], ?BACKWARD, {timestamp, 15683}, #{off => -895, lim => ?rand(1000)}),
    ?testList([0], ?FORWARD, {timestamp, 15683}, #{off => -895, lim => ?rand(1000)}),

    ?testList([], ?BACKWARD, {timestamp, 0}, #{off => 0, lim => ?rand(1000)}),
    ?testList([0], ?FORWARD, {timestamp, 0}, #{off => 0, lim => ?rand(1000)}),
    ?testList([], ?BACKWARD, {timestamp, 0}, #{off => 33, lim => ?rand(1000)}),
    ?testList([], ?FORWARD, {timestamp, 0}, #{off => 33, lim => ?rand(1000)}),
    ?testList([0], ?BACKWARD, {timestamp, 0}, #{off => -70, lim => ?rand(1000)}),
    ?testList([0], ?FORWARD, {timestamp, 0}, #{off => -70, lim => ?rand(1000)}).


append_with_time_warps(LogId, _) ->
    % in case of backward time warps, the infinite log should artificially
    % keep the entries monotonic - consecutive entry cannot be older than the previous
    clock_freezer_mock:set_current_time_millis(1000),
    infinite_log:append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(950),
    infinite_log:append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(953),
    infinite_log:append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(993),
    infinite_log:append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(1000),
    infinite_log:append(LogId, str_utils:rand_hex(100)),

    {ok, {done, ListResultsPrim}} = infinite_log:list(LogId, #{direction => ?FORWARD}),
    ?assertEqual(
        [1000, 1000, 1000, 1000, 1000],
        extract_timestamps(ListResultsPrim)
    ),

    % forward time warp should cause the new entries to get later timestamps
    clock_freezer_mock:set_current_time_millis(1300),
    infinite_log:append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(1306),
    infinite_log:append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(1296),
    infinite_log:append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(1296),
    infinite_log:append(LogId, str_utils:rand_hex(100)),

    clock_freezer_mock:set_current_time_millis(1318),
    infinite_log:append(LogId, str_utils:rand_hex(100)),

    {ok, {done, ListResultsBis}} = infinite_log:list(LogId, #{direction => ?FORWARD}),
    ?assertEqual(
        [1000, 1000, 1000, 1000, 1000, 1300, 1306, 1306, 1306, 1318],
        extract_timestamps(ListResultsBis)
    ).


append_too_large_content(LogId, MaxEntriesPerNode) ->
    TooLargeSize = 20000000 div MaxEntriesPerNode,
    % rand hex returns two output bytes for every input byte
    Content = str_utils:rand_hex(TooLargeSize div 2),
    ?assertEqual({error, log_content_too_large}, infinite_log:append(LogId, Content)).

%%=====================================================================
%% Helper functions
%%=====================================================================

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
        infinite_log:append(LogId, Content),
        store_entry_count(LogId, EntryNumber + 1),
        clock_freezer_mock:simulate_millis_passing(case Interval of
            random -> ?rand(10000);
            _ -> Interval
        end)
    end, lists:seq(1, EntryCount)).


list_ids_and_verify(Direction, StartFrom, OtherOpts) ->
    Offset = maps:get(off, OtherOpts, 0),
    Limit = maps:get(lim, OtherOpts, 1000),
    LogId = get_current_log_id(),
    {ok, {ProgressMarker, Batch}} = infinite_log:list(LogId, #{
        direction => Direction,
        start_from => StartFrom,
        offset => Offset,
        limit => Limit
    }),
    ListedIds = lists:map(fun({EntryId, {Timestamp, Content}}) ->
        ?assertEqual({Timestamp, Content}, get_entry(LogId, EntryId)),
        EntryId
    end, Batch),

    ExpProgressMarker = case length(ListedIds) of
        0 ->
            done;
        _ ->
            LastEntryId = case Direction of
                ?FORWARD -> get_entry_count(LogId) - 1;
                ?BACKWARD -> 0
            end,
            case lists:last(ListedIds) of
                LastEntryId -> done;
                _ -> more
            end
    end,
    ?assertEqual(ExpProgressMarker, ProgressMarker),

    ListedIds.


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


store_entry(Id, EntryId, Entry) ->
    node_cache:put({entry, Id, EntryId}, Entry).


get_entry(Id, EntryId) ->
    node_cache:get({entry, Id, EntryId}).


sentinel_exists(LogId) ->
    case infinite_log_persistence:get_record(LogId) of
        {ok, _} -> true;
        {error, not_found} -> false
    end.


node_exists(LogId, NodeNumber) ->
    NodeId = datastore_key:build_adjacent(integer_to_binary(NodeNumber), LogId),
    case infinite_log_persistence:get_record(NodeId) of
        {ok, _} -> true;
        {error, not_found} -> false
    end.


signum(X) when X < 0 -> -1;
signum(X) when X == 0 -> 0;
signum(X) when X > 0 -> 1.


-endif.
