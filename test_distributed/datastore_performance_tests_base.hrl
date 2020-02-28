%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Definitions of datastore performance tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(STRESS_PERFORMANCE_TEST_BASE_HRL).
-define(STRESS_PERFORMANCE_TEST_BASE_HRL, 1).

-define(MAIN_TEST(MEMORY_ONLY, HA, REPEATS, CHECK_NEXT_WORKER),
    {repeats, REPEATS},
    {success_rate, 100},
    {parameters, [
        [{name, proc_num}, {value, 200}, {description, "Number of threads used during the test."}],
        [{name, proc_repeats}, {value, 50}, {description, "Number of operations done by single threads."}],
        [{name, many_keys}, {value, true}, {description, "Generate new key for each request"}],
        [{name, ha}, {value, HA}, {description, "HA settings"}],
        [{name, ha_mode}, {value, cast}, {description, "HA settings"}],
        [{name, memory_only}, {value, MEMORY_ONLY}, {description, "Use memory only model"}],
        [{name, check_next_worker}, {value, CHECK_NEXT_WORKER}, {description, "Check if copy exists on next worker node"}]
    ]},
    {description, "Performs many datastore calls"}
).

-define(TEST_CONFIGS(HA, HA_MODE),
    {config, [{name, one_proc},
        {parameters, [
            [{name, proc_num}, {value, 1}],
            [{name, proc_repeats}, {value, 50000}],
            [{name, many_keys}, {value, true}],
            [{name, ha}, {value, HA}],
            [{name, ha_mode}, {value, HA_MODE}]
        ]},
        {description, "One thread does many operations"}
    ]},
    {config, [{name, few_procs},
        {parameters, [
            [{name, proc_num}, {value, 20}],
            [{name, proc_repeats}, {value, 5000}],
            [{name, many_keys}, {value, true}],
            [{name, ha}, {value, HA}],
            [{name, ha_mode}, {value, HA_MODE}]
        ]},
        {description, "Multiple threads, each thread does only one operation of each type"}
    ]},
    {config, [{name, many_procs},
        {parameters, [
            [{name, proc_num}, {value, 500}],
            [{name, proc_repeats}, {value, 200}],
            [{name, many_keys}, {value, true}],
            [{name, ha}, {value, HA}],
            [{name, ha_mode}, {value, HA_MODE}]
        ]},
        {description, "Many threads do many operations"}
    ]},
    {config, [{name, one_proc_one_key},
        {parameters, [
            [{name, proc_num}, {value, 1}],
            [{name, proc_repeats}, {value, 50000}],
            [{name, many_keys}, {value, false}],
            [{name, ha}, {value, HA}],
            [{name, ha_mode}, {value, HA_MODE}]
        ]},
        {description, "One thread does many operations"}
    ]},
    {config, [{name, few_procs_one_key},
        {parameters, [
            [{name, proc_num}, {value, 20}],
            [{name, proc_repeats}, {value, 5000}],
            [{name, many_keys}, {value, false}],
            [{name, ha}, {value, HA}],
            [{name, ha_mode}, {value, HA_MODE}]
        ]},
        {description, "Multiple threads, each thread does only one operation of each type"}
    ]},
    {config, [{name, many_procs_one_key},
        {parameters, [
            [{name, proc_num}, {value, 500}],
            [{name, proc_repeats}, {value, 200}],
            [{name, many_keys}, {value, false}],
            [{name, ha}, {value, HA}],
            [{name, ha_mode}, {value, HA_MODE}]
        ]},
        {description, "Many threads do many operations"}
    ]}
).

-define(SINGLENODE_TEST(MEMORY_ONLY, REPEATS),
    ?MAIN_TEST(MEMORY_ONLY, false, REPEATS, false),
    ?TEST_CONFIGS(false, cast)
).

-define(MULTINODE_TEST(MEMORY_ONLY, REPEATS),
    ?MAIN_TEST(MEMORY_ONLY, true, REPEATS, false),
    ?TEST_CONFIGS(false, cast),
    ?TEST_CONFIGS(true, cast),
    ?TEST_CONFIGS(true, call)
).

-define(MULTINODE_WITH_CHECK_TEST(MEMORY_ONLY, REPEATS),
    ?MAIN_TEST(MEMORY_ONLY, true, REPEATS, true),
    {config, [{name, many_procs_cast},
        {parameters, [
            [{name, proc_num}, {value, 500}],
            [{name, proc_repeats}, {value, 20}],
            [{name, many_keys}, {value, true}],
            [{name, ha}, {value, true}],
            [{name, ha_mode}, {value, cast}]
        ]},
        {description, "Many threads do many operations"}
    ]},
    {config, [{name, many_procs_call},
        {parameters, [
            [{name, proc_num}, {value, 500}],
            [{name, proc_repeats}, {value, 20}],
            [{name, many_keys}, {value, true}],
            [{name, ha}, {value, true}],
            [{name, ha_mode}, {value, call}]
        ]},
        {description, "Many threads do many operations"}
    ]}
).

-endif.
