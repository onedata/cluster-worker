%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file defines macros used during basic datastore tests.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(DATASTORE_BASIC_OPS_UTILS_HRL).
-define(DATASTORE_BASIC_OPS_UTILS_HRL, 1).

-include("datastore_test_models_def.hrl").

-define(basic_test_def(Desc, Repeats),
    [
        {repeats, Repeats},
        {success_rate, 100},
        {parameters, [
            [{name, threads_num}, {value, 20}, {description, "Number of threads used during the test."}],
            [{name, docs_per_thead}, {value, 3}, {description, "Number of documents used by single threads."}],
            [{name, ops_per_doc}, {value, 5}, {description, "Number of oprerations on each document."}],
            [{name, conflicted_threads}, {value, 10}, {description, "Number of threads that work with the same documents set."}]
        ]},
        {description, Desc},
        {config, [{name, single_short_thread},
            {description, "Test config that uses single thread that does only few operations on few docs"},
            {repeats, 4 * Repeats},
            {success_rate, 95},
            {parameters, [
                [{name, threads_num}, {value, 1}],
                [{name, docs_per_thead}, {value, 5}],
                [{name, conflicted_threads}, {value, 1}]
            ]}
        ]},
        {config, [{name, single_long_thread},
            {description, "Test config that uses single thread that does many operations on multiple docs"},
            {parameters, [
                [{name, threads_num}, {value, 1}],
                [{name, docs_per_thead}, {value, 60}],
                [{name, conflicted_threads}, {value, 1}]
            ]}
        ]},
        {config, [{name, single_long_thread_one_op_per_doc},
            {description, "Test config that uses single thread that does only one operation on each doc (multiple docs used)"},
            {parameters, [
                [{name, threads_num}, {value, 1}],
                [{name, docs_per_thead}, {value, 300}],
                [{name, ops_per_doc}, {value, 1}],
                [{name, conflicted_threads}, {value, 1}]
            ]}
        ]},
        {config, [{name, multiple_threads_no_conflicts},
            {description, "Test config that uses many threads that do only one operation on each doc (multiple docs used)"},
            {parameters, [
                [{name, threads_num}, {value, 60}],
                [{name, ops_per_doc}, {value, 1}],
                [{name, conflicted_threads}, {value, 1}]
            ]}
        ]},
        {config, [{name, multiple_threads_with_repeats},
            {description, "Test config that uses many threads that do many operations on multiple docs (no conflicts between threads)"},
            {parameters, [
                [{name, conflicted_threads}, {value, 1}]
            ]}
        ]},
        {config, [{name, multiple_threads_with_conflits},
            {description, "Test config that uses many threads that do many operations on multiple docs (with conflicts between threads)"},
            {parameters, [
                [{name, conflicted_threads}, {value, 20}]
            ]}
        ]}
    ]
).

-define(long_test_def,
    [
        {repeats, 30},
        {success_rate, 95},
        {parameters, [
            [{name, threads_num}, {value, 20}, {description, "Number of threads used during the test."}],
            [{name, docs_per_thead}, {value, 3}, {description, "Number of documents used by single threads."}],
            [{name, ops_per_doc}, {value, 5}, {description, "Number of oprerations on each document."}],
            [{name, conflicted_threads}, {value, 10}, {description, "Number of threads that work with the same documents set."}]
        ]},
        {description, "Performs multipe datastore operations using many threads."},
        {config, [{name, multiple_threads_with_conflits},
            {description, "Test config that uses many threads that do many operations on multiple docs (with conflicts between threads)"},
            {parameters, [
                [{name, threads_num}, {value, 40}],
                [{name, conflicted_threads}, {value, 20}]
            ]}
        ]}
    ]
).

-define(create_delete_test_def, ?create_delete_test_def(5)).

-define(save_test_def, ?save_test_def(5)).

-define(update_test_def, ?update_test_def(5)).

-define(create_sync_delete_test_def, ?create_sync_delete_test_def(5)).

-define(save_sync_test_def, ?save_sync_test_def(5)).

-define(update_sync_test_def, ?update_sync_test_def(5)).

-define(no_transactions_create_delete_test_def, ?no_transactions_create_delete_test_def(5)).

-define(no_transactions_save_test_def, ?no_transactions_save_test_def(5)).

-define(no_transactions_update_test_def, ?no_transactions_update_test_def(5)).

-define(get_test_def, ?get_test_def(5)).

-define(exists_test_def, ?exists_test_def(5)).

-define(links_test_def, ?links_test_def(5)).


-define(create_delete_test_def(Repeats),
    ?basic_test_def("Performs multipe create/delete operations using many threads.", Repeats)
).

-define(save_test_def(Repeats),
    ?basic_test_def("Performs multipe save operations using many threads. "
    "Document may be saved many times.", Repeats)
).

-define(update_test_def(Repeats),
    ?basic_test_def("Performs multipe update operations using many threads. "
    "Document may be updated many times.", Repeats)
).

-define(create_sync_delete_test_def(Repeats),
    ?basic_test_def("Performs multipe create_sync/delete_sync operations using many threads.", Repeats)
).

-define(save_sync_test_def(Repeats),
    ?basic_test_def("Performs multipe save_sync operations using many threads. "
    "Document may be saved many times.", Repeats)
).

-define(update_sync_test_def(Repeats),
    ?basic_test_def("Performs multipe update_sync operations using many threads. "
    "Document may be updated many times.", Repeats)
).

-define(no_transactions_create_delete_test_def(Repeats),
    ?basic_test_def("Performs multipe non-transactional create/delete operations using many threads.", Repeats)
).

-define(no_transactions_save_test_def(Repeats),
    ?basic_test_def("Performs multipe non-transactional save operations using many threads. "
    "Document may be saved many times.", Repeats)
).

-define(no_transactions_update_test_def(Repeats),
    ?basic_test_def("Performs multipe non-transactional update operations using many threads. "
    "Document may be updated many times.", Repeats)
).

-define(get_test_def(Repeats),
    ?basic_test_def("Performs multipe get operations using many threads.", Repeats)
).

-define(exists_test_def(Repeats),
    ?basic_test_def("Performs multipe exists operations using many threads.", Repeats)
).

-define(links_test_def(Repeats),
    ?basic_test_def("Performs multipe operations on links using many threads.", Repeats)
).

-endif.