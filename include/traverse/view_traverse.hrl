%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros and records used by
%%% view_traverse mechanism.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(VIEW_TRAVERSE_HRL).
-define(VIEW_TRAVERSE_HRL, 1).

-include("global_definitions.hrl").

% this record is used to define the starting row for a query on the couchbase view
-record(query_view_token, {
    % doc_id of the last returned row
    % if defined it will be used with start_key to start the query
    % from the previously finished row
    last_doc_id :: undefined | binary(),
    % start_key, it is updated with the key of the last returned row
    % it is used (with last_doc_id) to start the query
    % from the previously finished row
    start_key :: undefined | term()
}).

% default pool opts
-define(DEFAULT_MASTER_JOBS_LIMIT,
    application:get_env(?CLUSTER_WORKER_APP_NAME, view_traverse_default_master_jobs_limit, 10)).
-define(DEFAULT_SLAVE_JOBS_LIMIT,
    application:get_env(?CLUSTER_WORKER_APP_NAME, view_traverse_default_slave_jobs_limit, 20)).
-define(DEFAULT_PARALLELISM_LIMIT,
    application:get_env(?CLUSTER_WORKER_APP_NAME, view_traverse_default_parallelism_limit, 5)).

% default run opts
-define(DEFAULT_QUERY_BATCH, 1000).
-define(DEFAULT_ASYNC_NEXT_BATCH_JOB, false).
-define(DEFAULT_QUERY_OPTS, #{
    stale => false,
    limit => ?DEFAULT_QUERY_BATCH
}).

-record(view_traverse_master, {
    view_name :: couchbase_driver:view(),
    view_processing_module :: view_traverse:view_processing_module(),
    query_view_token = #query_view_token{} :: view_traverse:token(),
    query_opts :: view_traverse:query_opts(),
    async_next_batch_job = ?DEFAULT_ASYNC_NEXT_BATCH_JOB :: boolean(),
    info :: view_traverse:info(),
    offset = 0 :: non_neg_integer()
}).

-record(view_traverse_slave, {
    view_processing_module :: view_traverse:view_processing_module(),
    info :: view_traverse:info(),
    row :: json_utils:json_term(),
    row_number :: non_neg_integer()
}).



-endif.