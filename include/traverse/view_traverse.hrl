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

% This record is used to define the starting row for a query on the couchbase view.
% Each row in the view is created by calling the function emit(Key, Value) in a
% view function that is called for each document in the database.
% Each row is composed of:
%  * key - 1st argument passed to emit function
%  * value - 2nd argument passed to emit function
%  * id - id of a document for which the view function was called
% The row IS NOT uniquely identified by pair (key, id) as emit function may be called many times for one document.
%
% The best way to paginate the view
% (advised in Couchbase documentation: https://docs.couchbase.com/server/4.1/developer-guide/views-querying.html#pagination)
% is to pass to the query the following options:
%   * startkey_docid - id from last processed row
%   * startkey - key from last processed row
%   * skip = 1 - to skip last processed row because startkey_docid and startkey options are inclusive
-record(view_traverse_token, {
    % offset field is not used internally to query couchbase
    % it is only used to keep track of offset of processed rows in the view
    % and to pass row's number to callbacks defined in modules using view_traverse mechanism
    offset = 0 :: non_neg_integer(),
    % id field extracted from last processed row
    last_doc_id :: undefined | binary(),
    % key field extracted from last processed row
    last_start_key :: undefined | term()
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
    token = #view_traverse_token{} :: view_traverse:token(),
    query_opts :: view_traverse:query_opts(),
    async_next_batch_job = ?DEFAULT_ASYNC_NEXT_BATCH_JOB :: boolean(),
    info :: view_traverse:info()
}).

-record(view_traverse_slave, {
    view_processing_module :: view_traverse:view_processing_module(),
    info :: view_traverse:info(),
    row :: json_utils:json_term(),
    row_number :: non_neg_integer()
}).



-endif.