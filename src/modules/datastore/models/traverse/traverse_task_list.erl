%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module that allows listing of traverse tasks and jobs (see traverse.erl) using links.
%%% The tasks are sorted basing on timestamps provided via callback module or generated automatically.
%%% Different link forests are used for scheduled, ongoing and ended tasks.
%%% Only task creator can modify scheduled links while task executor ongoing and ended links.
%%% Additional link forests are created for scheduled tasks for load balancing purposes (each executor uses multiple
%%% queues for different groups - see traverse_tasks_scheduler.erl). Such forests gather tasks of particular pool,
%%% to be executed on particular environment and belonging to particular group.
%%% While task links can be viewed by on different environments, job links are local for each environment (they
%%% are used during environment restart).
%%% The links are synchronized between environments similarly to travers tasks (see travers_task.erl).
%%% @end
%%% TODO - VFS-5528 - Extend listing filters.
%%% TODO - VFS-5529 - Allow use of different timestamps in different types of trees.
%%% TODO - VFS-5533 - Task listing functions containing forest type (and information that it is tasks listing)
%%% TODO - VFS-5534 - Use batches during jobs listing
%%%-------------------------------------------------------------------
-module(traverse_task_list).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_links.hrl").

%% List API
-export([list/2, list/3, list_scheduled/3, list_scheduled/4, get_first_scheduled_link/3, list_local_jobs/2]).
%% Modify API
-export([add_link/6, add_scheduled_link/6, add_job_link/3,
    delete_link/6, delete_scheduled_link/6, delete_job_link/3]).

% Forests for scheduled, ongoing and ended tasks
-define(SCHEDULED_FOREST_KEY(Pool), ?FOREST_KEY(Pool, "SCHEDULED_")).
-define(ONGOING_FOREST_KEY(Pool), ?FOREST_KEY(Pool, "ONGOING_")).
-define(ENDED_FOREST_KEY(Pool), ?FOREST_KEY(Pool, "ENDED_")).
-define(FOREST_KEY(Pool, Prefix), <<Prefix, Pool/binary>>).
% Additional forests for load balancing purposes
-define(LOAD_BALANCING_FOREST_KEY(ScheduledForestKey, Group, EnvironmentID),
    <<ScheduledForestKey/binary, "###", Group/binary, "###", EnvironmentID/binary>>).
% Definitions used to list ongoing jobs (used during provider restart)
-define(JOB_KEY(Pool, CallbackModule),
    <<Pool/binary, "###", (atom_to_binary(CallbackModule, utf8))/binary, "###JOBS">>).
-define(JOB_TREE, <<"JOB_TREE">>).
% Other definitions
-define(LINK_NAME_ID_PART_LENGTH, 6).
-define(EPOCH_INFINITY, 9999999999). % GMT: Saturday, 20 November 2286 17:46:39

-type forest_key() :: datastore:key().
-type forest_type() :: scheduled | ongoing | ended.
-type tree() :: datastore_links:tree_id().
-type link_key() :: binary().
-type list_opts() :: #{
    % Basic list start options (use only one):
    token => datastore_links_iter:token(), % Use tokens to list faster
    start_id => link_key(), % List from particular id
    prev_traverse => {tree(), link_key()}, % Start in place where last listing finished
    % Additional list start option (can be used with ones above)
    offset => integer(),
    % Other list options
    limit => non_neg_integer(),
    tree_id => tree(),
    % Option used to sync data between many environments
    sync_info => traverse:sync_info()
}.
-type restart_info() :: #{
    token => datastore_links_iter:token(),
    prev_traverse => {tree(), link_key()}
}.

-export_type([forest_type/0, tree/0]).

%%%===================================================================
%%% List API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv list(Pool, Type, #{}).
%% @end
%%--------------------------------------------------------------------
-spec list(traverse:pool(), forest_type()) ->
    {ok, [traverse:id()], restart_info()} | {error, term()}.
list(Pool, Type) ->
    list(Pool, Type, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Lists tasks connected with particular forest.
%% @end
%%--------------------------------------------------------------------
-spec list(traverse:pool(), forest_type(), list_opts()) ->
    {ok, [traverse:id()], restart_info()} | {error, term()}.
list(Pool, Type, Opts) ->
    Forest = forest_key(Pool, Type),
    list_internal(Forest, Opts).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_scheduled(Pool, GroupID, EnvironmentID, #{}).
%% @end
%%--------------------------------------------------------------------
-spec list_scheduled(traverse:pool(), traverse:group(), traverse:environment_id()) ->
    {ok, [traverse:id()], restart_info()} | {error, term()}.
list_scheduled(Pool, GroupID, EnvironmentID) ->
    list_scheduled(Pool, GroupID, EnvironmentID, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Lists links of scheduled transfers for group/environment_id pair.
%% @end
%%--------------------------------------------------------------------
-spec list_scheduled(traverse:pool(), traverse:group(), traverse:environment_id(),
    list_opts()) -> {ok, [traverse:id()], restart_info()} | {error, term()}.
list_scheduled(Pool, GroupID, EnvironmentID, Opts) ->
    ForestKey = forest_key(Pool, scheduled),
    GroupForestKey = ?LOAD_BALANCING_FOREST_KEY(ForestKey, GroupID, EnvironmentID),
    list_internal(GroupForestKey, Opts).

%%--------------------------------------------------------------------
%% @doc
%% Gets first scheduled transfer for group/environment_id pair.
%% @end
%%--------------------------------------------------------------------
-spec get_first_scheduled_link(traverse:pool(), traverse:group(),
    traverse:environment_id()) -> {ok, traverse:id() | not_found}.
get_first_scheduled_link(Pool, GroupID, EnvironmentID) ->
    BasicKey = forest_key(Pool, scheduled),
    datastore_model:fold_links(
        traverse_task:get_ctx(),
        ?LOAD_BALANCING_FOREST_KEY(BasicKey, GroupID, EnvironmentID),
        all,
        fun(#link{target = Target}, _) -> {stop, Target} end,
        not_found,
        #{}
    ).

%%--------------------------------------------------------------------
%% @doc
%% Gets list of ongoing jobs.
%% @end
%%--------------------------------------------------------------------
-spec list_local_jobs(traverse:pool(), traverse:callback_module()) -> {ok, [traverse:id()]}.
% TODO VFS-5528 - use batches
list_local_jobs(Pool, CallbackModule) ->
    Ctx = traverse_task:get_ctx(),
    datastore_model:fold_links(
        Ctx#{local_links_tree_id => ?JOB_TREE, routing => local},
        ?JOB_KEY(Pool, CallbackModule),
        ?JOB_TREE,
        fun(#link{target = Target}, Acc) -> {ok, [Target | Acc]} end,
        [],
        #{}
    ).

%%%===================================================================
%%% Modify API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds link to tree of tasks.
%% @end
%%--------------------------------------------------------------------
-spec add_link(traverse_task:ctx(), traverse:pool(), forest_type(),
    tree(), traverse:id(), traverse:timestamp()) -> ok.
add_link(Ctx, Pool, Type, Tree, ID, Timestamp) ->
    add_link_with_timestamp(Ctx, forest_key(Pool, Type), Tree, ID, Timestamp).

%%--------------------------------------------------------------------
%% @doc
%% Adds link to main and group/environment_id scheduled trees of tasks.
%% @end
%%--------------------------------------------------------------------
-spec add_scheduled_link(traverse:pool(), tree(), traverse:id(), traverse:timestamp(), traverse:group(),
    traverse:environment_id()) -> ok.
add_scheduled_link(Pool, Tree, ID, Timestamp, GroupID, EnvironmentID) ->
    BasicKey = forest_key(Pool, scheduled),
    add_link_with_timestamp(traverse_task:get_ctx(),
        ?LOAD_BALANCING_FOREST_KEY(BasicKey, GroupID, EnvironmentID), Tree, ID, Timestamp).

%%--------------------------------------------------------------------
%% @doc
%% Adds link to jobs tree.
%% @end
%%--------------------------------------------------------------------
-spec add_job_link(traverse:pool(), traverse:callback_module(), traverse:job_id()) -> ok.
add_job_link(Pool, CallbackModule, JobID) ->
    Ctx = traverse_task:get_ctx(),
    case datastore_model:add_links(
        Ctx#{local_links_tree_id => ?JOB_TREE, routing => local},
        ?JOB_KEY(Pool, CallbackModule),
        ?JOB_TREE,
        [{JobID, JobID}]
    ) of
        [{ok, _}] -> ok;
        [{error,already_exists}] -> ok % in case of restart
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Deletes link from main tree of tasks.
%% @end
%%--------------------------------------------------------------------
-spec delete_link(traverse_task:ctx(), traverse:pool(), forest_type(),
    tree(), traverse:id(), traverse:timestamp()) -> ok.
delete_link(Ctx, Pool, Type, Tree, ID, Timestamp) ->
    delete_link_with_timestamp(Ctx, forest_key(Pool, Type), Tree, ID, Timestamp).

%%--------------------------------------------------------------------
%% @doc
%% Deletes link from main and group/environment_id scheduled trees of tasks.
%% @end
%%--------------------------------------------------------------------
-spec delete_scheduled_link(traverse:pool(), tree(), traverse:id(), traverse:timestamp(), traverse:group(),
    traverse:environment_id()) -> ok.
delete_scheduled_link(Pool, Tree, ID, Timestamp, GroupID, EnvironmentID) ->
    BasicKey = forest_key(Pool, scheduled),
    delete_link_with_timestamp(traverse_task:get_ctx(),
        ?LOAD_BALANCING_FOREST_KEY(BasicKey, GroupID, EnvironmentID), Tree, ID, Timestamp).

%%--------------------------------------------------------------------
%% @doc
%% Deletes link from jobs tree.
%% @end
%%--------------------------------------------------------------------
-spec delete_job_link(traverse:pool(), traverse:callback_module(), traverse:job_id()) -> ok.
delete_job_link(Pool, CallbackModule, JobID) ->
    Ctx = traverse_task:get_ctx(),
    [ok] = datastore_model:delete_links(
        Ctx#{local_links_tree_id => ?JOB_TREE, routing => local},
        ?JOB_KEY(Pool, CallbackModule),
        ?JOB_TREE,
        [JobID]
    ),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec add_link_with_timestamp(traverse_task:ctx(), forest_key(), tree(),
    traverse:id(), traverse:timestamp()) -> ok.
add_link_with_timestamp(Ctx, Key, Tree, ID, Timestamp) ->
    [{ok, _}] = datastore_model:add_links(Ctx,
        Key, Tree, [{link_key(ID, Timestamp), ID}]),
    ok.

-spec delete_link_with_timestamp(traverse_task:ctx(), forest_key(), tree(),
    traverse:id(), traverse:timestamp()) -> ok.
delete_link_with_timestamp(Ctx, Key, Tree, ID, Timestamp) ->
    [ok] = datastore_model:delete_links(Ctx,
        Key, Tree, [link_key(ID, Timestamp)]),
    ok.

-spec link_key(traverse:id(), traverse:timestamp()) -> link_key().
link_key(ID0, Timestamp) ->
    ID = consistent_hashing:get_random_labal_part(ID0),
    TimestampPart = (integer_to_binary(?EPOCH_INFINITY - Timestamp)),
    Length = min(byte_size(ID), ?LINK_NAME_ID_PART_LENGTH),
    IdPart = binary:part(ID, 0, Length),
    <<TimestampPart/binary, IdPart/binary>>.

-spec forest_key(traverse:pool(), forest_type()) -> forest_key().
forest_key(Pool, scheduled) ->
    ?SCHEDULED_FOREST_KEY(Pool);
forest_key(Pool, ongoing) ->
    ?ONGOING_FOREST_KEY(Pool);
forest_key(Pool, ended) ->
    ?ENDED_FOREST_KEY(Pool).

-spec list_internal(forest_key(), list_opts()) -> {ok, [traverse:id()], restart_info()} | {error, term()}.
list_internal(Forest, Opts) ->
    Ctx0 = traverse_task:get_ctx(),
    Ctx = maps:merge(Ctx0, maps:get(sync_info, Opts, #{})),
    ListOpts = #{offset => maps:get(offset, Opts, 0)},
    ListOpts2 = case {maps:get(start_id, Opts, undefined), maps:get(prev_traverse, Opts, undefined)} of
        {undefined, undefined} -> ListOpts;
        {StartId, undefined} -> ListOpts#{prev_link_name => StartId};
        {_, {PrevID, PrevTree}} -> ListOpts#{prev_link_name => PrevID, prev_tree_id => PrevTree}
    end,

    ListOpts3 = case maps:get(limit, Opts, undefined) of
        undefined -> ListOpts2;
        Limit -> ListOpts2#{size => Limit}
    end,

    ListOpts4 = case maps:get(token, Opts, undefined) of
        undefined -> ListOpts3;
        Token -> ListOpts3#{token => Token}
    end,

    Tree = maps:get(tree_id, Opts, all),
    Result = datastore_model:fold_links(Ctx, Forest, Tree, fun
        (#link{target = Target, tree_id = TargetTree}, Acc) -> {ok, [{Target, TargetTree} | Acc]}
    end, [], ListOpts4),

    case Result of
        {{ok, Links}, Token2} -> prepare_list_ans(Links, #{token => Token2});
        {ok, Links} -> prepare_list_ans(Links, #{});
        {error, Reason} -> {error, Reason}
    end.

-spec prepare_list_ans([{traverse:id(), tree()}], restart_info()) ->
    {ok, [traverse:id()], restart_info()}.
prepare_list_ans([], Info) ->
    {ok, [], Info};
prepare_list_ans([{LastTarget, LastTree} | _] = Links, Info) ->
    Links2 = lists:map(fun({Target, _}) -> Target end, lists:reverse(Links)),
    {ok, Links2, Info#{prev_traverse => {LastTarget, LastTree}}}.