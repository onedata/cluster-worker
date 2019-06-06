%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model that allows listing of traverse tasks.
%%% @end
%%%-------------------------------------------------------------------
-module(traverse_task_list).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_links.hrl").

%% List API
-export([list/2, list/3, list_scheduled/3, list_scheduled/4, get_first_scheduled_link/3, list_ongoing_jobs/2]).
%% Modify API
-export([add_link/6, add_scheduled_link/7, add_job_link/3,
    delete_link/6, delete_scheduled_link/7, delete_job_link/3]).

-define(SCHEDULED_KEY(Pool), ?LINK_KEY(Pool, "SCHEDULED_")).
-define(ONGOING_KEY(Pool), ?LINK_KEY(Pool, "ONGOING_")).
-define(ENDED_KEY(Pool), ?LINK_KEY(Pool, "ENDED_")).
-define(LINK_KEY(Pool, Prefix), <<Prefix, Pool/binary>>).
-define(GROUP_KEY(Key, Group, Executor), <<Key/binary, "###", Group/binary, "###", Executor/binary>>).
-define(ONGOING_JOB_KEY(Pool, CallbackModule),
    <<Pool/binary, "###", (atom_to_binary(CallbackModule, utf8))/binary, "###ONGOING_JOBS">>).
-define(ONGOING_JOB_TREE, <<"ONGOING_TREE">>).

-define(LINK_NAME_ID_PART_LENGTH, 6).
-define(EPOCH_INFINITY, 9999999999). % GMT: Saturday, 20 November 2286 17:46:39

-type forest_key() :: datastore:key().
-type forest_type() :: scheduled | ongoing | ended.
-type tree() :: datastore_links:tree_id().
-type link_key() :: binary().
-type list_opts() :: #{
    offset => integer(),
    start_id => link_key(),
    prev_traverse => {tree(), link_key()},
    limit => non_neg_integer(),
    token => datastore_links_iter:token(),
    tree_id => tree(),
    sync_info => traverse:sync_info()
}.
-type restart_info() :: #{
    token => datastore_links_iter:token(),
    prev_traverse => {tree(), link_key()}
}.

-export_type([forest_type/0]).

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
%% Lists links of particular type.
%% @end
%%--------------------------------------------------------------------
-spec list(traverse:pool(), forest_type(), list_opts()) ->
    {ok, [traverse:id()], restart_info()} | {error, term()}.
list(Pool, Type, Opts) ->
    % TODO - filtrowanie po pozostalych drzewach??!! Tu czy na poziomie op_worker,
    % TODO - Dodac mozliwosc podawania ExtendedCtx
    Forest = forest_key(Pool, Type),
    list_internal(Forest, Opts).

%%--------------------------------------------------------------------
%% @doc
%% @equiv list_scheduled(Pool, GroupID, Executor, #{}).
%% @end
%%--------------------------------------------------------------------
-spec list_scheduled(traverse:pool(), traverse:group(), traverse:executor()) ->
    {ok, [traverse:id()], restart_info()} | {error, term()}.
list_scheduled(Pool, GroupID, Executor) ->
    list_scheduled(Pool, GroupID, Executor, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Lists links of scheduled transfers for group/executor pair.
%% @end
%%--------------------------------------------------------------------
-spec list_scheduled(traverse:pool(), traverse:group(), traverse:executor(),
    list_opts()) -> {ok, [traverse:id()], restart_info()} | {error, term()}.
list_scheduled(Pool, GroupID, Executor, Opts) ->
    BasicKey = forest_key(Pool, scheduled),
    Forest = ?GROUP_KEY(BasicKey, GroupID, Executor),
    list_internal(Forest, Opts).

%%--------------------------------------------------------------------
%% @doc
%% Gets first scheduled transfer for group/executor pair.
%% @end
%%--------------------------------------------------------------------
-spec get_first_scheduled_link(traverse:pool(), traverse:group(),
    traverse:executor()) -> {ok, traverse:id() | not_found}.
get_first_scheduled_link(Pool, GroupID, Executor) ->
    BasicKey = forest_key(Pool, scheduled),
    datastore_model:fold_links(traverse_task:get_ctx(),
        ?GROUP_KEY(BasicKey, GroupID, Executor), all,
        fun(#link{target = Target}, _) ->
            {stop, Target} end, not_found, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Gets list of ongoing jobs.
%% @end
%%--------------------------------------------------------------------
-spec list_ongoing_jobs(traverse:pool(), traverse:callback_module()) -> {ok, [traverse:id()]}.
% TODO - use batches
list_ongoing_jobs(Pool, CallbackModule) ->
    Ctx = traverse_task:get_ctx(),
    datastore_model:fold_links(Ctx#{local_links_tree_id => ?ONGOING_JOB_TREE, routing => local},
        ?ONGOING_JOB_KEY(Pool, CallbackModule), ?ONGOING_JOB_TREE,
        fun(#link{target = Target}, Acc) -> 
            {ok, [Target | Acc]} 
        end, [], #{}).

%%%===================================================================
%%% Modify API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Adds link to main tree.
%% @end
%%--------------------------------------------------------------------
-spec add_link(traverse_task:ctx(), traverse:pool(), forest_type(),
    tree(), traverse:id(), traverse:timestamp()) -> ok.
add_link(Ctx, Pool, Type, Tree, ID, Timestamp) ->
    add_link(Ctx, forest_key(Pool, Type), Tree, ID, Timestamp).

%%--------------------------------------------------------------------
%% @doc
%% Adds link to main and group/executor scheduled trees.
%% @end
%%--------------------------------------------------------------------
-spec add_scheduled_link(traverse_task:ctx(), traverse:pool(), tree(),
    traverse:id(), traverse:timestamp(), traverse:group(), traverse:executor()) -> ok.
add_scheduled_link(Ctx, Pool, Tree, ID, Timestamp, GroupID, Executor) ->
    run_on_trees(forest_key(Pool, scheduled), GroupID, Executor, fun(Key) ->
        add_link(Ctx, Key, Tree, ID, Timestamp)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Adds link to jobs tree.
%% @end
%%--------------------------------------------------------------------
-spec add_job_link(traverse:pool(), traverse:callback_module(), traverse:job_id()) -> ok.
add_job_link(Pool, CallbackModule, JobID) ->
    Ctx = traverse_task:get_ctx(),
    case datastore_model:add_links(Ctx#{local_links_tree_id => ?ONGOING_JOB_TREE, routing => local},
        ?ONGOING_JOB_KEY(Pool, CallbackModule), ?ONGOING_JOB_TREE, [{JobID, JobID}]) of
        [{ok, _}] -> ok;
        [{error,already_exists}] -> ok % in case of restart
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Deletes link from main tree.
%% @end
%%--------------------------------------------------------------------
-spec delete_link(traverse_task:ctx(), traverse:pool(), forest_type(),
    tree(), traverse:id(), traverse:timestamp()) -> ok.
delete_link(Ctx, Pool, Type, Tree, ID, Timestamp) ->
    delete_link(Ctx, forest_key(Pool, Type), Tree, ID, Timestamp).

%%--------------------------------------------------------------------
%% @doc
%% Deletes link from main and group/executor scheduled trees.
%% @end
%%--------------------------------------------------------------------
-spec delete_scheduled_link(traverse_task:ctx(), traverse:pool(), tree(),
    traverse:id(), traverse:timestamp(), traverse:group(), traverse:executor()) -> ok.
delete_scheduled_link(Ctx, Pool, Tree, ID, Timestamp, GroupID, Executor) ->
    run_on_trees(forest_key(Pool, scheduled), GroupID, Executor, fun(Key) ->
        delete_link(Ctx, Key, Tree, ID, Timestamp)
    end).

%%--------------------------------------------------------------------
%% @doc
%% Deletes link from jobs tree.
%% @end
%%--------------------------------------------------------------------
-spec delete_job_link(traverse:pool(), traverse:callback_module(), traverse:job_id()) -> ok.
delete_job_link(Pool, CallbackModule, JobID) ->
    Ctx = traverse_task:get_ctx(),
    [ok] = datastore_model:delete_links(Ctx#{local_links_tree_id => ?ONGOING_JOB_TREE, routing => local},
        ?ONGOING_JOB_KEY(Pool, CallbackModule), ?ONGOING_JOB_TREE, [JobID]),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec add_link(traverse_task:ctx(), forest_key(), tree(), 
    traverse:id(), traverse:timestamp()) -> ok.
add_link(Ctx, Key, Tree, ID, Timestamp) ->
    [{ok, _}] = datastore_model:add_links(Ctx,
        Key, Tree, [{link_key(ID, Timestamp), ID}]),
    ok.

-spec delete_link(traverse_task:ctx(), forest_key(), tree(),
    traverse:id(), traverse:timestamp()) -> ok.
delete_link(Ctx, Key, Tree, ID, Timestamp) ->
    [ok] = datastore_model:delete_links(Ctx,
        Key, Tree, [link_key(ID, Timestamp)]),
    ok.

-spec link_key(traverse:id(), traverse:timestamp()) -> link_key().
link_key(ID, Timestamp) ->
    TimestampPart = (integer_to_binary(?EPOCH_INFINITY - Timestamp)),
    Length = min(byte_size(ID), ?LINK_NAME_ID_PART_LENGTH),
    IdPart = binary:part(ID, 0, Length),
    <<TimestampPart/binary, IdPart/binary>>.

-spec forest_key(traverse:pool(), forest_type()) -> forest_key().
forest_key(Pool, scheduled) ->
    ?SCHEDULED_KEY(Pool);
forest_key(Pool, ongoing) ->
    ?ONGOING_KEY(Pool);
forest_key(Pool, ended) ->
    ?ENDED_KEY(Pool).

-spec list_internal(forest_key(), list_opts()) -> {ok, [traverse:id()], restart_info()} | {error, term()}.
list_internal(Forest, Opts) ->
    Ctx0 = traverse_task:get_ctx(),
    Ctx = maps:merge(Ctx0, maps:get(sync_info, Opts, #{})),
    ListOpts = #{offset => maps:get(offset, Opts, 0)},
    ListOpts2 = case {maps:get(start_id, Opts, undefined),
        maps:get(prev_traverse, Opts, undefined)} of
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
        {{ok, Links}, Token2} ->
            prepare_list_ans(Links, #{token => Token2});
        {ok, Links} ->
            prepare_list_ans(Links, #{});
        {error, Reason} ->
            {error, Reason}
    end.

-spec prepare_list_ans([{traverse:id(), tree()}], restart_info()) ->
    {ok, [traverse:id()], restart_info()}.
prepare_list_ans([], Info) ->
    {ok, [], Info};
prepare_list_ans([{LastTarget, LastTree} | _] = Links, Info) ->
    Links2 = lists:map(fun({Target, _}) -> Target end, lists:reverse(Links)),
    {ok, Links2, Info#{prev_traverse => {LastTarget, LastTree}}}.


-spec run_on_trees(forest_key(), traverse:group(), traverse:executor(),
    fun((forest_key()) -> ok)) -> ok.
run_on_trees(Key, Group, Executor, Fun) ->
    Fun(Key),
    Fun(?GROUP_KEY(Key, Group, Executor)),
    ok.