%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model that holds information about traverse tasks load balancing (see traverse.erl).
%%% Single traverse_tasks_scheduler document is created for all instances of pool working on different nodes of cluster
%%% (each pool is balanced separately). The model is not synchronized between environments.
%%% Documents contain information needed to control number of tasks executed in parallel on single pool on all nodes
%%% and to choose next task to be executed on that pool. The tasks are started immediately until parallel
%%% tasks limit is reached for particular pool. Next tasks are started when an ongoing tasks are ended.
%%% Task to be executed is chosen using groups. Choosing task, the group is chosen first (using round robin)
%%% and then first (according to timestamp - see traverse_task_list) task from the group is processed.
%%% Nodes for tasks are chosen using round robin.
%%% @end
%%%-------------------------------------------------------------------
-module(traverse_tasks_scheduler).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%%% Pool management API
-export([init/3, clear/1]).
%%% Task management API
-export([increment_ongoing_tasks_and_choose_node/1, decrement_ongoing_tasks/1, change_node_ongoing_tasks/3]).
%%% Group management API
-export([register_group/2, deregister_group/2, get_next_group/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type ctx() :: datastore:ctx().
-type ongoing_tasks_map() :: #{node() => non_neg_integer()}.
-type ongoing_tasks_limit() :: non_neg_integer().

-export_type([ongoing_tasks_map/0, ongoing_tasks_limit/0]).

-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% Pool management API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes new document for pool. If document exists - updates it.
%% @end
%%--------------------------------------------------------------------
-spec init(traverse:pool(), ongoing_tasks_limit(), ongoing_tasks_limit()) -> ok | {error, term()}.
init(Pool, Limit, NodeLimit) ->
    Node = node(),
    Default = #traverse_tasks_scheduler{
        pool = Pool,
        ongoing_tasks_limit = Limit,
        ongoing_tasks_per_node_limit = NodeLimit,
        nodes = [Node]},
    Diff = fun(#traverse_tasks_scheduler{nodes = Nodes} = Record) ->
        {ok, Record#traverse_tasks_scheduler{
            nodes = [Node | (Nodes -- [Node])],
            ongoing_tasks_limit = Limit,
            ongoing_tasks_per_node_limit = NodeLimit
        }}
    end,
    extract_ok(datastore_model:update(?CTX, Pool, Diff, Default)).

%%--------------------------------------------------------------------
%% @doc
%% Deletes document for pool.
%% @end
%%--------------------------------------------------------------------
-spec clear(traverse:pool()) -> ok | {error, term()}.
clear(Pool) ->
    datastore_model:delete(?CTX, Pool).

%%%===================================================================
%%% Task management API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Updates information that new task has been scheduled.
%% Returns node chosen for task execution of error if it can not be executed (limit is reached).
%% @end
%%--------------------------------------------------------------------
-spec increment_ongoing_tasks_and_choose_node(traverse:pool()) -> {ok, node()} | {error, term()}.
increment_ongoing_tasks_and_choose_node(Pool) ->
    Diff = fun(#traverse_tasks_scheduler{ongoing_tasks = OT, ongoing_tasks_per_node = NodesOT,
        ongoing_tasks_limit = TL, ongoing_tasks_per_node_limit = NodeTL, nodes = Nodes} = Record) ->
        [ChosenNode | _] = NewNodes = update_nodes(Nodes),
        NodeOT = maps:get(ChosenNode, NodesOT, 0),
        case OT < TL andalso NodeOT < NodeTL of
            false ->
                {error, limit_exceeded};
            _ ->
                {ok, Record#traverse_tasks_scheduler{
                    ongoing_tasks = OT + 1,
                    ongoing_tasks_per_node = NodesOT#{ChosenNode => NodeOT + 1},
                    nodes = NewNodes
                }}
        end
    end,
    case datastore_model:update(?CTX, Pool, Diff) of
        {ok, #document{value = #traverse_tasks_scheduler{nodes = [Next | _]}}} ->
            {ok, Next};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates information that task has been ended (and no task has been started in its place).
%% @end
%%--------------------------------------------------------------------
-spec decrement_ongoing_tasks(traverse:pool()) -> ok | {error, term()}.
decrement_ongoing_tasks(Pool) ->
    Node = node(),
    Diff = fun(#traverse_tasks_scheduler{ongoing_tasks = OT, ongoing_tasks_per_node = NodesOT} = Record) ->
        NodeOT = maps:get(Node, NodesOT, 0),
        {ok, Record#traverse_tasks_scheduler{
            ongoing_tasks = OT - 1, ongoing_tasks_per_node = NodesOT#{Node => NodeOT - 1}}}
    end,
    extract_ok(datastore_model:update(?CTX, Pool, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Updates information about tasks on node.
%% @end
%%--------------------------------------------------------------------
-spec change_node_ongoing_tasks(traverse:pool(), node(), integer()) -> ok | {error, term()}.
change_node_ongoing_tasks(Pool, Node, TasksNum) ->
    Diff = fun(#traverse_tasks_scheduler{ongoing_tasks_per_node = NodesOT} = Record) ->
        NodeOT = maps:get(Node, NodesOT, 0),
        {ok, Record#traverse_tasks_scheduler{ongoing_tasks_per_node = NodesOT#{Node => max(NodeOT + TasksNum, 0)}}}
    end,
    extract_ok(datastore_model:update(?CTX, Pool, Diff)).

%%%===================================================================
%%% Group management API (groups waiting to be executed)
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Registers information about group of tasks. Tasks connected with the group are waiting to be executed on pool.
%% @end
%%--------------------------------------------------------------------
-spec register_group(traverse:pool(), traverse:group()) -> ok | {error, term()}.
register_group(Pool, Group) ->
    Diff = fun(#traverse_tasks_scheduler{groups = Groups} = Record) ->
        case lists:member(Group, Groups) of
            true ->
                {error, already_registered};
            _ ->
                {ok, Record#traverse_tasks_scheduler{groups = [Group | Groups]}}
        end
    end,
    Default = #traverse_tasks_scheduler{pool = Pool, groups = [Group]},
    extract_ok(datastore_model:update(?CTX, Pool, Diff, Default), {error, already_registered}).

%%--------------------------------------------------------------------
%% @doc
%% Deletes information about group of tasks. No tasks connected with the group is waiting be executed on pool.
%% @end
%%--------------------------------------------------------------------
-spec deregister_group(traverse:pool(), traverse:group()) -> ok | {error, term()}.
deregister_group(Pool, Group) ->
    Diff = fun(#traverse_tasks_scheduler{groups = Groups} = Record) ->
        case lists:member(Group, Groups) of
            false ->
                {error, already_canceled};
            _ ->
                {ok, Record#traverse_tasks_scheduler{groups = Groups -- [Group]}}
        end
    end,
    extract_ok(datastore_model:update(?CTX, Pool, Diff), {error, already_canceled}).

%%--------------------------------------------------------------------
%% @doc
%% Returns next group for task execution (task from the group will be started now).
%% @end
%%--------------------------------------------------------------------
-spec get_next_group(traverse:pool()) -> {ok, traverse:group()} | {error, term()}.
get_next_group(Pool) ->
    Diff = fun
        (#traverse_tasks_scheduler{groups = []}) ->
            {error, no_groups};
        (#traverse_tasks_scheduler{groups = [Group]}) ->
            {error, {single_group, Group}};
        (#traverse_tasks_scheduler{groups = Groups} = Record) ->
            Next = lists:last(Groups),
            Groups2 = [Next | lists:droplast(Groups)],
            {ok, Record#traverse_tasks_scheduler{groups = Groups2}}
    end,
    case datastore_model:update(?CTX, Pool, Diff) of
        {ok, #document{value = #traverse_tasks_scheduler{groups = [Next | _]}}} ->
            {ok, Next};
        {error, {single_group, Group}} ->
            {ok, Group};
        Other ->
            Other
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.

-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    2.

-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {pool, string},
        {ongoing_tasks, integer},
        {ongoing_tasks_limit, integer},
        {groups, [string]},
        {nodes, [atom]}
    ]};
get_record_struct(2) ->
    {record, [
        {pool, string},
        {ongoing_tasks, integer},
        {ongoing_tasks_per_node, #{atom => integer}},
        {ongoing_tasks_limit, integer},
        {ongoing_tasks_per_node_limit, integer},
        {groups, [string]},
        {nodes, [atom]}
    ]}.

-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, Pool, OngoingTasks, OngoingTasksLimit, Groups, Nodes}) ->
    OngoingTasksPerNodeLimit = max(1, ceil(OngoingTasksLimit / length(consistent_hashing:get_all_nodes()))),
    {2, {?MODULE, Pool, OngoingTasks, #{}, OngoingTasksLimit, OngoingTasksPerNodeLimit, Groups, Nodes}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec extract_ok(term()) -> term().
extract_ok({ok, _}) -> ok;
extract_ok(Result) -> Result.

-spec extract_ok(term(), term()) -> term().
extract_ok({ok, _}, _) -> ok;
extract_ok(IgnoredResult, IgnoredResult) -> ok;
extract_ok(Result, _) -> Result.

-spec update_nodes([node()]) -> [node()].
update_nodes([_] = Nodes) ->
    Nodes;
update_nodes(Nodes) ->
    Next = lists:last(Nodes),
    [Next | lists:droplast(Nodes)].