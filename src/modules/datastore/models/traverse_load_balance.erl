%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model that holds information about traverse tasks load balancing.
%%% @end
%%%-------------------------------------------------------------------
-module(traverse_load_balance).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%%% Pool management API
-export([init_pool/2, clear_pool/1]).
%%% Task management API
-export([add_task/1, finish_task/1]).
%%% Group management API
-export([register_group/2, cancel_group/2, get_next_group/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type ctx() :: datastore:ctx().
-type ongoing_tasks_limit() :: non_neg_integer().

-export_type([ongoing_tasks_limit/0]).

-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% Pool management API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes document for pool.
%% @end
%%--------------------------------------------------------------------
-spec init_pool(traverse:pool(), ongoing_tasks_limit()) -> ok | {error, term()}.
init_pool(Pool, Limit) ->
    Node = node(),
    New = #traverse_load_balance{pool = Pool,
        ongoing_tasks_limit = Limit, nodes = [Node]},
    Diff = fun(#traverse_load_balance{nodes = Nodes} = Record) ->
        {ok, Record#traverse_load_balance{nodes = [Node | (Nodes -- [Node])],
            ongoing_tasks_limit = Limit}}
    end,
    extract_ok(datastore_model:update(?CTX, Pool, Diff, New)).

%%--------------------------------------------------------------------
%% @doc
%% Clears document for pool.
%% @end
%%--------------------------------------------------------------------
-spec clear_pool(traverse:pool()) -> ok | {error, term()}.
clear_pool(Pool) ->
    datastore_model:delete(?CTX, Pool).

%%%===================================================================
%%% Task management API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves information about new task.
%% @end
%%--------------------------------------------------------------------
-spec add_task(traverse:pool()) -> {ok, node()} | {error, term()}.
add_task(Pool) ->
    Diff = fun(#traverse_load_balance{ongoing_tasks = OT,
        ongoing_tasks_limit = TL, nodes = Nodes} = Record) ->
        case OT < TL of
            false ->
                {error, limit_exceeded};
            _ ->
                {ok, Record#traverse_load_balance{ongoing_tasks = OT + 1,
                    nodes = update_nodes(Nodes)}}
        end
    end,
    case datastore_model:update(?CTX, Pool, Diff) of
        {ok, #document{value = #traverse_load_balance{nodes = [Next | _]}}} ->
            {ok, Next};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves information about task's finish.
%% @end
%%--------------------------------------------------------------------
-spec finish_task(traverse:pool()) -> ok | {error, term()}.
finish_task(Pool) ->
    Diff = fun(#traverse_load_balance{ongoing_tasks = OT} = Record) ->
        {ok, Record#traverse_load_balance{ongoing_tasks = OT - 1}}
    end,
    extract_ok(datastore_model:update(?CTX, Pool, Diff)).

%%%===================================================================
%%% Group management API (groups waiting to be executed)
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Registers information about group of tasks.
%% @end
%%--------------------------------------------------------------------
-spec register_group(traverse:pool(), traverse:group()) -> ok | {error, term()}.
register_group(Pool, Group) ->
    Diff = fun(#traverse_load_balance{groups = Groups} = Record) ->
        case lists:member(Group, Groups) of
            true ->
                {error, already_registered};
            _ ->
                {ok, Record#traverse_load_balance{groups = [Group | Groups]}}
        end
    end,
    Default = #traverse_load_balance{pool = Pool, groups = [Group]},
    extract_ok(datastore_model:update(?CTX, Pool, Diff, Default), {error, already_registered}).

%%--------------------------------------------------------------------
%% @doc
%% Deletes information about group of tasks.
%% @end
%%--------------------------------------------------------------------
-spec cancel_group(traverse:pool(), traverse:group()) -> ok | {error, term()}.
cancel_group(Pool, Group) ->
    Diff = fun(#traverse_load_balance{groups = Groups} = Record) ->
        case lists:member(Group, Groups) of
            false ->
                {error, already_canceled};
            _ ->
                {ok, Record#traverse_load_balance{groups = Groups -- [Group]}}
        end
    end,
    extract_ok(datastore_model:update(?CTX, Pool, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Returns next group for task execution.
%% @end
%%--------------------------------------------------------------------
-spec get_next_group(traverse:pool()) -> {ok, traverse:group()} | {error, term()}.
get_next_group(Pool) ->
    Diff = fun
               (#traverse_load_balance{groups = []}) ->
                   {error, no_groups};
               (#traverse_load_balance{groups = [Group]}) ->
                   {error, {single_group, Group}};
               (#traverse_load_balance{groups = Groups} = Record) ->
                   Next = lists:last(Groups),
                   Groups2 = [Next | lists:droplast(Groups)],
                   {ok, Record#traverse_load_balance{groups = Groups2}}
    end,
    case datastore_model:update(?CTX, Pool, Diff) of
        {ok, #document{value = #traverse_load_balance{groups = [Next | _]}}} ->
            {ok, Next};
        {error, {single_group, Group}} ->
            {ok, Group};
        Other ->
            Other
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {pool, atom},
        {ongoing_tasks, integer},
        {ongoing_tasks_limit, integer},
        {groups, [string]},
        {nodes, [atom]}
    ]}.

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
update_nodes(Nodes) ->
    case Nodes of
        [_] ->
            Nodes;
        _ ->
            Next = lists:last(Nodes),
            [Next | lists:droplast(Nodes)]
    end.