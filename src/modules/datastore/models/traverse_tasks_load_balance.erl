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
-module(traverse_tasks_load_balance).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%%% Task and task module management API
-export([list_task_modules/0, init_task_module/2, add_task/1, add_task/2, finish_task/1]).
%%% Group management API
-export([register_group/2, get_next_group/1, cancel_group_init/2, cancel_group/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type ctx() :: datastore:ctx().

-define(KEY(TaskModule), atom_to_binary(TaskModule, utf8)).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

% TODO - co z wieloma providerami? co w wieloma node'ami?

%%%===================================================================
%%% Task and task module management API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all task modules.
%% @end
%%--------------------------------------------------------------------
-spec list_task_modules() -> {ok, [traverse:task_module()]} | {error, term()}.
list_task_modules() ->
    datastore_model:fold(?CTX,
        fun(#document{value = #traverse_tasks_load_balance{task_module = Module}}, Acc) ->
            {ok, [Module | Acc]}
        end, []).

%%--------------------------------------------------------------------
%% @doc
%% Initializes document for task module.
%% @end
%%--------------------------------------------------------------------
-spec init_task_module(traverse:task_module(), non_neg_integer()) -> ok | {error, term()}.
init_task_module(TaskModule, Limit) ->
    Doc = #document{key = ?KEY(TaskModule),
        value = #traverse_tasks_load_balance{task_module = TaskModule, ongoing_tasks_limit = Limit}},
    extract_ok(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% @equiv add_task(TaskModule, undefined).
%% @end
%%--------------------------------------------------------------------
-spec add_task(traverse:task_module()) -> ok | {error, term()}.
add_task(TaskModule) ->
    add_task(TaskModule, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Saves information about new task.
%% @end
%%--------------------------------------------------------------------
-spec add_task(traverse:task_module(), traverse:group() | undefined) -> ok | {error, term()}.
add_task(TaskModule, Group) ->
    Diff = fun(#traverse_tasks_load_balance{ongoing_tasks = OT,
        ongoing_tasks_limit = TL, groups = Groups} = Record) ->
        case {OT < TL, Group} of
            {false, _} ->
                {error, limit_exceeded};
            {_, undefined} ->
                {ok, Record#traverse_tasks_load_balance{ongoing_tasks = OT + 1}};
            _ ->
                {ok, Record#traverse_tasks_load_balance{
                    ongoing_tasks = OT + 1, groups = [Group | (Groups -- [Group])]}}
        end
    end,
    extract_ok(datastore_model:update(?CTX, ?KEY(TaskModule), Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Saves information about task's finish.
%% @end
%%--------------------------------------------------------------------
-spec finish_task(traverse:task_module()) -> ok | {error, term()}.
finish_task(TaskModule) ->
    Diff = fun(#traverse_tasks_load_balance{ongoing_tasks = OT} = Record) ->
        {ok, Record#traverse_tasks_load_balance{ongoing_tasks = OT - 1}}
    end,
    extract_ok(datastore_model:update(?CTX, ?KEY(TaskModule), Diff)).

%%%===================================================================
%%% Group management API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Registers information about group of tasks.
%% @end
%%--------------------------------------------------------------------
-spec register_group(traverse:task_module(), traverse:group()) -> ok | {error, term()}.
register_group(TaskModule, Group) ->
    Diff = fun(#traverse_tasks_load_balance{groups = Groups, groups_to_cancel = ToCancel} = Record) ->
        case lists:member(Group, Groups) of
            true ->
                {error, already_registered};
            _ ->
                {ok, Record#traverse_tasks_load_balance{groups = [Group | Groups],
                    groups_to_cancel = ToCancel -- [Group]}}
        end
    end,
    Default = #traverse_tasks_load_balance{task_module = TaskModule, groups = [Group]},
    case datastore_model:update(?CTX, ?KEY(TaskModule), Diff, Default) of
        {ok, _} ->
            ok;
        {error, already_registered} ->
            ok;
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Initializes delete of information about group of tasks.
%% @end
%%--------------------------------------------------------------------
-spec cancel_group_init(traverse:task_module(), traverse:group()) ->
    ok | already_canceled | {error, term()}.
cancel_group_init(TaskModule, Group) ->
    Diff = fun(#traverse_tasks_load_balance{groups = Groups, groups_to_cancel = ToCancel} = Record) ->
        case lists:member(Group, Groups) of
            false ->
                {error, not_found};
            _ ->
                {ok, Record#traverse_tasks_load_balance{groups = Groups -- [Group],
                    groups_to_cancel = [Group | ToCancel]}}
        end
    end,
    case datastore_model:update(?CTX, ?KEY(TaskModule), Diff) of
        {ok, _} ->
            ok;
        {error, not_found} ->
            already_canceled;
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes information about group of tasks.
%% @end
%%--------------------------------------------------------------------
-spec cancel_group(traverse:task_module(), traverse:group()) -> ok | {error, term()}.
cancel_group(TaskModule, Group) ->
    Diff = fun(#traverse_tasks_load_balance{groups_to_cancel = ToCancel} = Record) ->
        case lists:member(Group, ToCancel) of
            false ->
                {error, not_found};
            _ ->
                {ok, Record#traverse_tasks_load_balance{groups_to_cancel = ToCancel -- [Group]}}
        end
           end,
    case datastore_model:update(?CTX, ?KEY(TaskModule), Diff) of
        {ok, _} ->
            ok;
        {error, not_found} ->
            ok;
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns next group for task execution.
%% @end
%%--------------------------------------------------------------------
-spec get_next_group(traverse:task_module()) -> {ok, traverse:group()} | {error, term()}.
get_next_group(TaskModule) ->
    Diff = fun
               (#traverse_tasks_load_balance{groups = []}) ->
                   {error, no_groups};
               (#traverse_tasks_load_balance{groups = [Group]}) ->
                   {error, {single_group, Group}};
               (#traverse_tasks_load_balance{groups = Groups} = Record) ->
                   Next = lists:last(Groups),
                   Groups2 = [Next | lists:droplast(Groups)],
                   {ok, Record#traverse_tasks_load_balance{groups = Groups2}}
    end,
    case datastore_model:update(?CTX, ?KEY(TaskModule), Diff) of
        {ok, #document{value = #traverse_tasks_load_balance{groups = [Next | _]}}} ->
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
        {task_module, atom},
        {ongoing_tasks, integer},
        {ongoing_tasks_limit, integer},
        {groups, [string]},
        {groups_to_cancel, [string]}
    ]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec extract_ok(term()) -> term().
extract_ok({ok, _}) -> ok;
extract_ok(Result) -> Result.