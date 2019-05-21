%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model that holds information about traverse tasks.
%%% @end
%%%-------------------------------------------------------------------
-module(traverse_task).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([create/7, start/6, on_task_start/5, update_description/2, update_status/2,
    finish/5, cancel/1, on_task_cancel/1, get/1, get_next_task/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type record() :: #traverse_task{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([key/0]).

% TODO - sortowac drzewa po czasie
-define(SCHEDULED_KEY(TaskModule), ?LINK_KEY(TaskModule, "SCHEDULED_")).
-define(ONGOING_KEY(TaskModule), ?LINK_KEY(TaskModule, "ONGOING_")).
-define(ENDED_KEY(TaskModule), ?LINK_KEY(TaskModule, "ENDED_")).
-define(LINK_KEY(TaskModule, Prefix), <<Prefix, (atom_to_binary(TaskModule, utf8))/binary>>).
-define(GROUP_KEY(Key, Group), <<Key/binary, "###", Group/binary>>).

-define(CTX, #{
    model => ?MODULE
}).

-define(CTX(TreeID), #{
    model => ?MODULE,
    local_links_tree_id => TreeID
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates task document.
%% @end
%%--------------------------------------------------------------------
-spec create(key(), traverse:task_module(), traverse:executor(), traverse:executor(),
    traverse:group(), traverse:job_id() | undefined, traverse:description()) -> ok.
create(ID, TaskModule, Executor, Creator, GroupID, ScheduledJob, InitialDescription) ->
    Value0 = #traverse_task{task_module = TaskModule,
            description = InitialDescription, executor = Executor, group = GroupID},

    {LinkKey, LinkValue, Value} = case ScheduledJob of
        undefined -> {?ONGOING_KEY(TaskModule), ID, Value0#traverse_task{status = ongoing, enqueued = false}};
        _ -> {?SCHEDULED_KEY(TaskModule), ScheduledJob, Value0}
    end,

    {ok, _} = datastore_model:create(?CTX, #document{key = ID, value = Value}),

    run_on_trees(LinkKey, GroupID, fun(Key) ->
        [{ok, _}] = datastore_model:add_links(?CTX(Creator),
            Key, Creator, [{ID, LinkValue}])
    end),
    % TODO - chyba bez sensu bo rejestrujemy grupy ktore czekaja (wiec nie zawsze trzeba to robic - sprawdz ScheduledJob)
    ok = traverse_tasks_load_balance:register_group(TaskModule, GroupID),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Updates information about task's start.
%% @end
%%--------------------------------------------------------------------
-spec start(key(), traverse:task_module(), traverse:group(),
    traverse:executor(), traverse:executor(), traverse:description()) -> ok.
start(ID, TaskModule, GroupID, Executor, Creator, NweDescription) ->
    Diff = fun(Task) ->
        {ok, Task#traverse_task{status = ongoing, enqueued = false}}
    end,
    {ok, _} = datastore_model:update(?CTX, ID, Diff),

    run_on_trees(?ONGOING_KEY(TaskModule), GroupID, fun(Key) ->
        [{ok, _}] = datastore_model:add_links(?CTX(Executor),
            Key, Executor, [{ID, ID}])
    end),

    case Creator =:= Executor of
        true ->
            on_task_start(ID, TaskModule, GroupID, Creator, NweDescription);
        _ ->
            ok
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Updates information when task is started by other executor.
%% @end
%%--------------------------------------------------------------------
-spec on_task_start(key(), traverse:task_module(), traverse:group(),
    traverse:executor(), traverse:description()) -> ok.
on_task_start(ID, TaskModule, GroupID, Creator, NweDescription) ->
    {ok, _} = update_description(ID, NweDescription),
    run_on_trees(?SCHEDULED_KEY(TaskModule), GroupID, fun(Key) ->
        [ok] = datastore_model:delete_links(?CTX(Creator),
            Key, Creator, [ID])
    end),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Updates task description field.
%% @end
%%--------------------------------------------------------------------
-spec update_description(key(), traverse:description()) ->
    {ok, traverse:description()} | {error, term()}.
update_description(ID, NweDescription) ->
    Diff = fun(#traverse_task{description = Description} = Task) ->
        FinalDescription = maps:fold(fun(K, V, Acc) ->
            Acc#{K => V + maps:get(K, Description, 0)}
        end, Description, NweDescription),
        {ok, Task#traverse_task{description = FinalDescription}}
    end,
    case datastore_model:update(?CTX, ID, Diff) of
        {ok, #document{value = #traverse_task{description = UpdatedDescription}}} ->
            {ok, UpdatedDescription};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates task status field.
%% @end
%%--------------------------------------------------------------------
-spec update_status(key(), traverse:status()) ->
    {ok, doc()} | {error, term()}.
update_status(ID, NewStatus) ->
    Diff = fun(Task) ->
        {ok, Task#traverse_task{status = NewStatus}}
    end,
    datastore_model:update(?CTX, ID, Diff).

%%--------------------------------------------------------------------
%% @doc
%% Finishes task.
%% @end
%%--------------------------------------------------------------------
-spec finish(key(), traverse:task_module(), traverse:executor(),
    traverse:group(), traverse:status()) -> ok | {error, term()}.
finish(ID, TaskModule, Executor, GroupID, FinalStatus) ->
    {ok, _} = update_status(ID, FinalStatus),

    run_on_trees(?ENDED_KEY(TaskModule), GroupID, fun(Key) ->
        [{ok, _}] = datastore_model:add_links(?CTX(Executor),
            Key, Executor, [{ID, ID}])
    end),
    run_on_trees(?ONGOING_KEY(TaskModule), GroupID, fun(Key) ->
        [ok] = datastore_model:delete_links(?CTX(Executor),
            Key, Executor, [ID])
    end),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Cancels task.
%% @end
%%--------------------------------------------------------------------
-spec cancel(key()) -> ok | {error, term()}.
cancel(ID) ->
    Diff = fun(Task) ->
        {ok, Task#traverse_task{canceled = true}}
    end,
    {ok, _} = datastore_model:update(?CTX, ID, Diff),
    ok.

% TODO !!!
on_task_cancel(_ID) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns task.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns next task if for group.
%% @end
%%--------------------------------------------------------------------
-spec get_next_task(traverse:task_module(), traverse:group()) ->
    {ok, {traverse:id(), traverse:job_id(), traverse:executor()} | no_tasks_found}
    | {error, term()}.
get_next_task(TaskModule, Group) ->
    datastore_model:fold_links(?CTX, ?GROUP_KEY(?SCHEDULED_KEY(TaskModule), Group), all, fun
        (#link{name = Name, target = Target, tree_id = Creator}, _) ->
            {stop, {Name, Target, Creator}}
    end, no_tasks_found, #{}).

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
        {status, atom},
        {enqueued, boolean},
        {canceled, boolean},
        {value, {custom, {json_utils, encode, decode}}}
    ]}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec run_on_trees(key(), traverse:group(), fun((key()) -> ok)) -> ok | no_return().
run_on_trees(Key, Group, Fun) ->
    Fun(Key),
    Fun(?GROUP_KEY(Key, Group)),
    ok.