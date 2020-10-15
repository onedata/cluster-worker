%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Basic model that holds information about traverse tasks (see traverse.erl).
%%% Each traverse_task document represents single traverse that can be scheduled, ongoing (executed on pool)
%%% or finished (successfully or canceled).
%%% Model can be synchronized as context is provided to functions as an argument (which can include sync info).
%%% @end
%%%-------------------------------------------------------------------
-module(traverse_task).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

%% Lifecycle API
-export([create/11, start/5, schedule_for_local_execution/3, finish/5, cancel/5, on_task_change/2,
    on_remote_change/4, delete_ended/2]).

%%% Setters and getters API
-export([update_description/4, update_status/4, fix_description/4,
    get/2, get_execution_info/1, get_execution_info/2, is_enqueued/1,
    get_additional_data/1, get_additional_data/2, update_additional_data/4]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1, resolve_conflict/3]).

%% code/decode description
-export([encode_description/1, decode_description/1]).

-type ctx() :: datastore:ctx().
-type record() :: #traverse_task{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([ctx/0, doc/0]).

-define(CTX, #{
    model => ?MODULE
}).

-define(ID_SEPARATOR, "###").
-define(ID_SEPARATOR_BINARY, <<?ID_SEPARATOR>>).
-define(DOC_ID(Pool, TASK_ID), <<Pool/binary, ?ID_SEPARATOR, TASK_ID/binary>>).

%%%===================================================================
%%% Lifecycle API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates task document. Values of document fields are different when task will be started immediately and different
%% when the task will wait (see traverse_tasks_scheduler.erl).
%% @end
%%--------------------------------------------------------------------
-spec create(ctx(), traverse:pool(), traverse:callback_module(), traverse:id(), traverse:environment_id(),
    traverse:environment_id(), traverse:group(), traverse:job_id(), remote | undefined | node(),
    traverse:description(), traverse:additional_data()) -> ok.
create(ExtendedCtx, Pool, CallbackModule, TaskId, Creator, Executor, GroupId, Job, Node, InitialDescription,
    AdditionalData) ->
    {ok, Timestamp} = get_timestamp(CallbackModule),
    Value0 = #traverse_task{
        callback_module = CallbackModule,
        creator = Creator,
        executor = Executor,
        group = GroupId,
        description = InitialDescription,
        schedule_time = Timestamp,
        main_job_id = Job,
        additional_data = AdditionalData
    },

    Value = case Node of
        undefined -> Value0;
        remote -> Value0;
        _ -> Value0#traverse_task{status = ongoing, enqueued = false, node = Node, start_time = Timestamp}
    end,
    {ok, _} = datastore_model:create(ExtendedCtx, #document{key = ?DOC_ID(Pool, TaskId), value = Value}),

    case Node of
        undefined -> % task is scheduled for later execution
            ok = traverse_task_list:add_scheduled_link(Pool, Creator, TaskId, Timestamp, GroupId, Executor),
            ok = traverse_task_list:add_link(ExtendedCtx, Pool, scheduled, Executor, TaskId, Timestamp),
            ok = traverse_tasks_scheduler:register_group(Pool, GroupId);
        remote ->
            ok = traverse_task_list:add_link(ExtendedCtx, Pool, scheduled, Executor, TaskId, Timestamp);
        _ -> % task will be immediately started
            ok = traverse_task_list:add_link(ExtendedCtx, Pool, ongoing, Executor, TaskId, Timestamp)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates information about task's start (used if task has not been started immediately after creation).
%% @end
%%--------------------------------------------------------------------
-spec start(ctx(), traverse:pool(), traverse:callback_module(), traverse:id(), traverse:description()) ->
    ok | {error, term()}.
start(ExtendedCtx, Pool, CallbackModule, TaskId, NewDescription) ->
    Node = node(),
    {ok, Timestamp} = get_timestamp(CallbackModule),
    Diff = fun
        (#traverse_task{
            status = scheduled,
            canceled = false,
            executor = Executor,
            creator = Creator
        } = Task) when Creator =:= Executor ->
            {ok, Task#traverse_task{
                status = ongoing,
                enqueued = false,
                node = Node,
                description = NewDescription,
                start_time = Timestamp
            }};
        (#traverse_task{status = scheduled, canceled = false} = Task) ->
            {ok, Task#traverse_task{
                status = ongoing,
                node = Node,
                description = NewDescription,
                start_time = Timestamp
            }};
        (#traverse_task{status = scheduled, canceled = true} = Task) ->
            {ok, Task#traverse_task{
                status = canceled,
                finish_time = Timestamp
            }};
        (_) ->
            {error, start_aborted}
    end,
    case datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskId), Diff) of
        {ok, #document{value = #traverse_task{
            canceled = Canceled,
            schedule_time = ScheduleTimestamp,
            executor = Executor,
            creator = Creator,
            group = GroupId
        }}} ->
            case Canceled of
                false -> ok = traverse_task_list:add_link(ExtendedCtx, Pool, ongoing, Executor, TaskId, Timestamp);
                true -> ok = traverse_task_list:add_link(ExtendedCtx, Pool, ended, Executor, TaskId, Timestamp)
            end,

            ok = traverse_task_list:delete_scheduled_link(Pool, Creator, TaskId, ScheduleTimestamp, GroupId, Executor),

            case Creator =:= Executor of
                true ->
                    ok = traverse_task_list:delete_link(
                        ExtendedCtx, Pool, scheduled, Executor, TaskId, ScheduleTimestamp);
                _ ->
                    ok
            end,

            case Canceled of
                true -> {error, start_aborted};
                false -> ok
            end;
        Other ->
            Other
    end.

-spec schedule_for_local_execution(traverse:pool(), traverse:id(), traverse:task()) -> ok.
schedule_for_local_execution(Pool, TaskId, #document{value = #traverse_task{
    schedule_time = Timestamp,
    executor = Executor,
    creator = Creator,
    group = GroupId
}}) ->
    ok = traverse_task_list:add_scheduled_link(Pool, Creator, TaskId, Timestamp, GroupId, Executor),
    ok = traverse_tasks_scheduler:register_group(Pool, GroupId).


-spec finish(ctx(), traverse:pool(), traverse:callback_module(), traverse:id(), boolean()) -> ok | {error, term()}.
finish(ExtendedCtx, Pool, CallbackModule, TaskId, Cancel) ->
    {ok, Timestamp} = get_timestamp(CallbackModule),
    Diff = fun
        (#traverse_task{status = ongoing, canceled = false} = Task) when Cancel =:= false ->
            {ok, Task#traverse_task{status = finished, finish_time = Timestamp}};
        (#traverse_task{status = ongoing} = Task) ->
            {ok, Task#traverse_task{status = canceled, finish_time = Timestamp}};
        (#traverse_task{status = canceling, canceled = true} = Task) ->
            {ok, Task#traverse_task{status = canceled, finish_time = Timestamp}};
        (#traverse_task{status = finished}) ->
            {error, already_finished};
        (#traverse_task{status = canceled}) ->
            {error, already_finished}
    end,
    case datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskId), Diff) of
        {ok, #document{value = #traverse_task{start_time = StartTimestamp, executor = Executor}}} ->
            ok = traverse_task_list:add_link(ExtendedCtx, Pool, ended, Executor, TaskId, Timestamp),
            ok = traverse_task_list:delete_link(ExtendedCtx, Pool, ongoing, Executor, TaskId, StartTimestamp);
        Other ->
            Other
    end.

-spec cancel(ctx(), traverse:pool(), traverse:callback_module(), traverse:id(), traverse:environment_id()) ->
    {ok, local_cancel | remote_cancel | already_canceled} | {error, term()}.
cancel(ExtendedCtx, Pool, CallbackModule, TaskId, Environment) ->
    {ok, Timestamp} = get_timestamp(CallbackModule),
    Diff = fun
        (#traverse_task{status = scheduled, canceled = false, executor = Executor, creator = Creator} = Task) when
            Executor =:= Environment, Creator =:= Environment ->
            {ok, Task#traverse_task{status = canceled, canceled = true, enqueued = false, finish_time = Timestamp}};
        (#traverse_task{status = scheduled, canceled = false, executor = Executor} = Task) when Executor =:= Environment ->
            {ok, Task#traverse_task{status = canceled, canceled = true, finish_time = Timestamp}};
        (#traverse_task{status = Status, canceled = false, executor = Executor} = Task) when
            Executor =:= Environment, Status =/= finished ->
            {ok, Task#traverse_task{status = canceling, canceled = true}};
        (#traverse_task{status = Status, canceled = false} = Task) when Status =/= finished ->
            {ok, Task#traverse_task{canceled = true}};
        (_) ->
            {error, already_canceled}
    end,
    case datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskId), Diff) of
        {ok, #document{value = #traverse_task{
            schedule_time = ScheduleTimestamp,
            executor = Executor,
            creator = Creator,
            group = GroupId,
            status = Status
        }}} ->
            case {Status, Environment} of
                {canceled, Creator} ->
                    ok = traverse_task_list:delete_link(
                        ExtendedCtx, Pool, scheduled, Executor, TaskId, ScheduleTimestamp);
                _ ->
                    ok
            end,

            case {Status, Environment} of
                {canceled, Executor} ->
                    ok = traverse_task_list:delete_scheduled_link(
                        Pool, Creator, TaskId, ScheduleTimestamp, GroupId, Executor),
                    ok = traverse_task_list:add_link(ExtendedCtx, Pool, ended, Executor, TaskId, Timestamp),
                    {ok, ended};
                {_, Executor} ->
                    {ok, local_cancel};
                _ ->
                    {ok, remote_cancel}
            end;
        {error, already_canceled} ->
            {ok, already_canceled};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Decides action to be done on remote task change.
%% @end
%%--------------------------------------------------------------------
-spec on_task_change(doc(), traverse:environment_id()) ->
    {remote_change, traverse:callback_module(), traverse:job_id()} |
    {run, traverse:callback_module(), traverse:job_id()} |
    ignore.
on_task_change(#document{value = #traverse_task{
    status = Status,
    enqueued = true,
    creator = Environment,
    callback_module = CallbackModule,
    main_job_id = MainJobId}
}, Environment) when Status =/= scheduled ->
    {remote_change, CallbackModule, MainJobId};
on_task_change(#document{value = #traverse_task{
    status = Status,
    canceled = true,
    executor = Environment,
    callback_module = CallbackModule,
    main_job_id = MainJobId}
}, Environment) when Status =:= scheduled ; Status =:= ongoing ->
    {remote_change, CallbackModule, MainJobId};
on_task_change(#document{value = #traverse_task{
    status = scheduled,
    canceled = false,
    executor = Environment,
    callback_module = CallbackModule,
    main_job_id = MainJobId}
}, Environment) ->
    {run, CallbackModule, MainJobId};
on_task_change(_Doc, _Environment) ->
    ignore.

%%--------------------------------------------------------------------
%% @doc
%% Updates information when task is processed on different environment.
%% @end
%% TODO - VFS-5530
%%--------------------------------------------------------------------
-spec on_remote_change(ctx(), doc(), traverse:callback_module(), traverse:environment_id()) ->
    ok | {ok, remote_cancel, traverse:id()}.
on_remote_change(ExtendedCtx, #document{key = DocId, value = #traverse_task{
    status = Status,
    enqueued = true,
    creator = Environment}
}, _CallbackModule, Environment) when Status =/= scheduled ->
    {Pool, TaskId} = decode_id(DocId),
    Diff = fun
        (#traverse_task{enqueued = true} = Task) ->
            {ok, Task#traverse_task{enqueued = false}};
        (_) ->
            {error, update_not_needed}
    end,

    case datastore_model:update(ExtendedCtx, DocId, Diff) of
        {ok, #document{value = #traverse_task{
            schedule_time = Timestamp,
            executor = Executor
        }}} ->
            ok = traverse_task_list:delete_link(
                ExtendedCtx, Pool, scheduled, Executor, TaskId, Timestamp);
        {error, update_not_needed} ->
            ok
    end;
on_remote_change(ExtendedCtx, #document{key = DocId, value = #traverse_task{
    status = scheduled,
    canceled = true,
    schedule_time = ScheduleTimestamp,
    executor = Executor,
    creator = Creator,
    group = GroupId}
} = Doc, CallbackModule, Environment) ->
    {ok, Timestamp} = get_timestamp(CallbackModule),
    {Pool, TaskId} = decode_id(DocId),
    Diff = fun
        (#traverse_task{status = scheduled} = Task) ->
            {ok, Task#traverse_task{status = canceled, finish_time = Timestamp}};
        (ChangedRecord) ->
            {error, {update_not_needed, ChangedRecord}}
    end,

    UpdatedDoc = case datastore_model:update(ExtendedCtx, DocId, Diff) of
        {ok, NewDoc} ->
            ok = traverse_task_list:delete_scheduled_link(Pool, Creator, TaskId, ScheduleTimestamp, GroupId, Executor),
            ok = traverse_task_list:add_link(ExtendedCtx, Pool, ended, Environment, TaskId, Timestamp),
            NewDoc;
        {error, {update_not_needed, Changed}} ->
            Doc#document{value = Changed}
    end,
    on_remote_change(ExtendedCtx, UpdatedDoc, CallbackModule, Environment);
on_remote_change(ExtendedCtx, #document{key = DocId, value = #traverse_task{
    status = ongoing,
    canceled = true,
    executor = Environment}
}, _CallbackModule, Environment) ->
    Diff = fun
        (#traverse_task{status = ongoing} = Task) ->
            {ok, Task#traverse_task{status = canceling}};
        (_) ->
            {error, update_not_needed}
    end,

    case datastore_model:update(ExtendedCtx, DocId, Diff) of
        {ok, _} ->
            {_Pool, TaskId} = decode_id(DocId),
            {ok, remote_cancel, TaskId};
        {error, update_not_needed} ->
            ok
    end;
on_remote_change(_ExtendedCtx, _Doc, _CallbackModule, _Environment) ->
    ok. % Task has been modified in parallel - ignore

-spec delete_ended(traverse:pool(), traverse:id()) -> ok.
delete_ended(Pool, TaskId) ->
    case get(Pool, TaskId) of
        {ok, #document{value = #traverse_task{finish_time = Timestamp, executor = Executor}}} ->
            traverse_task_list:delete_link(?CTX, Pool, ended, Executor, TaskId, Timestamp),
            datastore_model:delete(?CTX, ?DOC_ID(Pool, TaskId));
        {error, not_found} ->
            ok
    end.

%%%===================================================================
%%% Setters and getters API
%%%===================================================================

-spec update_description(ctx(), traverse:pool(), traverse:id(), traverse:description()) ->
    {ok, traverse:description(), boolean()} | {error, term()}.
update_description(ExtendedCtx, Pool, TaskId, NewDescription) ->
    Diff = fun(#traverse_task{description = Description} = Task) ->
        FinalDescription = maps:fold(fun(K, V, Acc) ->
            Acc#{K => V + maps:get(K, Description, 0)}
        end, Description, NewDescription),
        {ok, Task#traverse_task{description = FinalDescription}}
    end,
    case datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskId), Diff) of
        {ok, #document{value = #traverse_task{
            description = UpdatedDescription,
            canceled = Canceled
        }}} ->
            {ok, UpdatedDescription, Canceled};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates task status field. To be used by user of framework if intermediate states are needed
%% (the function is not used by the framework).
%% @end
%%--------------------------------------------------------------------
-spec update_status(ctx(), traverse:pool(), traverse:id(), traverse:status()) ->
    {ok, doc()} | {error, term()}.
update_status(ExtendedCtx, Pool, TaskId, NewStatus) ->
    Diff = fun(Task) ->
        {ok, Task#traverse_task{status = NewStatus}}
    end,
    datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskId), Diff).

%%--------------------------------------------------------------------
%% @doc
%% Fix task description after reboot clearing information about delegated and not finished tasks.
%% @end
%%--------------------------------------------------------------------
-spec fix_description(ctx(), traverse:pool(), traverse:id(), node()) -> {ok, doc()} | {error, term()}.
fix_description(ExtendedCtx, Pool, TaskId, NodeToFix) ->
    LocalNode = node(),
    Diff = fun
        (#traverse_task{
            node = TaskNode,
            description = Description} = Task
        ) when TaskNode =:= NodeToFix ->
            MDone = maps:get(master_jobs_done, Description, 0),
            MFailed = maps:get(master_jobs_failed, Description, 0),

            SDone = maps:get(slave_jobs_done, Description, 0),
            SFailed = maps:get(slave_jobs_failed, Description, 0),

            FinalDescription = Description#{
                master_jobs_delegated => MDone + MFailed,
                slave_jobs_delegated => SDone + SFailed
            },
            {ok, Task#traverse_task{description = FinalDescription, node = LocalNode}};
        (_) ->
            {error, other_node}
    end,

    datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskId), Diff).

-spec get(traverse:pool(), traverse:id()) -> {ok, doc()} | {error, term()}.
get(Pool, TaskId) ->
    datastore_model:get(?CTX, ?DOC_ID(Pool, TaskId)).

-spec is_enqueued(doc()) -> boolean().
is_enqueued(#document{value = #traverse_task{
    enqueued = Enqueued,
    canceled = Canceled
}}) ->
    Enqueued and not Canceled.

-spec get_execution_info(doc()) -> {ok, traverse:callback_module(), traverse:environment_id(),
    traverse:job_id(), node(), traverse:timestamp()}.
get_execution_info(#document{value = #traverse_task{
    callback_module = CallbackModule,
    executor = Executor,
    main_job_id = MainJobId,
    node = Node,
    start_time = StartTimestamp
}}) ->
    {ok, CallbackModule, Executor, MainJobId, Node, StartTimestamp}.

-spec get_execution_info(traverse:pool(), traverse:id()) -> {ok, traverse:callback_module(),
    traverse:environment_id(), traverse:job_id(), node()} | {error, term()}.
get_execution_info(Pool, TaskId) ->
    case datastore_model:get(?CTX, ?DOC_ID(Pool, TaskId)) of
        {ok, Doc} ->
            get_execution_info(Doc);
        Other ->
            Other
    end.

-spec get_additional_data(doc()) -> {ok, traverse:additional_data()}.
get_additional_data(#document{value = #traverse_task{additional_data = AdditionalData}}) ->
    {ok, AdditionalData}.

-spec get_additional_data(traverse:pool(), traverse:id()) -> {ok, traverse:additional_data()} | {error, term()}.
get_additional_data(Pool, TaskId) ->
    case datastore_model:get(?CTX, ?DOC_ID(Pool, TaskId)) of
        {ok, Doc} ->
            get_additional_data(Doc);
        Other ->
            Other
    end.

-spec update_additional_data(ctx(), traverse:pool(), traverse:id(), traverse:status()) ->
    {ok, doc()} | {error, term()}.
update_additional_data(ExtendedCtx, Pool, TaskId, NewAdditionalData) ->
    Diff = fun(Task) ->
        {ok, Task#traverse_task{additional_data = NewAdditionalData}}
    end,
    datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskId), Diff).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns basic model's context. It can be extended using callback and such extended context is provided
%% to functions (see traverse.erl).
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.

-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {callback_module, atom},
        {creator, string},
        {executor, string},
        {group, string},
        {schedule_time, integer},
        {start_time, integer},
        {finish_time, integer},
        {main_job_id, string},
        {enqueued, boolean},
        {canceled, boolean},
        {node, atom},
        {status, atom},
        {description, {custom, json, {?MODULE, encode_description, decode_description}}},
        {additional_data, #{string => binary}}
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Provides custom resolution of remote, concurrent modification conflicts.
%% Prefers documents modified by task executor. Allows canceled and enqueued fields modification on other environments.
%% @end
%%--------------------------------------------------------------------
-spec resolve_conflict(ctx(), doc(), doc()) -> {boolean(), doc()} | ignore | default.
resolve_conflict(_Ctx, NewDoc, PrevDoc) ->
    #document{value = #traverse_task{executor = Executor} = NewValue, mutators = [NewExecutor | _]} = NewDoc,
    #document{value = PrevValue, mutators = [PrevExecutor | _]} = PrevDoc,

    #traverse_task{canceled = C1, enqueued = E1} = NewValue,
    #traverse_task{canceled = C2, enqueued = E2} = PrevValue,

    Diff = (C1 =/= C2) orelse (E1 =/= E2),

    case {Executor, Diff} of
        {NewExecutor, false} ->
            {false, NewDoc};
        {PrevExecutor, false} ->
            ignore;
        {NewExecutor, true} ->
            {true, NewDoc#document{value = NewValue#traverse_task{canceled = C1 or C2, enqueued = E1 and E2}}};
        {PrevExecutor, true} ->
            {true, PrevDoc#document{value = PrevValue#traverse_task{canceled = C1 or C2, enqueued = E1 and E2}}};
        {_, true} ->
            #document{revs = [NewRev | _]} = NewDoc,
            #document{revs = [PrevRev | _]} = PrevDoc,
            case datastore_rev:is_greater(NewRev, PrevRev) of
                true ->
                    {true, NewDoc#document{value = NewValue#traverse_task{canceled = C1 or C2, enqueued = E1 and E2}}};
                false ->
                    {true, PrevDoc#document{value = PrevValue#traverse_task{canceled = C1 or C2, enqueued = E1 and E2}}}
            end;
        _ ->
            default
    end.

%%--------------------------------------------------------------------
%% @doc
%% Encodes description field to JSON format.
%% @end
%%--------------------------------------------------------------------
-spec encode_description(traverse:description()) -> binary().
encode_description(Description) ->
    json_utils:encode(Description).

%%--------------------------------------------------------------------
%% @doc
%% Decodes description field from JSON format.
%% @end
%%--------------------------------------------------------------------
-spec decode_description(binary()) -> traverse:description().
decode_description(Term) ->
    Map = json_utils:decode(Term),
    maps:fold(fun(K, V, Acc) ->
        Acc#{binary_to_atom(K, utf8) => V}
    end, #{}, Map).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_timestamp(traverse:callback_module()) -> {ok, traverse:timestamp()}.
get_timestamp(CallbackModule) ->
    case erlang:function_exported(CallbackModule, get_timestamp, 0) of
        true -> CallbackModule:get_timestamp();
        _ -> {ok, time_utils:timestamp_seconds()}
    end.

-spec decode_id(datastore:key()) -> {traverse:pool(), traverse:id()}.
decode_id(DocId) ->
    [Pool, TaskId] = binary:split(DocId, ?ID_SEPARATOR_BINARY),
    {Pool, TaskId}.