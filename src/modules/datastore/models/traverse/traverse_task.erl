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
-export([create/10, start/4, finish/3, cancel/4, on_task_change/5]).
%%% Setters and getters API
-export([update_description/4, update_status/4, fix_description/3,
    get/2, get_execution_info/1, get_execution_info/2, is_enqueued/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

%% code/decode description
-export([encode_description/1, decode_description/1]).

-type ctx() :: datastore:ctx().
-type record() :: #traverse_task{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([ctx/0, doc/0]).

-define(CTX, #{
    model => ?MODULE
}).

-define(DOC_ID(Pool, TASK_ID), <<Pool/binary, "###", TASK_ID/binary>>).

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
    traverse:environment_id(), traverse:group(), traverse:job_id(), undefined | node(), traverse:description()) -> ok.
create(ExtendedCtx, Pool, CallbackModule, TaskID, Creator, Executor, GroupID, Job, Node, InitialDescription) ->
    {ok, Timestamp} = get_timestamp(CallbackModule),
    Value0 = #traverse_task{
        pool = Pool,
        callback_module = CallbackModule,
        creator = Creator,
        executor = Executor,
        group = GroupID,
        description = InitialDescription,
        timestamp = Timestamp,
        main_job_id = Job
    },

    Value = case Node of
        undefined -> Value0;
        _ -> Value0#traverse_task{status = ongoing, enqueued = false, node = Node}
    end,
    {ok, _} = datastore_model:create(ExtendedCtx, #document{key = ?DOC_ID(Pool, TaskID), value = Value}),

    case Node of
        undefined -> % task is scheduled for later execution
            ok = traverse_task_list:add_scheduled_link(ExtendedCtx, Pool, Creator, TaskID, Timestamp, GroupID, Executor),
            ok = traverse_tasks_scheduler:register_group(Pool, GroupID);
        _ -> % task will be immediately started
            ok = traverse_task_list:add_link(ExtendedCtx, Pool, ongoing, Executor, TaskID, Timestamp)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates information about task's start (used if task has not been started immediately after creation).
%% @end
%%--------------------------------------------------------------------
-spec start(ctx(), traverse:pool(), traverse:id(), traverse:description()) -> ok | {error, term()}.
start(ExtendedCtx, Pool, TaskID, NewDescription) ->
    Node = node(),
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
                description = NewDescription
            }};
        (#traverse_task{status = scheduled, canceled = false} = Task) ->
            {ok, Task#traverse_task{
                status = ongoing,
                node = Node,
                description = NewDescription
            }};
        (_) ->
            {error, start_aborted}
    end,
    case datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskID), Diff) of
        {ok, #document{value = #traverse_task{
            timestamp = Timestamp,
            executor = Executor,
            creator = Creator,
            group = GroupID
        }}} ->
            ok = traverse_task_list:add_link(ExtendedCtx, Pool, ongoing, Executor, TaskID, Timestamp),

            case Creator =:= Executor of
                true ->
                    ok = traverse_task_list:delete_scheduled_link(
                        ExtendedCtx, Pool, Creator, TaskID, Timestamp, GroupID, Executor);
                _ ->
                    ok
            end;
        Other ->
            Other
    end.

-spec finish(ctx(), traverse:pool(), traverse:id()) -> ok | {error, term()}.
finish(ExtendedCtx, Pool, TaskID) ->
    Diff = fun
        (#traverse_task{status = ongoing, canceled = false} = Task) ->
            {ok, Task#traverse_task{status = finished}};
        (#traverse_task{status = ongoing, canceled = true} = Task) ->
            {ok, Task#traverse_task{status = canceled}}
    end,
    case datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskID), Diff) of
        {ok, #document{value = #traverse_task{timestamp = Timestamp, executor = Executor}}} ->
            ok = traverse_task_list:add_link(ExtendedCtx, Pool, ended, Executor, TaskID, Timestamp),
            ok = traverse_task_list:delete_link(ExtendedCtx, Pool, ongoing, Executor, TaskID, Timestamp);
        Other ->
            Other
    end.

%% TODO - VFS-5531
-spec cancel(ctx(), traverse:pool(), traverse:id(), traverse:environment_id()) -> ok | {error, term()}.
cancel(ExtendedCtx, Pool, TaskID, Self) ->
    Diff = fun
        (#traverse_task{status = scheduled, canceled = false, executor = Executor, creator = Creator} = Task) when
            Executor =:= Self, Creator =:= Self ->
            {ok, Task#traverse_task{status = canceled, canceled = true, enqueued = false}};
        (#traverse_task{status = scheduled, canceled = false, executor = Executor} = Task) when Executor =:= Self ->
            {ok, Task#traverse_task{status = canceled, canceled = true}};
        (#traverse_task{status = Status, canceled = false} = Task) when Status =/= finished ->
            {ok, Task#traverse_task{canceled = true}};
        (_) ->
            {error, already_canceled}
    end,
    case datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskID), Diff) of
        {ok, #document{value = #traverse_task{
            timestamp = Timestamp,
            executor = Executor,
            creator = Creator,
            group = GroupID,
            status = Status
        }}} ->
            case {Status, Self} of
                {canceled, Executor} ->
                    ok = traverse_task_list:add_link(ExtendedCtx, Pool, ended, Executor, TaskID, Timestamp);
                _ ->
                    ok
            end,

            case {Status, Self} of
                {canceled, Creator} ->
                    ok = traverse_task_list:delete_scheduled_link(
                        ExtendedCtx, Pool, Creator, TaskID, Timestamp, GroupID, Executor);
                _ ->
                    ok
            end;
        {error, already_canceled} ->
            ok;
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates information when task is processed on different environment.
%% @end
%% TODO - VFS-5530
%%--------------------------------------------------------------------
-spec on_task_change(ctx(), traverse:pool(), traverse:id(), doc(), traverse:environment_id()) -> ok.
on_task_change(ExtendedCtx, Pool, TaskID, #document{key = DocID, value = #traverse_task{
    status = scheduled,
    canceled = true,
    executor = Self}
} = Doc, Self) ->
    % TODO - konflikty na dokumentach
    % TODO - zabezpieczyc jak ktos scancelowal zdalnie i lokalnie
    % TODO - wywolac callback o cancelowaniu z taskID (bedzie potrzebny taskID w callbacku jobu) - inny task na koniec a inny na zlecenie cancelacji?
    % uporzadkowac API czy job ma w sobie przechowywac czy wszystkie callbacki dostaja
    Diff = fun
        (#traverse_task{status = scheduled} = Task) ->
            {ok, Task#traverse_task{status = canceled}};
        (ChangedRecord) ->
            {error, {update_not_needed, ChangedRecord}}
    end,

    UpdatedDoc = case datastore_model:update(ExtendedCtx, DocID, Diff) of
        {ok, #document{value = #traverse_task{
            timestamp = Timestamp,
            executor = Executor
        }} = NewDoc} ->
            ok = traverse_task_list:add_link(ExtendedCtx, Pool, ended, Executor, TaskID, Timestamp),
            NewDoc;
        {error, {update_not_needed, Changed}} ->
            Doc#document{value = Changed}
    end,
    on_task_change(ExtendedCtx, Pool, TaskID, UpdatedDoc, Self);
on_task_change(ExtendedCtx, Pool, TaskID, #document{key = DocID, value = #traverse_task{
    status = Status,
    enqueued = true,
    creator = Self}
}, Self) when Status =/= scheduled ->
    Diff = fun
        (#traverse_task{enqueued = true} = Task) ->
            {ok, Task#traverse_task{enqueued = false}};
        (_) ->
            {error, update_not_needed}
    end,

    case datastore_model:update(ExtendedCtx, DocID, Diff) of
        {ok, #document{value = #traverse_task{
            timestamp = Timestamp,
            executor = Executor,
            creator = Creator,
            group = GroupID
        }}} ->
            ok = traverse_task_list:delete_scheduled_link(
                ExtendedCtx, Pool, Creator, TaskID, Timestamp, GroupID, Executor);
        {error, update_not_needed} ->
            ok
    end;
on_task_change(_ExtendedCtx, _Pool, _TaskID, _Doc, _Self) ->
    ok.

%%%===================================================================
%%% Setters and getters API
%%%===================================================================

-spec update_description(ctx(), traverse:pool(), traverse:id(), traverse:description()) ->
    {ok, traverse:description(), boolean()} | {error, term()}.
update_description(ExtendedCtx, Pool, TaskID, NewDescription) ->
    Diff = fun(#traverse_task{description = Description} = Task) ->
        FinalDescription = maps:fold(fun(K, V, Acc) ->
            Acc#{K => V + maps:get(K, Description, 0)}
        end, Description, NewDescription),
        {ok, Task#traverse_task{description = FinalDescription}}
    end,
    case datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskID), Diff) of
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
update_status(ExtendedCtx, Pool, TaskID, NewStatus) ->
    Diff = fun(Task) ->
        {ok, Task#traverse_task{status = NewStatus}}
    end,
    datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskID), Diff).

%%--------------------------------------------------------------------
%% @doc
%% Fix task description after reboot clearing information about delegated and not finished tasks.
%% @end
%%--------------------------------------------------------------------
-spec fix_description(ctx(), traverse:pool(), traverse:id()) -> {ok, doc()} | {error, term()}.
fix_description(ExtendedCtx, Pool, TaskID) ->
    Node = node(),
    Diff = fun
        (#traverse_task{
            node = TaskNode,
            description = Description} = Task
        ) when TaskNode =:= Node ->
            MDone = maps:get(master_jobs_done, Description, 0),
            MFailed = maps:get(master_jobs_failed, Description, 0),

            SDone = maps:get(slave_jobs_done, Description, 0),
            SFailed = maps:get(slave_jobs_failed, Description, 0),

            FinalDescription = Description#{
                master_jobs_delegated => MDone + MFailed,
                slave_jobs_delegated => SDone + SFailed
            },
            {ok, Task#traverse_task{description = FinalDescription}};
        (_) ->
            {error, other_node}
    end,

    datastore_model:update(ExtendedCtx, ?DOC_ID(Pool, TaskID), Diff).

-spec get(traverse:pool(), traverse:id()) -> {ok, doc()} | {error, term()}.
get(Pool, TaskID) ->
    datastore_model:get(?CTX, ?DOC_ID(Pool, TaskID)).

-spec is_enqueued(doc()) -> boolean().
is_enqueued(#document{value = #traverse_task{
    enqueued = Enqueued,
    canceled = Canceled
}}) ->
    Enqueued and not Canceled.

-spec get_execution_info(doc()) -> {ok, traverse:callback_module(), traverse:environment_id(), traverse:job_id()}.
get_execution_info(#document{value = #traverse_task{
    callback_module = CallbackModule,
    executor = Executor,
    main_job_id = MainJobID
}}) ->
    {ok, CallbackModule, Executor, MainJobID}.

-spec get_execution_info(traverse:pool(), traverse:id()) -> {ok, traverse:callback_module(),
    traverse:environment_id(), traverse:job_id()} | {error, term()}.
get_execution_info(Pool, TaskID) ->
    case datastore_model:get(?CTX, ?DOC_ID(Pool, TaskID)) of
        {ok, Doc} ->
            get_execution_info(Doc);
        Other ->
            Other
    end.

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns basin model's context. It can be extended using callback and such extended context is provided
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
        {pool, string},
        {callback_module, atom},
        {creator, string},
        {executor, string},
        {group, string},
        {timestamp, integer},
        {main_job_id, string},
        {enqueued, boolean},
        {canceled, boolean},
        {node, atom},
        {status, atom},
        {description, {custom, {?MODULE, encode_description, decode_description}}}
    ]}.

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
        true ->
            CallbackModule:get_timestamp();
        _ ->
            {ok, time_utils:system_time_millis()}
    end.