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

%% Lifecycle API
-export([create/10, start/4, finish/4, cancel/4, on_task_change/2]).
%%% Setters and getters API
-export([update_description/4, update_status/4, fix_description/3,
    get/2, get_info/1, get_info/2]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

%% code/decode description
-export([encode_description/1, decode_description/1]).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type record() :: #traverse_task{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([ctx/0, key/0, doc/0]).

-define(CTX, #{
    model => ?MODULE
}).

-define(ID(Pool, ID), <<Pool/binary, "###", ID/binary>>).

%%%===================================================================
%%% Lifecycle API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates task document.
%% @end
%%--------------------------------------------------------------------
-spec create(ctx(), traverse:pool(), traverse:callback_module(), key(), traverse:executor(), traverse:executor(),
    traverse:group(), traverse:job_id(), undefined | node(), traverse:description()) -> ok.
create(ExtendedCtx, Pool, CallbackModule, ID, Creator, Executor, GroupID, Job, Node, InitialDescription) ->
    {ok, Timestamp} = get_timestamp(CallbackModule),
    Value0 = #traverse_task{pool = Pool, callback_module = CallbackModule, creator = Creator, executor = Executor,
        group = GroupID, description = InitialDescription, timestamp = Timestamp, main_job_id = Job},

    Value = case Node of
        undefined -> Value0;
        _ -> Value0#traverse_task{status = ongoing, enqueued = false, node = Node}
    end,
    {ok, _} = datastore_model:create(ExtendedCtx, #document{key = ?ID(Pool, ID), value = Value}),

    case Node of
        undefined ->
            ok = traverse_task_list:add_scheduled_link(ExtendedCtx, Pool, Creator, ID, Timestamp, GroupID, Executor),
            ok = traverse_load_balance:register_group(Pool, GroupID);
        _ ->
            ok = traverse_task_list:add_link(ExtendedCtx, Pool, ongoing, Executor, ID, Timestamp)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates information about task's start.
%% @end
%%--------------------------------------------------------------------
-spec start(ctx(), traverse:pool(), key(), traverse:description()) -> ok | {error, term()}.
start(ExtendedCtx, Pool, ID, NewDescription) ->
    Node = node(),
    Diff = fun
               (#traverse_task{status = scheduled} = Task) ->
                   {ok, Task#traverse_task{status = ongoing, enqueued = false, node = Node, description = NewDescription}};
               (_) ->
                   {error, already_started}
           end,
    case datastore_model:update(ExtendedCtx, ?ID(Pool, ID), Diff) of
        {ok, #document{value = #traverse_task{timestamp = Timestamp, executor = Executor, creator = Creator, group = GroupID}}} ->
            ok = traverse_task_list:add_link(ExtendedCtx, Pool, ongoing, Executor, ID, Timestamp),

            case Creator =:= Executor of
                true ->
                    ok = traverse_task_list:delete_scheduled_link(ExtendedCtx, Pool, Creator, ID, Timestamp, GroupID, Executor);
                _ ->
                    ok
            end;
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Finishes task.
%% @end
%%--------------------------------------------------------------------
-spec finish(ctx(), traverse:pool(), key(), traverse:status()) -> ok | {error, term()}.
finish(ExtendedCtx, Pool, ID, FinalStatus) ->
    Diff = fun(Task) ->
        {ok, Task#traverse_task{status = FinalStatus}}
    end,
    case {datastore_model:update(ExtendedCtx, ?ID(Pool, ID), Diff), FinalStatus} of
        {{ok, _}, canceled} ->
            ok;
        {{ok, #document{value = #traverse_task{timestamp = Timestamp, executor = Executor}}}, _} ->
            ok = traverse_task_list:add_link(ExtendedCtx, Pool, ended, Executor, ID, Timestamp),
            ok = traverse_task_list:delete_link(ExtendedCtx, Pool, ongoing, Executor, ID, Timestamp);
        {Other, _} ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Cancels task.
%% @end
%%--------------------------------------------------------------------
-spec cancel(ctx(), traverse:pool(), key(), traverse:executor()) -> ok | {error, term()}.
cancel(ExtendedCtx, Pool, ID, Self) ->
    Diff = fun(Task) ->
        {ok, Task#traverse_task{canceled = true}}
    end,
    case datastore_model:update(ExtendedCtx, ?ID(Pool, ID), Diff) of
        {ok, #document{value = #traverse_task{timestamp = Timestamp, executor = Executor, creator = Creator, group = GroupID}}} ->
            case Self of
                Executor ->
                    ok = traverse_task_list:add_link(ExtendedCtx, Pool, ended, Executor, ID, Timestamp),
                    ok = traverse_task_list:delete_link(ExtendedCtx, Pool, ongoing, Executor, ID, Timestamp);
                _ ->
                    ok
            end,

            case Self of
                Creator ->
                    ok = traverse_task_list:delete_scheduled_link(ExtendedCtx, Pool, Creator, ID, Timestamp, GroupID, Executor);
                _ ->
                    ok
            end;
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates information when task is started by other executor.
%% @end
%%--------------------------------------------------------------------
-spec on_task_change(doc(), traverse:executor()) -> ok.
on_task_change(#document{value = #traverse_task{}}, _Self) ->
    ok.

%%%===================================================================
%%% Setters and getters API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Updates task description field.
%% @end
%%--------------------------------------------------------------------
-spec update_description(ctx(), traverse:pool(), key(), traverse:description()) ->
    {ok, traverse:description(), boolean()} | {error, term()}.
update_description(ExtendedCtx, Pool, ID, NweDescription) ->
    Diff = fun(#traverse_task{description = Description} = Task) ->
        FinalDescription = maps:fold(fun(K, V, Acc) ->
            Acc#{K => V + maps:get(K, Description, 0)}
        end, Description, NweDescription),
        {ok, Task#traverse_task{description = FinalDescription}}
    end,
    case datastore_model:update(ExtendedCtx, ?ID(Pool, ID), Diff) of
        {ok, #document{value = #traverse_task{description = UpdatedDescription,
            canceled = Canceled}}} ->
            {ok, UpdatedDescription, Canceled};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates task status field.
%% @end
%%--------------------------------------------------------------------
-spec update_status(ctx(), traverse:pool(), key(), traverse:status()) ->
    {ok, doc()} | {error, term()}.
update_status(ExtendedCtx, Pool, ID, NewStatus) ->
    Diff = fun(Task) ->
        {ok, Task#traverse_task{status = NewStatus}}
    end,
    datastore_model:update(ExtendedCtx, ?ID(Pool, ID), Diff).

%%--------------------------------------------------------------------
%% @doc
%% Fix task description after reboot.
%% @end
%%--------------------------------------------------------------------
-spec fix_description(ctx(), traverse:pool(), key()) -> {ok, doc()} | {error, term()}.
fix_description(ExtendedCtx, Pool, ID) ->
    Node = node(),
    Diff = fun
               (#traverse_task{node = TaskNode, description = Description} = Task) when TaskNode =:= Node ->
                   MDone = maps:get(master_jobs_done, Description, 0),
                   MFailed = maps:get(master_jobs_failed, Description, 0),

                   SDone = maps:get(slave_jobs_done, Description, 0),
                   SFailed = maps:get(slave_jobs_failed, Description, 0),

                   FinalDescription = Description#{master_jobs_delegated => MDone + MFailed,
                       slave_jobs_delegated => SDone + SFailed},
                   {ok, Task#traverse_task{description = FinalDescription}};
               (_) ->
                   {error, other_node}
           end,

    datastore_model:update(ExtendedCtx, ?ID(Pool, ID), Diff).

%%--------------------------------------------------------------------
%% @doc
%% Returns task.
%% @end
%%--------------------------------------------------------------------
-spec get(traverse:pool(), key()) -> {ok, doc()} | {error, term()}.
get(Pool, ID) ->
    datastore_model:get(?CTX, ?ID(Pool, ID)).

%%--------------------------------------------------------------------
%% @doc
%% Returns info about task.
%% @end
%%--------------------------------------------------------------------
-spec get_info(doc()) -> {ok, traverse:callback_module(),
    traverse:executor(), traverse:job_id(), boolean(), boolean()}.
get_info(#document{value = #traverse_task{callback_module = CallbackModule,
    executor = Executor, main_job_id = MainJobID, enqueued = Enqueued, canceled = Canceled}}) ->
    {ok, CallbackModule, Executor, MainJobID, Enqueued, Canceled}.

%%--------------------------------------------------------------------
%% @doc
%% Returns info about task.
%% @end
%%--------------------------------------------------------------------
-spec get_info(traverse:pool(), key()) -> {ok, traverse:callback_module(),
    traverse:executor(), traverse:job_id(), boolean(), boolean()} | {error, term()}.
get_info(Pool, ID) ->
    case datastore_model:get(?CTX, ?ID(Pool, ID)) of
        {ok, Doc} ->
            get_info(Doc);
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
%% Encodes description to JSON format.
%% @end
%%--------------------------------------------------------------------
-spec encode_description(traverse:description()) -> binary().
encode_description(Description) ->
    json_utils:encode(Description).

%%--------------------------------------------------------------------
%% @doc
%% Decodes description from JSON format.
%% @end
%%--------------------------------------------------------------------
-spec decode_description(binary()) -> traverse:description().
decode_description(Term) ->
    Map = json_utils:decode(Term),
    lists:foldl(fun({K, V}, Acc) ->
        Acc#{binary_to_atom(K, utf8) => V}
    end, #{}, maps:to_list(Map)).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_timestamp(traverse:callback_module()) -> {ok, traverse:timestamp()}.
get_timestamp(CallbackModule) ->
    try
        CallbackModule:get_timestamp()
    catch
        _:undef -> {ok, time_utils:system_time_millis()}
    end.