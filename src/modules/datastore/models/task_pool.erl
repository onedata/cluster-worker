%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Information about tasks to be done.
%%% @end
%%%-------------------------------------------------------------------
-module(task_pool).
-author("Michał Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").
-include("elements/task_manager/task_manager.hrl").

%% API
-export([create/2, update/3, delete/2]).
-export([list/1, list_failed/1, count_tasks/3]).

%% datastore_model callbacks
-export([get_record_struct/1]).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type record() :: #task_pool{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type level() :: task_manager:level().

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates task at provided level of certainty.
%% @end
%%--------------------------------------------------------------------
-spec create(level(), doc()) -> {ok, non | key()} | {error, term()}.
create(?NON_LEVEL, _Doc) ->
    {ok, non};
create(Level, Doc) ->
    case datastore_model:create(level_to_ctx(Level), Doc) of
        {ok, #document{key = Key}} -> {ok, Key};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Updates task at provided level of certainty.
%% @end
%%--------------------------------------------------------------------
-spec update(level(), key(), diff()) -> {ok, non | key()} | {error, term()}.
update(?NON_LEVEL, _Key, _Diff) ->
    {ok, non};
update(Level, Key, Diff) ->
    case datastore_model:update(level_to_ctx(Level), Key, Diff) of
        {ok, #document{key = Key}} -> {ok, Key};
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes task at provided level of certainty.
%% @end
%%--------------------------------------------------------------------
-spec delete(level(), key()) -> ok | {error, term()}.
delete(?NON_LEVEL, _Key) ->
    ok;
delete(Level, Key) ->
    datastore_model:delete(level_to_ctx(Level), Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all tasks at provided level of certainty.
%% @end
%%--------------------------------------------------------------------
-spec list(level()) -> {ok, [doc()]} | {error, term()}.
list(?NON_LEVEL) ->
    {ok, []};
list(Level) ->
    datastore_model:fold(level_to_ctx(Level), fun(Doc, Acc) ->
        {ok, [Doc | Acc]}
    end, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of failed tasks at provided level of certainty.
%% @end
%%--------------------------------------------------------------------
-spec list_failed(level()) -> {ok, [doc()]} | {error, term()}.
list_failed(?NON_LEVEL) ->
    {ok, []};
list_failed(Level = ?NODE_LEVEL) ->
    datastore_model:fold(level_to_ctx(Level), fun
        (Doc = #document{value = Task}, Acc) ->
            RemoteTask = Task#task_pool.node =/= node(),
            OwnerAlive = task_manager:check_owner(Task#task_pool.owner),
            case RemoteTask orelse OwnerAlive of
                true -> {ok, Acc};
                false -> {ok, [Doc | Acc]}
            end
    end, []);
list_failed(Level) ->
    datastore_model:fold(level_to_ctx(Level), fun
        (Doc = #document{value = Task}, Acc) ->
            case task_manager:is_task_alive(Task) of
                true -> {ok, Acc};
                false -> {ok, [Doc | Acc]}
            end
    end, []).

%%--------------------------------------------------------------------
%% @doc
%% Counts alive and failed tasks. Stops counting after specified limit.
%% @end
%%--------------------------------------------------------------------
-spec count_tasks(level(), atom(), non_neg_integer()) ->
    {ok, {Failed :: non_neg_integer(), All :: non_neg_integer()}} |
    {error, term()}.
count_tasks(Level, Type, Limit) ->
    datastore_model:fold(level_to_ctx(Level), fun
        (#document{value = Task}, {Failed, All}) ->
            case Task#task_pool.task_type == Type of
                true ->
                    Failed2 = case is_failed(Level, Task) of
                        true -> Failed + 1;
                        false -> Failed
                    end,
                    All2 = All + 1,
                    case All2 >= Limit of
                        true -> {stop, {Failed2, All2}};
                        false -> {ok, {Failed2, All2}}
                    end;
                false ->
                    {ok, {Failed, All}}
            end
    end, {0, 0}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Translates task level to appropriate operation context.
%% @end
%%--------------------------------------------------------------------
-spec level_to_ctx(level()) -> ctx().
level_to_ctx(?NODE_LEVEL) ->
    ?CTX#{routing => local, disc_driver => undefined};
level_to_ctx(?CLUSTER_LEVEL) ->
    ?CTX#{disc_driver => undefined};
level_to_ctx(?PERSISTENT_LEVEL) ->
    ?CTX#{memory_driver => undefined}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Check whether task failed.
%% @end
%%--------------------------------------------------------------------
-spec is_failed(level(), record()) -> boolean().
is_failed(?NODE_LEVEL, Task) ->
    LocalTask = node() == Task#task_pool.node,
    OwnerAlive = task_manager:check_owner(Task#task_pool.owner),
    LocalTask andalso not OwnerAlive;
is_failed(_, Task) ->
    not task_manager:is_task_alive(Task).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of model in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {task, term},
        {task_type, atom},
        {owner, term},
        {node, atom}
    ]}.