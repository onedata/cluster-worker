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
-export([get/1, create/3, update_description/2,
    update_status/2, delete/1]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type ctx() :: datastore:ctx().
-type key() :: datastore:key().
-type record() :: #traverse_task{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([key/0]).

-define(TREE_ID, <<"main_tree">>).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true,
    local_links_tree_id => ?TREE_ID
}).

%%%===================================================================
%%% API
%%%===================================================================

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
%% Creates task document.
%% @end
%%--------------------------------------------------------------------
-spec create(key(), traverse:pool(), traverse:task_module()) -> ok | no_return().
create(ID, Pool, TaskModule) ->
    Doc = #document{
        key = ID,
        value = #traverse_task{task_module = TaskModule}
    },
    {ok, _} = datastore_model:create(?CTX, Doc),
    % TODO - taski sa grupowane, co ze zmianami statusu i load balancingiem?
    [{ok, _}] = datastore_model:add_links(?CTX,
        atom_to_binary(Pool, utf8), ?TREE_ID, [{ID, ID}]),
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
%% Deletes task.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).


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
        {value, {custom, {json_utils, encode, decode}}}
    ]}.