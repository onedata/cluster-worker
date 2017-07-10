%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Common definions and configurations for datastore.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_COMMON_HRL).
-define(DATASTORE_COMMON_HRL, 1).

%% Common predicates
-define(PRED_ALWAYS, fun() -> true end).


%% Utils
-define(RESPONSE(R), begin
                         {ok, Response} = R,
                         Response
                     end
).

%% Common funs
-define(GET_ALL,
    fun
        ('$end_of_table', Acc) ->
            {abort, Acc};
        (Obj, Acc) ->
            {next, [Obj | Acc]}
    end).

%% ETS name for local (node scope) state.
-define(LOCAL_STATE, datastore_local_state).

%% ETS for counters of changes' streams.
-define(CHANGES_COUNTERS, changes_countes).

%% ETS name for couchbase gateway details
-define(COUCHBASE_GATEWAYS, couchbase_gateways).

%% Name of datastore pool manager
-define(DATASTORE_POOL_MANAGER, datastore_pool).

-endif.
