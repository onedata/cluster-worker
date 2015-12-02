%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Common definions and configurations for datastore.
%%%      For cluster-worker internal use only.
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

-endif.
