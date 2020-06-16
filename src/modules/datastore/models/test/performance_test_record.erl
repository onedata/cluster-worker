%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model used for performance testing
%%% (mocked models cannot be used as they affect performance).
%%% @end
%%%-------------------------------------------------------------------
-module(performance_test_record).
-author("Michał Wrzeszcz").

-include("modules/datastore/datastore_models.hrl").

-export([get_ctx/0, get_memory_only_ctx/0, get_record_struct/1]).

-type ctx() :: datastore:ctx().

-define(CTX, #{
    model => ?MODULE
}).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

-spec get_ctx() -> ctx().
get_ctx() ->
    ?CTX.

-spec get_memory_only_ctx() -> ctx().
get_memory_only_ctx() ->
    ?CTX#{disc_driver => undefined}.

-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {value, string}
    ]}.
