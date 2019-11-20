%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Tests of view_traverse framework.
%%% @end
%%%-------------------------------------------------------------------
-module(view_traverse_test_SUITE).
-author("Jakub Kudzia").

-include_lib("ctool/include/test/performance.hrl").

%% export for ct
-export([all/0, init_per_suite/1, end_per_suite/1]).

%% tests
-export([foo_test/1]).

all() -> ?ALL([foo_test]).

%%%===================================================================
%%% Test functions
%%%===================================================================

foo_test(_) -> ok.

%%%===================================================================
%%% Init/teardown functions
%%%===================================================================

init_per_suite(Config) ->
    datastore_test_utils:init_suite(Config).

end_per_suite(_Config) ->
    ok.

