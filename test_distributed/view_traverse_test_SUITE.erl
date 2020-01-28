%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Empty test SUITE to be implemented in the future major version.
%%% @end
%%%-------------------------------------------------------------------
-module(view_traverse_test_SUITE).
-author("Lukasz Opiola").

-include("datastore_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/test/assertions.hrl").
-include_lib("ctool/include/test/performance.hrl").

%% API
-export([all/0]).
-export([dummy_test/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

all() -> ?ALL([dummy_test]).

dummy_test(_Config) ->
    ok.