-module(basic_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0]).
-export([test1/1, test2/1, test3/1]).

all() -> [test1,test2,test3].

test1(_Config) ->
  1 = 1.

test2(_Config) ->
  1 = 3.

test3(_Config) ->
  1 = 1.