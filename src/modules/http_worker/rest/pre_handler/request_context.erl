%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Operations on process dictionary, that manipulate on context of rest request.
%%% @end
%%%--------------------------------------------------------------------
-module(request_context).
-author("Tomasz Lichon").

%% API
-export([set_handler/1, get_handler/0, set_exception_handler/1, get_exception_handler/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Convenience function that stores the reference to handler module in
%% process dict.
%% @end
%%--------------------------------------------------------------------
-spec set_handler(Module :: atom()) -> term().
set_handler(Module) ->
    erlang:put(handler, Module).

%%--------------------------------------------------------------------
%% @doc
%% Convenience function that retrieves the reference to handler module from
%% process dict.
%% @end
%%--------------------------------------------------------------------
-spec get_handler() -> atom().
get_handler() ->
    erlang:get(handler).

%%--------------------------------------------------------------------
%% @doc
%% Convenience function that stores the error_translator fun in process dict.
%% @end
%%--------------------------------------------------------------------
-spec set_exception_handler(Delegation :: request_exception_handler:exception_handler()) -> term().
set_exception_handler(Delegation) ->
    erlang:put(exception_handler, Delegation).

%%--------------------------------------------------------------------
%% @doc
%% Convenience function that retrieves the error_translator fun from process
%% dict.
%% @end
%%--------------------------------------------------------------------
-spec get_exception_handler() -> request_exception_handler:exception_handler().
get_exception_handler() ->
    erlang:get(exception_handler).

%%%===================================================================
%%% Internal functions
%%%===================================================================