%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Functions delegating request to appripriate handlers.
%%% @end
%%%--------------------------------------------------------------------
-module(request_delegation).
-author("Tomasz Lichon").

-include_lib("ctool/include/logging.hrl").

%% API
-export([delegate/5]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Function used to delegate a cowboy callback.
%% If handler implements callback - it is called, otherwise the default
%% cowboy callback is used.
%% @end
%%--------------------------------------------------------------------
-spec delegate(cowboy_req:req(), term(), atom(), [term()],
  integer()) -> term().
delegate(Req, State, Fun, Args, Arity) ->
    ?info("~p", [rest_handler:module_info()]),
    case erlang:function_exported(request_context:get_handler(), Fun, Arity) of
        false ->
            no_call;
        true ->
            call_and_handle_exception(Req, State, Fun, Args)
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%%--------------------------------------------------------------------
-spec call_and_handle_exception(cowboy_req:req(), term(), atom(), [term()]) -> term().
call_and_handle_exception(Req, State, Fun, Args) ->
    try
        erlang:apply(request_context:get_handler(), Fun, Args)
    catch
        T:E ->
            erlang:apply(request_context:get_exception_handler(), [Req, State, T, E])
    end.
