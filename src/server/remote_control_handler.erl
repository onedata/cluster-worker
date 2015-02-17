%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is a cowboy handler and processes remote control requests.
%%% @end
%%%-------------------------------------------------------------------
-module(remote_control_handler).
-author("Lukasz Opiola").

-include_lib("ctool/include/logging.hrl").
-include("appmock_internal.hrl").

%% Cowboy API
-export([init/3, handle/2, terminate/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback, called to initialize the state of the handler.
%% @end
%%--------------------------------------------------------------------
-spec init(Type :: term(), Req :: cowboy_req:req(), Args :: term()) -> {ok, term(), Path :: string()}.
init(_Type, Req, Args) ->
    [Path] = Args,
    % The request state is it's path, so we can easily create cases for handle function.
    {ok, Req, Path}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback, called to process remote control requests.
%% It decodes a request, delegates the logic to remote_control_server and encodes the answer.
%% @end
%%--------------------------------------------------------------------
-spec handle(Req :: cowboy_req:req(), State :: term()) -> {ok, term(), State :: term()}.
handle(Req, ?VERIFY_REST_ENDPOINT_PATH = State) ->
    {ok, NewReq} =
        try
            % Unpack the request, getting a history list
            {ok, JSONBody, _} = cowboy_req:body(Req),
            Body = appmock_utils:decode_from_json(JSONBody),
            {Port, Path, Number} = ?VERIFY_REST_ENDPOINT_UNPACK_REQUEST(Body),
            % Verify the endpoint and return the result encoded to JSON.
            ReplyTerm = case remote_control_server:verify_rest_mock_endpoint(Port, Path, Number) of
                            ok ->
                                ?OK_RESULT;
                            {different, ActualNumber} ->
                                ?VERIFY_REST_ENDPOINT_PACK_ERROR(ActualNumber);
                            {error, wrong_endpoint} ->
                                ?VERIFY_REST_ENDPOINT_PACK_ERROR_WRONG_ENDPOINT
                        end,
            Req2 = cowboy_req:set_resp_body(appmock_utils:encode_to_json(ReplyTerm), Req),
            Req3 = gui_utils:cowboy_ensure_header(<<"content-type">>, <<"application/json">>, Req2),
            {ok, _NewReq} = cowboy_req:reply(200, Req3)
        catch T:M ->
            ?error_stacktrace("Error in remote_control_handler. Path: ~p. ~p:~p.",
                [State, T, M]),
            {ok, _ErrorReq} = cowboy_req:reply(500, Req)
        end,
    {ok, NewReq, State};

handle(Req, ?VERIFY_REST_HISTORY_PATH = State) ->
    {ok, NewReq} =
        try
            % Unpack the request, getting a history list
            {ok, JSONBody, _} = cowboy_req:body(Req),
            BodyStruct = appmock_utils:decode_from_json(JSONBody),
            History = ?VERIFY_REST_HISTORY_UNPACK_REQUEST(BodyStruct),
            % Verify the history and return the result encoded to JSON.
            ReplyTerm = case remote_control_server:verify_rest_mock_history(History) of
                            ok ->
                                ?OK_RESULT;
                            {different, ActualHistory} ->
                                ?VERIFY_REST_HISTORY_PACK_ERROR(ActualHistory)
                        end,
            Req2 = cowboy_req:set_resp_body(appmock_utils:encode_to_json(ReplyTerm), Req),
            Req3 = gui_utils:cowboy_ensure_header(<<"content-type">>, <<"application/json">>, Req2),
            {ok, _NewReq} = cowboy_req:reply(200, Req3)
        catch T:M ->
            ?error_stacktrace("Error in remote_control_handler. Path: ~p. ~p:~p.",
                [State, T, M]),
            {ok, _ErrorReq} = cowboy_req:reply(500, Req)
        end,
    {ok, NewReq, State};

handle(Req, ?NAGIOS_ENPOINT = State) ->
    {ok, NewReq} =
        try
            HealthcheckResponses = [
                rest_mock_server:healthcheck(),
                remote_control_server:healthcheck()
            ],

            AppStatus = case lists:duplicate(length(HealthcheckResponses), ok) of
                            HealthcheckResponses -> ok;
                            _ -> error
                        end,

            {{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(now()),
            DateString = gui_str:format("~4..0w/~2..0w/~2..0w ~2..0w:~2..0w:~2..0w", [YY, MM, DD, Hour, Min, Sec]),
            Healthdata = {healthdata, [{date, DateString}, {status, atom_to_list(AppStatus)}], []},
            Content = lists:flatten([Healthdata]),
            Export = xmerl:export_simple(Content, xmerl_xml),
            Reply = io_lib:format("~s", [lists:flatten(Export)]),

            % Send the reply
            {ok, Req2} = cowboy_req:reply(200, [{<<"content-type">>, <<"application/xml">>}], Reply, Req),
            {ok, Req2}
        catch T:M ->
            ?error_stacktrace("Error in remote_control_handler. Path: ~p. ~p:~p.",
                [State, T, M]),
            {ok, _ErrorReq} = cowboy_req:reply(500, Req)
        end,
    {ok, NewReq, State}.


%%--------------------------------------------------------------------
%% @doc
%% Cowboy callback, called to perform cleanup after the request is handled.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), Req :: cowboy_req:req(), State :: term()) -> ok.
terminate(_Reason, _Req, _State) ->
    ok.