%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2025 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Logging utilities for GraphSync.
%%% By default, verbose logs are disabled. Using a set of env variables
%%% (see the macros), the admin may control the verbosity of the logs
%%% and the target severity.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_verbose_logger).
-author("Lukasz Opiola").


-include("graph_sync/graph_sync.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/logging.hrl").


%% API
-export([report_handshake_success/4, report_handshake_failure/4]).
-export([report_heartbeat/1, report_connection_terminated/1]).
-export([report_throttling_applied/2, report_throttling_ceased/2, report_throttling_delay_enforced/3]).
-export([report_cannot_decode_request/5]).

-export([report_request_received/3]).
-export([report_job_finished/4]).
-export([report_reply_sent/4]).
-export([report_stale_request_pruned/4]).

-export([report_auth_override_success/5, report_auth_override_error/5]).

-export([report_message_pushed/2]).


% severities of logs that can occur in this module; subject
% to env variables that may apply a mask (or disable them completely)
-type severity() :: info | notice | warning | error.


-define(ENV(Name, Default), cluster_worker:get_env(Name, Default)).
%% @see cluster_worker.app.src for descriptions
-define(SEVERITY_MASK, ?ENV(graph_sync_verbose_logs_severity, none)).
-define(IDENTITY_FILTER, ?ENV(graph_sync_verbose_logs_identity_filter, undefined)).
-define(PRINT_CREDENTIALS, ?ENV(graph_sync_verbose_logs_print_credentials, false)).
-define(PRINT_ERRORS, ?ENV(graph_sync_verbose_logs_print_errors, false)).
-define(PRINT_DATA, ?ENV(graph_sync_verbose_logs_print_data, false)).


%%%===================================================================
%%% API
%%%===================================================================


-spec report_handshake_success(
    gs_protocol:client_auth(),
    ip_utils:ip(),
    gs_protocol:cookies(),
    gs_session:data()
) ->
    ok.
report_handshake_success(ClientAuth, PeerIp, Cookies, SessionData) ->
    Identity = SessionData#gs_session.auth#auth.subject,
    dispatch_log(notice, SessionData, fun() ->
        str_utils:format("HANDSHAKE SUCCESS  [~ts] [~ts] [~ts] [~ts] [~ts] [~ts] [~ts]", [
            format_ip(PeerIp),
            format_identity(Identity),
            format_session_id(SessionData#gs_session.id),
            format_version(SessionData#gs_session.protocol_version),
            format_client_auth(ClientAuth),
            format_cookies(Cookies),
            format_conn_ref(SessionData#gs_session.conn_ref)
        ])
    end).


-spec report_handshake_failure(
    gs_protocol:client_auth(),
    ip_utils:ip(),
    gs_protocol:cookies(),
    errors:error()
) ->
    ok.
report_handshake_failure(ClientAuth, PeerIp, Cookies, HandshakeError) ->
    dispatch_log(error, #subject{type = nobody}, fun() ->
        str_utils:format("HANDSHAKE FAILURE  [~ts] [~ts] [~ts] SENDING REPLY: ~ts~ts", [
            format_ip(PeerIp),
            format_client_auth(ClientAuth),
            format_cookies(Cookies),
            format_error(HandshakeError),
            maybe_format_error_dump(HandshakeError)
        ])
    end).


-spec report_heartbeat(gs_session:data()) -> ok.
report_heartbeat(SessionData) ->
    dispatch_log(info, SessionData, fun() ->
        str_utils:format("HEARTBEAT RECEIVED [~ts] [~ts] [~ts] [~ts]", [
            format_ip(SessionData#gs_session.auth#auth.peer_ip),
            format_identity(SessionData),
            format_session_id(SessionData#gs_session.id),
            format_conn_ref(SessionData#gs_session.conn_ref)
        ])
    end).


-spec report_throttling_applied(gs_session:data(), non_neg_integer()) -> ok.
report_throttling_applied(SessionData, QueueSize) ->
    dispatch_log(warning, SessionData, fun() ->
        str_utils:format("THROTTLING APPLIED [~ts] [~ts] [~ts] [~ts] (queue size: ~B)", [
            format_ip(SessionData#gs_session.auth#auth.peer_ip),
            format_identity(SessionData),
            format_session_id(SessionData#gs_session.id),
            format_conn_ref(SessionData#gs_session.conn_ref),
            QueueSize
        ])
    end).


-spec report_throttling_ceased(gs_session:data(), non_neg_integer()) -> ok.
report_throttling_ceased(SessionData, QueueSize) ->
    dispatch_log(info, SessionData, fun() ->
        str_utils:format("THROTTLING CEASED  [~ts] [~ts] [~ts] [~ts] (queue size: ~B)", [
            format_ip(SessionData#gs_session.auth#auth.peer_ip),
            format_identity(SessionData),
            format_session_id(SessionData#gs_session.id),
            format_conn_ref(SessionData#gs_session.conn_ref),
            QueueSize
        ])
    end).


-spec report_throttling_delay_enforced(gs_session:data(), non_neg_integer(), non_neg_integer()) -> ok.
report_throttling_delay_enforced(SessionData, QueueSize, Delay) ->
    dispatch_log(warning, SessionData, fun() ->
        str_utils:format("TH. DELAY ENFORCED [~ts] [~ts] [~ts] [~ts] (queue size: ~B): ~B ms", [
            format_ip(SessionData#gs_session.auth#auth.peer_ip),
            format_identity(SessionData),
            format_session_id(SessionData#gs_session.id),
            format_conn_ref(SessionData#gs_session.conn_ref),
            QueueSize,
            Delay
        ])
    end).


-spec report_connection_terminated(gs_session:data()) -> ok.
report_connection_terminated(SessionData) ->
    dispatch_log(warning, SessionData, fun() ->
        str_utils:format("CONNCTN TERMINATED [~ts] [~ts] [~ts] [~ts]", [
            format_ip(SessionData#gs_session.auth#auth.peer_ip),
            format_identity(SessionData),
            format_session_id(SessionData#gs_session.id),
            format_conn_ref(SessionData#gs_session.conn_ref)
        ])
    end).


-spec report_cannot_decode_request(gs_session:data(), binary(), atom(), term(), stacktrace()) -> ok.
report_cannot_decode_request(SessionData, Data, Class, Reason, Stacktrace) ->
    dispatch_log(error, SessionData, fun() ->
        str_utils:format(
            "CANNOT DECODE REQ  [~ts] [~ts] [~ts] [~ts]~n"
            "> Stacktrace:~ts~n"
            "> Class: ~ts~n"
            "> Reason: ~tp"
            "~ts",
            [
                format_ip(SessionData#gs_session.auth#auth.peer_ip),
                format_identity(SessionData),
                format_session_id(SessionData#gs_session.id),
                format_conn_ref(SessionData#gs_session.conn_ref),
                lager:pr_stacktrace(Stacktrace),
                Class,
                Reason,
                maybe_format_data_dump(Data)
            ]
        )
    end).


-spec report_request_received(gs_session:data(), gs_protocol:message_id(), gs_protocol:req_wrapper()) -> ok.
report_request_received(SessionData, RequestId, RequestWrapper) ->
    dispatch_log(info, SessionData, fun() ->
        str_utils:format("[~ts] [~ts] [~ts] [~ts] ~ts", [
            format_request_id(RequestId),
            format_ip(SessionData#gs_session.auth#auth.peer_ip),
            format_identity(SessionData),
            format_session_id(SessionData#gs_session.id),
            format_request(RequestWrapper)
        ])
    end).


-spec report_job_finished(
    gs_session:data(),
    gs_protocol:message_id(),
    gs_protocol:resp_wrapper(),
    stopwatch:instance()
) ->
    ok.
report_job_finished(SessionData, RequestId, ResponseWrapper, Stopwatch) ->
    Severity = case ResponseWrapper#gs_resp.success of
        true -> info;
        false -> error
    end,
    dispatch_log(Severity, SessionData, fun() ->
        str_utils:format("[~ts] (~ts) JOB FINISHED: ~ts", [
            format_request_id(RequestId),
            format_time_elapsed(Stopwatch),
            case ResponseWrapper#gs_resp.success of
                true ->
                    <<"OK ", (maybe_format_data_dump(ResponseWrapper))/binary>>;
                false ->
                    Error = ResponseWrapper#gs_resp.error,
                    <<(format_error(Error))/binary, (maybe_format_error_dump(Error))/binary>>
            end
        ])
    end).


-spec report_reply_sent(
    gs_session:data(),
    gs_protocol:message_id(),
    gs_protocol:resp_wrapper(),
    stopwatch:instance()
) ->
    ok.
report_reply_sent(SessionData, RequestId, ResponseWrapper, Stopwatch) ->
    dispatch_log(notice, SessionData, fun() ->
        str_utils:format("[~ts] (~ts) SENDING REPLY: ~ts", [
            format_request_id(RequestId),
            format_time_elapsed(Stopwatch),
            case ResponseWrapper#gs_resp.success of
                true ->
                    <<"OK ", (maybe_format_data_dump(ResponseWrapper))/binary>>;
                false ->
                    Error = ResponseWrapper#gs_resp.error,
                    <<(format_error(Error))/binary, (maybe_format_error_dump(Error))/binary>>
            end
        ])
    end).


-spec report_stale_request_pruned(
    gs_session:data(),
    gs_protocol:message_id(),
    errors:error(),
    stopwatch:instance()
) ->
    ok.
report_stale_request_pruned(SessionData, RequestId, TimeoutError, Stopwatch) ->
    dispatch_log(error, SessionData, fun() ->
        str_utils:format("[~ts] (~ts) STALE, SENDING REPLY: ~ts~ts", [
            format_request_id(RequestId),
            format_time_elapsed(Stopwatch),
            format_error(TimeoutError),
            maybe_format_error_dump(TimeoutError)
        ])
    end).


-spec report_auth_override_success(
    gs_session:data(),
    gs_protocol:message_id(),
    gs_protocol:auth_override(),
    aai:auth(),
    stopwatch:instance()
) ->
    ok.
report_auth_override_success(SessionData, RequestId, AuthOverride, OverriddenAuth, Stopwatch) ->
    dispatch_log(info, SessionData, fun() ->
        str_utils:format("[~ts] (~ts) AUTH OVERRIDE SUCCESS ~ts: ~ts", [
            format_request_id(RequestId),
            format_time_elapsed(Stopwatch),
            format_auth_override(AuthOverride),
            format_identity(OverriddenAuth#auth.subject)
        ])
    end).


-spec report_auth_override_error(
    gs_session:data(),
    gs_protocol:message_id(),
    gs_protocol:auth_override(),
    errors:error(),
    stopwatch:instance()
) ->
    ok.
report_auth_override_error(SessionData, RequestId, AuthOverride, Error, Stopwatch) ->
    dispatch_log(error, SessionData, fun() ->
        str_utils:format("[~ts] (~ts) AUTH OVERRIDE FAILURE ~ts: ~ts~ts", [
            format_request_id(RequestId),
            format_time_elapsed(Stopwatch),
            format_auth_override(AuthOverride),
            format_error(Error),
            maybe_format_error_dump(Error)
        ])
    end).


-spec report_message_pushed(gs_session:data(), gs_protocol:push_wrapper()) -> ok.
report_message_pushed(SessionData, PushWrapper) ->
    dispatch_log(notice, SessionData, fun() ->
        str_utils:format("SENDING PUSH MESSG [~ts] [~ts] [~ts] ~ts", [
            format_ip(SessionData#gs_session.auth#auth.peer_ip),
            format_identity(SessionData),
            format_session_id(SessionData#gs_session.id),
            format_push_message(PushWrapper)
        ])
    end).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec dispatch_log(severity(), gs_session:data() | aai:subject(), fun(() -> string())) -> ok.
dispatch_log(Severity, #gs_session{auth = #auth{subject = Identity}}, LogFun) ->
    dispatch_log(Severity, Identity, LogFun);
dispatch_log(Severity, Identity, LogFun) ->
    case ?SEVERITY_MASK of
        none ->
            ok;
        _ ->
            case identity_matches_filter(Identity) of
                false ->
                    ok;
                true ->
                    case {?SEVERITY_MASK, Severity} of
                        {debug, _} -> ?debug(LogFun());
                        {regular, info} -> ?info(LogFun());
                        {regular, notice} -> ?notice(LogFun());
                        {regular, warning} -> ?warning(LogFun());
                        {regular, error} -> ?error(LogFun())
                    end
            end
    end.


%% @private
-spec identity_matches_filter(aai:subject()) -> boolean().
identity_matches_filter(Subject) ->
    case ?IDENTITY_FILTER of
        undefined ->
            true;
        AllowedSubjectsAsBinaries ->
            lists:member(format_identity(Subject), AllowedSubjectsAsBinaries)
    end.


%% @private
-spec format_ip(ip_utils:ip() | undefined) -> binary().
format_ip(undefined) ->
    <<"???.???.???.???">>;
format_ip(Ip) ->
    {ok, IpAsBinary} = ip_utils:to_binary(Ip),
    str_utils:pad_right(IpAsBinary, 15, <<" ">>).


%% @private
-spec format_identity(gs_session:data() | aai:subject()) -> binary().
format_identity(#gs_session{auth = #auth{subject = Identity}}) ->
    format_identity(Identity);
format_identity(Identity) ->
    aai:serialize_subject(Identity).


%% @private
-spec format_session_id(gs_protocol:session_id()) -> binary().
format_session_id(SessionId) ->
    str_utils:format_bin("sid:~ts", [SessionId]).


%% @private
-spec format_version(gs_protocol:protocol_version()) -> binary().
format_version(ProtocolVersion) ->
    str_utils:format_bin("vsn:~B", [ProtocolVersion]).


%% @private
-spec format_client_auth(gs_protocol:client_auth()) -> binary().
format_client_auth(ClientAuth) ->
    str_utils:format_bin("auth:~ts", [case ClientAuth of
        undefined -> <<"undefined">>;
        nobody -> <<"nobody">>;
        {token, Token} -> format_token(<<"token">>, Token)
    end]).


%% @private
-spec format_token(binary(), undefined | tokens:serialized()) -> binary().
format_token(Label, undefined) ->
    format_token(Label, <<"undefined">>);
format_token(Label, Token) ->
    case ?PRINT_CREDENTIALS of
        true -> <<Label/binary, "=", Token/binary>>;
        false -> <<Label/binary, "=*****">>
    end.


%% @private
-spec format_cookies(gs_protocol:cookies()) -> binary().
format_cookies(Cookies) ->
    str_utils:format_bin("cookies:~ts", [case Cookies of
        [] ->
            <<"none">>;
        _ ->
            str_utils:join_binary(lists:map(fun({Key, Value}) ->
                case ?PRINT_CREDENTIALS of
                    true -> <<Key/binary, "=", Value/binary>>;
                    false -> <<Key/binary, "=*****">>
                end
            end, Cookies), <<",">>)
    end]).


%% @private
-spec format_conn_ref(gs_server:conn_ref()) -> binary().
format_conn_ref(ConnRef) ->
    str_utils:format_bin("pid:~tp", [ConnRef]).


%% @private
-spec format_request_id(gs_protocol:message_id()) -> binary().
format_request_id(RequestId) ->
    str_utils:format_bin("rid:~ts", [RequestId]).


%% @private
-spec format_time_elapsed(unknown | stopwatch:instance()) -> binary().
format_time_elapsed(unknown) ->
    <<"??.???.??? s">>;
format_time_elapsed(Stopwatch) ->
    TotalMicros = stopwatch:read_micros(Stopwatch),
    Secs = TotalMicros div 1000000,
    Millis = (TotalMicros rem 1000000) div 1000,
    Micros = TotalMicros rem 1000,
    str_utils:format_bin("~2.10.0b.~3.10.0b,~3.10.0b s", [Secs, Millis, Micros]).


%% @private
-spec format_error(errors:error()) -> binary().
format_error({error, Atom}) when is_atom(Atom) ->
    % TODO VFS-12637 covers deprecated errors, remove when not needed
    str_utils:format_bin("ERROR (~ts)", [Atom]);
format_error({error, #od_error{ctx = Ctx}} = Error) ->
    % this trick is needed to fool dialyzer, which emits a warning here
    % because it does not believe the ctx can be undefined
    % (and it can, but only in tests, due to a different trick...)
    ModuleLineSuffix = case utils:ensure_defined(Ctx, undefined) of
        #od_error_ctx{module = Module, line = Line} ->
            str_utils:format_bin(", ~ts:~B", [Module, Line]);
        undefined ->
            <<"">>
    end,
    str_utils:format_bin("ERROR (~ts~ts)", [
        case Error of
            ?ERR_INTERNAL_SERVER_ERROR(Ref) ->
                <<(?ERR_INTERNAL_SERVER_ERROR_ID)/binary, "{ref:", (str_utils:to_binary(Ref))/binary, "}">>;
            _ ->
                maps:get(<<"id">>, errors:to_json(Error))
        end,
        ModuleLineSuffix
    ]).


%% @private
-spec maybe_format_error_dump(errors:error()) -> binary().
maybe_format_error_dump(Error) ->
    case ?PRINT_ERRORS of
        false ->
            <<"">>;
        true ->
            <<"\n", (format_json_prettily(errors:to_json(Error)))/binary>>
    end.


%% @private
-spec maybe_format_data_dump(undefined | gs_protocol:resp_wrapper() | binary() | gs_protocol:data()) -> binary().
maybe_format_data_dump(undefined) ->
    <<"">>;
maybe_format_data_dump(#gs_resp{success = false, error = Error}) ->
    maybe_format_error_dump(Error);
maybe_format_data_dump(#gs_resp{response = #gs_resp_handshake{}}) ->
    <<"">>;
maybe_format_data_dump(#gs_resp{response = #gs_resp_rpc{result = undefined}}) ->
    <<"">>;
maybe_format_data_dump(#gs_resp{response = #gs_resp_rpc{result = Result}}) ->
    maybe_format_data_dump(Result);
maybe_format_data_dump(#gs_resp{response = #gs_resp_batch{responses = Responses}}) ->
    str_utils:format_bin("BATCH  ~ts (size:~B)", [
        case Responses of
            [] -> <<"[]">>;
            [First] -> <<"[", (maybe_format_data_dump(First))/binary, "]">>;
            [First | _] -> <<"[", (maybe_format_data_dump(First))/binary, ", ...]">>
        end,
        length(Responses)
    ]);
maybe_format_data_dump(#gs_resp{response = #gs_resp_graph{data = undefined}}) ->
    <<"">>;
maybe_format_data_dump(#gs_resp{response = #gs_resp_graph{data = Data}}) ->
    maybe_format_data_dump(Data);
maybe_format_data_dump(#gs_resp{response = #gs_resp_unsub{}}) ->
    <<"">>;
maybe_format_data_dump(Data) ->
    case ?PRINT_DATA of
        false ->
            <<"">>;
        true ->
            DataDump = case Data of
                Bin when is_binary(Bin) ->
                    try
                        maybe_format_data_dump(json_utils:decode(Bin))
                    catch _:_ ->
                        <<"base64(Data): ", (base64:encode(Bin))/binary>>
                    end;
                JsonTerm ->
                    format_json_prettily(JsonTerm)
            end,
            <<"\n", DataDump/binary>>
    end.


%% @private
-spec format_json_prettily(json_utils:json_term()) -> binary().
format_json_prettily(Json) ->
    jiffy:encode(Json, [pretty]).


%% @private
-spec format_request(gs_protocol:req_wrapper()) -> binary().
format_request(#gs_req{request = #gs_req_batch{requests = Requests}}) ->
    str_utils:format_bin("BATCH  ~ts (size:~B)", [
        case Requests of
            [] -> <<"[]">>;
            [First] -> <<"[", (format_request(First))/binary, "]">>;
            [First | _] -> <<"[", (format_request(First))/binary, ", ...]">>
        end,
        length(Requests)
    ]);
format_request(#gs_req{request = #gs_req_graph{
    operation = Operation,
    gri = Gri,
    data = Data,
    subscribe = Subscribe,
    auth_hint = AuthHint
}}) ->
    str_utils:format_bin("~ts ~ts~ts~ts", [
        case {Operation, Subscribe} of
            {create, false} -> <<"CREATE">>;
            {create, true} -> <<"CRT+SB">>;
            {get, false} -> <<"GET   ">>;
            {get, true} -> <<"GET+SB">>;
            {update, false} -> <<"UPDATE">>;
            {update, true} -> <<"UPD+SB">>;
            {delete, false} -> <<"DELETE">>;
            {delete, true} -> <<"DEL+SB">>
        end,
        gri:serialize(Gri),
        format_auth_hint_suffix(AuthHint),
        maybe_format_data_dump(Data)
    ]);
format_request(#gs_req{request = #gs_req_rpc{function = Function, args = Args}}) ->
    str_utils:format_bin("RPC    ~ts(~ts)", [
        Function,
        str_utils:join_binary(lists:map(fun(ArgName) ->
            <<ArgName/binary, "=***">>
        end, maps:keys(Args)), <<", ">>)
    ]);
format_request(#gs_req{request = #gs_req_unsub{gri = Gri}}) ->
    str_utils:format_bin("UNSUB  ~ts", [gri:serialize(Gri)]).


%% @private
-spec format_push_message(gs_protocol:push_wrapper()) -> binary().
format_push_message(#gs_push{message = #gs_push_error{error = Error}}) ->
    <<(format_error(Error))/binary, (maybe_format_error_dump(Error))/binary>>;
format_push_message(#gs_push{message = #gs_push_graph{gri = Gri, change_type = updated, data = Data}}) ->
    str_utils:format_bin("UPDTD ~ts~ts", [gri:serialize(Gri), maybe_format_data_dump(Data)]);
format_push_message(#gs_push{message = #gs_push_graph{gri = Gri, change_type = deleted}}) ->
    str_utils:format_bin("DELTD ~ts", [gri:serialize(Gri)]);
format_push_message(#gs_push{message = #gs_push_nosub{gri = Gri, reason = Reason, auth_hint = AuthHint}}) ->
    str_utils:format_bin("NOSUB ~ts (~ts)~ts", [
        gri:serialize(Gri),
        Reason,
        format_auth_hint_suffix(AuthHint)
    ]).


%% @private
-spec format_auth_override(gs_protocol:auth_override()) -> binary().
format_auth_override(AuthOverride) ->
    str_utils:format_bin("[~ts] [int:~ts] [dac-policy:~ts] [~ts] [~ts]", [
        format_ip(AuthOverride#auth_override.peer_ip),
        AuthOverride#auth_override.interface,
        case AuthOverride#auth_override.data_access_caveats_policy of
            allow_data_access_caveats -> <<"allow">>;
            disallow_data_access_caveats -> <<"disallow">>
        end,
        format_client_auth(AuthOverride#auth_override.client_auth),
        format_token(<<"consumer-token">>, AuthOverride#auth_override.consumer_token)
    ]).


%% @private
-spec format_auth_hint_suffix(gs_protocol:auth_hint()) -> binary().
format_auth_hint_suffix(AuthHint) ->
    case gs_protocol:auth_hint_to_nullable_binary(AuthHint) of
        null -> <<"">>;
        Bin -> <<" (", Bin/binary, ")">>
    end.
