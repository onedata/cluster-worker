%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc: This module handles DNS protocol requests (TCP and UDP) and allows using
%%% custom query handlers.
%%% @end
%%%-------------------------------------------------------------------
-module(dns_server).

-include_lib("ctool/include/logging.hrl").
-include_lib("kernel/src/inet_dns.hrl").
-include("global_definitions.hrl").
-include("timeouts.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

% Local name of the process waiting for dns udp messages
-define(DNS_UDP_LISTENER, dns_udp).

% Module name of dns tcp ranch listener
-define(DNS_TCP_LISTENER, dns_tcp).

% Maximum size of UDP DNS reply (without EDNS) as in RFC1035.
-define(NOEDNS_UDP_SIZE, 512).

%% API
-export([start/0, stop/0, handle_query/2, start_listening/0]).

% Functions useful in qury handler modules
-export([answer_record/4, authority_record/4, additional_record/4,
    authoritative_answer_flag/1, validate_query/1]).

% Server configuration (in runtime)
-export([set_max_edns_udp_size/1, get_max_edns_udp_size/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Starts a DNS server in a new process.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    % Listeners start is done in another process.
    % This is because this function is often called during the init of the supervisor process
    % that supervises the listeners - which causes a deadlock.
    case proc_lib:start(?MODULE, start_listening, [], ?LISTENERS_START_TIMEOUT) of
        ok -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc Stops the DNS server and cleans up.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.
stop() ->
    % Cleaning up must be done in another process. This is because this function is often called during the termination
    % of the supervisor process that supervises the listeners - which causes a deadlock.
    spawn(fun() -> clear_children_and_listeners() end),
    ok.

%%--------------------------------------------------------------------
%% @doc Handles a DNS request. This function is called from dns_upd_handler or dns_tcp_handler after
%% they have received a request. After evaluation, the response is sent back to the client.
%% @end
%%--------------------------------------------------------------------
-spec handle_query(Packet :: binary(), Transport :: udp | tcp) -> {ok, Packet :: binary()} | {error, term()}.
handle_query(Packet, Transport) ->
    case inet_dns:decode(Packet) of
        {ok, #dns_rec{qdlist = QDList, anlist = _AnList, nslist = _NSList, arlist = ARList} = DNSRecWithAdditionalSection} ->
            % Detach OPT RR from the DNS query record an proceed with processing it - the OPT RR will be added during answer generation
            DNSRec = DNSRecWithAdditionalSection#dns_rec{arlist = []},
            OPTRR = case ARList of
                [] -> undefined;
                [#dns_rr_opt{} = OptRR] -> OptRR
            end,
            try
                case validate_query(DNSRec) of
                    ok ->
                        [#dns_query{domain = Domain, type = Type, class = Class}] = QDList,
                        case call_worker(string:to_lower(Domain), Type) of
                            {error, _} ->
                                generate_answer(set_reply_code(DNSRec, serv_fail), OPTRR, Transport);
                            Reply when is_atom(Reply) -> % Reply :: reply_type()
                                generate_answer(set_reply_code(DNSRec, Reply), OPTRR, Transport);
                            {Reply, ResponseList} ->
                                DNSRecUpdatedHeader = set_reply_code(DNSRec, Reply),
                                %% If there was an OPT RR, it will be concated at the end of the process
                                NewRec = lists:foldl(
                                    fun(CurrentRecord, #dns_rec{header = CurrHeader, anlist = CurrAnList, nslist = CurrNSList, arlist = CurrArList} = CurrRec) ->
                                        case CurrentRecord of
                                            {aa, Flag} ->
                                                CurrRec#dns_rec{header = inet_dns:make_header(CurrHeader, aa, Flag)};
                                            {Section, CurrDomain, CurrTTL, CurrType, CurrData} ->
                                                RR = inet_dns:make_rr([
                                                    {data, CurrData},
                                                    {domain, CurrDomain},
                                                    {type, CurrType},
                                                    {ttl, CurrTTL},
                                                    {class, Class}]),
                                                case Section of
                                                    answer ->
                                                        CurrRec#dns_rec{anlist = CurrAnList ++ [RR]};
                                                    authority ->
                                                        CurrRec#dns_rec{nslist = CurrNSList ++ [RR]};
                                                    additional ->
                                                        CurrRec#dns_rec{arlist = CurrArList ++ [RR]}
                                                end
                                        end
                                    end, DNSRecUpdatedHeader, ResponseList),
                                generate_answer(NewRec#dns_rec{arlist = NewRec#dns_rec.arlist}, OPTRR, Transport)
                        end;
                    Error ->
                        generate_answer(set_reply_code(DNSRec, Error), OPTRR, Transport)
                end
            catch
                T:M ->
                    ?error_stacktrace("Error processing DNS request ~p:~p", [T, M]),
                    generate_answer(set_reply_code(DNSRec, serv_fail), OPTRR, Transport)
            end;
        _ ->
            {error, uprocessable}
    end.

%%--------------------------------------------------------------------
%% @doc Checks if a query is valid.
%% @end
%%--------------------------------------------------------------------
-spec validate_query(#dns_rec{}) -> ok | bad_version | form_error.
validate_query(DNSRec) ->
    case DNSRec of
        #dns_rec{qdlist = [#dns_query{class = Class}], anlist = [], nslist = [], arlist = []}
            when Class =:= ?C_IN orelse Class =:= in ->
            % The record includes a question section and no OPT RR section - ok
            ok;
        #dns_rec{qdlist = [#dns_query{class = Class}], anlist = [], nslist = [], arlist = [#dns_rr_opt{version = Version}]}
            when Class =:= ?C_IN orelse Class =:= in ->
            % The record includes a question section and an OPT RR section - check EDNS version
            case Version of
                0 -> ok;
                _ -> bad_version
            end;
        #dns_rec{} ->
            % Other - not ok
            form_error
    end.

%%--------------------------------------------------------------------
%% @doc Calls worker to handle the query or returns not_impl if
%% this kind of query is not accepted by the server.
%% @end
%%--------------------------------------------------------------------
-spec call_worker(Domain :: string(), Type :: atom()) ->
    dns_worker_plugin_behaviour:handler_reply() | {error, term()}.
call_worker(Domain, Type) ->
    case type_to_method(Type) of
        not_impl ->
            not_impl;
        Method ->
            dns_worker:resolve(Method, Domain)
    end.

%%--------------------------------------------------------------------
%% @doc Returns a method that should be passed to handle a query of given type.
%% Those methods are used in dns_worker_plugin_behaviour
%% and also are accepted by dns_worker API methods.
%% @end
%%--------------------------------------------------------------------
-spec type_to_method(QueryType :: atom()) -> atom().
type_to_method(?S_A) -> handle_a;
type_to_method(?S_NS) -> handle_ns;
type_to_method(?S_CNAME) -> handle_cname;
type_to_method(?S_SOA) -> handle_soa;
type_to_method(?S_WKS) -> handle_wks;
type_to_method(?S_PTR) -> handle_ptr;
type_to_method(?S_HINFO) -> handle_hinfo;
type_to_method(?S_MINFO) -> handle_minfo;
type_to_method(?S_MX) -> handle_mx;
type_to_method(?S_TXT) -> handle_txt;
type_to_method(_) -> not_impl.

%%--------------------------------------------------------------------
%% @doc Encodes a DNS record and returns a tuple accepted by dns_xxx_handler modules.
%% Modifies flags in header according to server's capabilities.
%% If there was an OPT RR record in request, it modifies it properly and concates to the ADDITIONAL section.
%% @end
%%--------------------------------------------------------------------
-spec generate_answer(DNSRec :: #dns_rec{}, OPTRR :: #dns_rr_opt{}, Transport :: atom()) -> {ok, binary()}.
generate_answer(DNSRec, OPTRR, Transport) ->
    % Update the header - no recursion available, qr=true -> it's a response
    Header2 = inet_dns:make_header(DNSRec#dns_rec.header, ra, false),
    NewHeader = inet_dns:make_header(Header2, qr, true),
    DNSRecUpdatedHeader = DNSRec#dns_rec{header = NewHeader},
    % Check if OPT RR is present. If so, retrieve client's max upd payload size and set the value to server's max udp.
    {NewDnsRec, ClientMaxUDP} = case OPTRR of
        undefined ->
            {DNSRecUpdatedHeader, undefined};
        #dns_rr_opt{udp_payload_size = ClMaxUDP} = RROPT ->
            NewRROPT = RROPT#dns_rr_opt{udp_payload_size = get_max_edns_udp_size()},
            {DNSRecUpdatedHeader#dns_rec{arlist = DNSRecUpdatedHeader#dns_rec.arlist ++ [NewRROPT]}, ClMaxUDP}
    end,
    case Transport of
        udp -> {ok, encode_udp(NewDnsRec, ClientMaxUDP)};
        tcp -> {ok, inet_dns:encode(NewDnsRec)}
    end.

%%--------------------------------------------------------------------
%% @doc Encodes a DNS record and returns a tuple accepted by dns_xxx_handler modules.
%% Modifies flags in header according to server's capabilities.
%% If there was an OPT RR record in request, it modifies it properly and concates to the ADDITIONAL section.
%% @end
%%--------------------------------------------------------------------
-spec set_reply_code(DNSRec :: #dns_rec{header :: #dns_header{}}, ReplyType :: dns_worker_plugin_behaviour:reply_code()) -> #dns_rec{}.
set_reply_code(#dns_rec{header = Header} = DNSRec, ReplyType) ->
    ReplyCode = case ReplyType of
        nx_domain -> ?NXDOMAIN;
        not_impl -> ?NOTIMP;
        refused -> ?REFUSED;
        form_error -> ?FORMERR;
        bad_version -> ?BADVERS;
        ok -> ?NOERROR;
        _ -> ?SERVFAIL
    end,
    DNSRec#dns_rec{header = inet_dns:make_header(Header, rcode, ReplyCode)}.

%%--------------------------------------------------------------------
%% @doc Encodes a DNS record and truncates it if required.
%% @end
%%--------------------------------------------------------------------
-spec encode_udp(DNSRec :: #dns_rec{}, ClientMaxUDP :: integer() | undefined) -> binary().
encode_udp(#dns_rec{} = DNSRec, ClientMaxUDP) ->
    TruncationSize = case ClientMaxUDP of
        undefined ->
            ?NOEDNS_UDP_SIZE;
        Value ->
            % If the client advertised a value, accept it but don't exceed the range [512, MAX_UDP_SIZE]
            max(?NOEDNS_UDP_SIZE, min(get_max_edns_udp_size(), Value))
    end,
    Packet = inet_dns:encode(DNSRec),
    case size(Packet) > TruncationSize of
        true ->
            NumBits = (8 * TruncationSize),
            % Truncate the packet
            <<TruncatedPacket:NumBits, _/binary>> = Packet,
            % Set the TC (truncation) flag, which is the 23th bit in header
            <<Head:22, _TC:1, LastBit:1, Tail/binary>> = <<TruncatedPacket:NumBits>>,
            NewPacket = <<Head:22, 1:1, LastBit:1, Tail/binary>>,
            NewPacket;
        false ->
            Packet
    end.

%%--------------------------------------------------------------------
%% @doc Starts dns listeners and terminates dns_worker process in case of error.
%% @end
%%--------------------------------------------------------------------
-spec start_listening() -> ok.
start_listening() ->
    try
        SupervisorName = ?CLUSTER_WORKER_APPLICATION_SUPERVISOR_NAME,
        {ok, DNSPort} = application:get_env(?CLUSTER_WORKER_APP_NAME, dns_port),
        {ok, TCPNumAcceptors} = application:get_env(
            ?CLUSTER_WORKER_APP_NAME, dns_tcp_acceptor_pool_size),
        {ok, TCPTimeout} = application:get_env(
            ?CLUSTER_WORKER_APP_NAME, dns_tcp_timeout_seconds),

        proc_lib:init_ack(ok),
        UDPChild = {?DNS_UDP_LISTENER, {dns_udp_handler, start_link, [DNSPort]}, permanent, 5000, worker, [dns_udp_handler]},
        TCPOptions = [{packet, 2}, {dns_tcp_timeout, TCPTimeout}, {keepalive, true}],

        % Start the UDP listener
        {ok, Pid} = supervisor:start_child(SupervisorName, UDPChild),
        % Start the TCP listener. In case of an error, stop the UDP listener
        try
            {ok, _} = ranch:start_listener(?DNS_TCP_LISTENER, TCPNumAcceptors, ranch_tcp, [{port, DNSPort}],
                dns_tcp_handler, TCPOptions)
        catch
            _:RanchError ->
                supervisor:terminate_child(SupervisorName, Pid),
                supervisor:delete_child(SupervisorName, Pid),
                ?error("Error while starting DNS TCP, ~p", [RanchError]),
                throw(RanchError)
        end,
        ?info("DNS server started successfully.")
    catch
        _:Reason ->
            ?error("DNS Error during starting listeners, ~p", [Reason])
    end,
    ok.

%%--------------------------------------------------------------------
%% @doc Convenience function used from a dns query handler. Creates a term that will cause
%% the AA (authoritative answer) flag to be set to desired value in response.
%% The term should be put in list returned from handle_xxx function.
%% @end
%%--------------------------------------------------------------------
-spec authoritative_answer_flag(Flag) -> {aa, Flag} when
    Flag :: boolean().
authoritative_answer_flag(Flag) ->
    {aa, Flag}.

%%--------------------------------------------------------------------
%% @doc Convenience function used from a dns query handler. Creates a term that will end up
%% as a record in ANSWER section of DNS reponse.
%% The term should be put in list returned from handle_xxx function.
%% @end
%%--------------------------------------------------------------------
-spec answer_record(Domain, TTL, Type, Data) ->
    {answer, Domain, TTL, Type, Data} when
    Domain :: string(),
    TTL :: integer(),
    Type :: dns_worker_plugin_behaviour:query_type(),
    Data :: term().
answer_record(Domain, TTL, Type, Data) ->
    {answer, Domain, TTL, Type, Data}.

%%--------------------------------------------------------------------
%% @doc Convenience function used from a dns query handler. Creates a term that will end up
%% as a record in AUTHORITY section of DNS reponse.
%% The term should be put in list returned from handle_xxx function.
%% @end
%%--------------------------------------------------------------------
-spec authority_record(Domain, TTL, Type, Data) ->
    {authority, Domain, TTL, Type, Data} when
    Domain :: string(),
    TTL :: integer(),
    Type :: dns_worker_plugin_behaviour:query_type(),
    Data :: term().
authority_record(Domain, TTL, Type, Data) ->
    {authority, Domain, TTL, Type, Data}.

%%--------------------------------------------------------------------
%% @doc Convenience function used from a dns query handler. Creates a term that will end up
%% as a record in ADDITIONAL section of DNS reponse.
%% The term should be put in list returned from handle_xxx function.
%% @end
%%--------------------------------------------------------------------
-spec additional_record(Domain, TTL, Type, Data) ->
    {additional, Domain, TTL, Type, Data} when
    Domain :: string(),
    TTL :: integer(),
    Type :: dns_worker_plugin_behaviour:query_type(),
    Data :: term().
additional_record(Domain, TTL, Type, Data) ->
    {additional, Domain, TTL, Type, Data}.

%%--------------------------------------------------------------------
%% @doc Saves DNS response TTL in application env.
%% @end
%%--------------------------------------------------------------------
-spec set_max_edns_udp_size(Size :: integer()) -> ok.
set_max_edns_udp_size(Size) ->
    ok = application:set_env(?CLUSTER_WORKER_APP_NAME, edns_max_udp_size, Size).

%%--------------------------------------------------------------------
%% @doc Retrieves DNS response TTL from application env.
%% @end
%%--------------------------------------------------------------------
-spec get_max_edns_udp_size() -> integer().
get_max_edns_udp_size() ->
    {ok, EdnsMaxUdpSize} = application:get_env(?CLUSTER_WORKER_APP_NAME, edns_max_udp_size),
    EdnsMaxUdpSize.

%%--------------------------------------------------------------------
%% Internal functions

%% clear_children_and_listeners/1
%% @doc Terminates listeners and created children, if they exist.
%% @end
%%--------------------------------------------------------------------
-spec clear_children_and_listeners() -> ok.
clear_children_and_listeners() ->
    try
        SupervisorName = ?CLUSTER_WORKER_APPLICATION_SUPERVISOR_NAME,
        SupChildren = supervisor:which_children(SupervisorName),
        case lists:keyfind(?DNS_UDP_LISTENER, 1, SupChildren) of
            {?DNS_UDP_LISTENER, _, _, _} ->
                ok = supervisor:terminate_child(SupervisorName, ?DNS_UDP_LISTENER),
                supervisor:delete_child(SupervisorName, ?DNS_UDP_LISTENER),
                ?debug("DNS UDP child has exited");
            _ ->
                ok
        end
    catch
        _:Error1 ->
            ?error_stacktrace("Error stopping dns udp listener, status ~p", [Error1])
    end,

    try
        ok = ranch:stop_listener(?DNS_TCP_LISTENER),
        ?debug("dns_tcp_listener has exited")
    catch
        _:Error2 ->
            ?error_stacktrace("Error stopping dns tcp listener, status ~p", [Error2])
    end,
    ok.