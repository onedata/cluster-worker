%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module tests the functionality of dns_server, using eunit tests.
%%% @end
%%%-------------------------------------------------------------------
-module(dns_server_tests).

-ifdef(TEST).

-include_lib("kernel/src/inet_dns.hrl").
-include_lib("eunit/include/eunit.hrl").

% Creates a #dns_rec record with NumberOfRecords answers.
% 30 answer records = 513 Bytes after encoding.
-define(EXAMPLE_DNS_REC(NumberOfRecords),
    #dns_rec{header = #dns_header{id = 10000, qr = true,
        opcode = 'query', aa = false, tc = false, rd = true, ra = true,
        pr = false, rcode = 0},
        qdlist = [#dns_query{domain = "some.domain.com",
            type = a, class = in}],
        anlist =
        lists:map(
            fun(Counter) ->
                #dns_rr{domain = "some.domain.com",
                    type = a, class = in, cnt = 0, ttl = 600,
                    data = {200, 100, 50, Counter},
                    tm = undefined, bm = [], func = false}
            end, lists:seq(1, NumberOfRecords)),
        nslist = [], arlist = []}).


-define(EXAMPLE_OPT_RR(UPDSize),
    #dns_rr_opt{domain = ".", type = opt, udp_payload_size = UPDSize, ext_rcode = 0, version = 0, z = 0, data = <<"">>}).

-define(dump(A), io:format(user, "~nDUMP: ~p~n~n", [A])).

%%%===================================================================
%%% Tests description
%%%===================================================================

%% This test generator tests the functionalities and returned values of functions
%% included in dns_server module.
dns_server_test_() ->
    {setup,
        fun() -> ok end,
        fun(_) -> ok end,
        [
            {"validate_query", fun validate_query/0},
            {"type_to_fun", fun type_to_fun/0},
            {"generate_answer", fun generate_answer/0},
            {"set_reply_code", fun set_reply_code/0},
            {"encode_udp", fun encode_udp/0},
            {"answer sections", fun answer_sections/0},
            {"configuration", fun configuration/0}
        ]
    }.

%%%===================================================================
%%% Test functions
%%%===================================================================

validate_query() ->
    DNSRec = ?EXAMPLE_DNS_REC(1),
    OPTRR = ?EXAMPLE_OPT_RR(1080),
    ExampleQuery = #dns_query{domain = "www.mail.onedata.org", type = a, class = in},
    DNSRecNoEDNS = DNSRec#dns_rec{qdlist = [ExampleQuery], anlist = [], nslist = [], arlist = []},
    DNSRecEDNSPresent = DNSRec#dns_rec{qdlist = [ExampleQuery], anlist = [], nslist = [], arlist = [OPTRR]},
    DNSRecEDNSPresentBadVersion = DNSRec#dns_rec{qdlist = [ExampleQuery], anlist = [], nslist = [], arlist = [OPTRR#dns_rr_opt{version = 1}]},
    DNSRecWrong1 = DNSRec#dns_rec{qdlist = [ExampleQuery, ExampleQuery], anlist = [], nslist = [], arlist = []},
    DNSRecWrong2 = DNSRec#dns_rec{qdlist = [ExampleQuery], anlist = [ExampleQuery], nslist = [], arlist = []},
    DNSRecWrong3 = DNSRec#dns_rec{qdlist = [ExampleQuery], anlist = [], nslist = [ExampleQuery], arlist = []},
    DNSRecWrong4 = DNSRec#dns_rec{qdlist = [ExampleQuery], anlist = [ExampleQuery], nslist = [ExampleQuery], arlist = []},
    DNSRecWrong5 = DNSRec#dns_rec{qdlist = [], anlist = [], nslist = [], arlist = []},
    DNSRecWrong6 = DNSRec#dns_rec{qdlist = [], anlist = [], nslist = [], arlist = [OPTRR]},
    ?assertEqual(ok, dns_server:validate_query(DNSRecNoEDNS)),
    ?assertEqual(ok, dns_server:validate_query(DNSRecEDNSPresent)),
    ?assertEqual(bad_version, dns_server:validate_query(DNSRecEDNSPresentBadVersion)),
    ?assertEqual(form_error, dns_server:validate_query(DNSRecWrong1)),
    ?assertEqual(form_error, dns_server:validate_query(DNSRecWrong2)),
    ?assertEqual(form_error, dns_server:validate_query(DNSRecWrong3)),
    ?assertEqual(form_error, dns_server:validate_query(DNSRecWrong4)),
    ?assertEqual(form_error, dns_server:validate_query(DNSRecWrong5)),
    ?assertEqual(form_error, dns_server:validate_query(DNSRecWrong6)).


type_to_fun() ->
    ?assertEqual(handle_a, dns_server:type_to_fun(?S_A)),
    ?assertEqual(handle_ns, dns_server:type_to_fun(?S_NS)),
    ?assertEqual(handle_cname, dns_server:type_to_fun(?S_CNAME)),
    ?assertEqual(handle_soa, dns_server:type_to_fun(?S_SOA)),
    ?assertEqual(handle_wks, dns_server:type_to_fun(?S_WKS)),
    ?assertEqual(handle_ptr, dns_server:type_to_fun(?S_PTR)),
    ?assertEqual(handle_hinfo, dns_server:type_to_fun(?S_HINFO)),
    ?assertEqual(handle_minfo, dns_server:type_to_fun(?S_MINFO)),
    ?assertEqual(handle_mx, dns_server:type_to_fun(?S_MX)),
    ?assertEqual(handle_txt, dns_server:type_to_fun(?S_TXT)),
    ?assertEqual(not_impl, dns_server:type_to_fun(?S_AXFR)).


generate_answer() ->
    % This test shall test only TCP answers - the only difference
    % between TCP and UDP is truncation, which is tested in encode_udp_t test.

    % Example dns_rec and opt_rr records
    #dns_rec{header = Header, arlist = ARList} = DNSRec = ?EXAMPLE_DNS_REC(1),
    OPTRR = ?EXAMPLE_OPT_RR(1080),

    % Set server's max edns udp
    dns_server:set_max_edns_udp_size(720),

    DNSRecNoOPTRR = DNSRec#dns_rec{header = Header#dns_header{ra = false, qr = true}},
    DNSRecOPTRRPresent = DNSRec#dns_rec{
        header = Header#dns_header{ra = false, qr = true},
        arlist = ARList ++ [OPTRR#dns_rr_opt{udp_payload_size = 720}]
    },

    ?assertEqual({ok, inet_dns:encode(DNSRecNoOPTRR)}, dns_server:generate_answer(DNSRec, undefined, tcp)),
    ?assertEqual({ok, inet_dns:encode(DNSRecOPTRRPresent)}, dns_server:generate_answer(DNSRec, OPTRR, tcp)).


set_reply_code() ->
    #dns_rec{header = Header} = DNSRec = ?EXAMPLE_DNS_REC(1),
    DNSRecServFail = DNSRec#dns_rec{header = Header#dns_header{rcode = ?SERVFAIL}},
    DNSRecNXDomain = DNSRec#dns_rec{header = Header#dns_header{rcode = ?NXDOMAIN}},
    DNSRecNotImp = DNSRec#dns_rec{header = Header#dns_header{rcode = ?NOTIMP}},
    DNSRecRefused = DNSRec#dns_rec{header = Header#dns_header{rcode = ?REFUSED}},
    DNSRecNoError = DNSRec#dns_rec{header = Header#dns_header{rcode = ?NOERROR}},
    ?assertEqual(DNSRecServFail, dns_server:set_reply_code(DNSRec, serv_fail)),
    ?assertEqual(DNSRecNXDomain, dns_server:set_reply_code(DNSRec, nx_domain)),
    ?assertEqual(DNSRecNotImp, dns_server:set_reply_code(DNSRec, not_impl)),
    ?assertEqual(DNSRecRefused, dns_server:set_reply_code(DNSRec, refused)),
    ?assertEqual(DNSRecNoError, dns_server:set_reply_code(DNSRec, ok)).


encode_udp() ->
    % Prepare some example dns answers
    SmallDNSRec = ?EXAMPLE_DNS_REC(1),
    BigDNSRec = ?EXAMPLE_DNS_REC(30),
    HugeDNSRec = ?EXAMPLE_DNS_REC(70),

    % Set server's max edns udp
    dns_server:set_max_edns_udp_size(720),

    % Small response will never be truncated.
    % If the client specifies max UDP size of less than 512, its assumed 512. The same goes
    % when he does not send an EDNS OPTRR.
    % So, none of below should be truncated (the packet is 49B big, and the truncation
    % may start at 512).
    SmallWhole = inet_dns:encode(SmallDNSRec),
    ?assertEqual(SmallWhole, dns_server:encode_udp(SmallDNSRec, undefined)),
    ?assertEqual(SmallWhole, dns_server:encode_udp(SmallDNSRec, 10)),
    ?assertEqual(SmallWhole, dns_server:encode_udp(SmallDNSRec, 512)),
    ?assertEqual(SmallWhole, dns_server:encode_udp(SmallDNSRec, 1080)),

    % BigDNSRec is just above the 512 limit (513 B). Compute how it should be truncated (set TC bit to 1).
    % The reponse should be truncated for first three cases, to 512.
    BigWhole = inet_dns:encode(BigDNSRec),
    <<BigTruncatedBytes:(512 * 8), _/binary>> = inet_dns:encode(BigDNSRec#dns_rec{header = inet_dns:make_header(BigDNSRec#dns_rec.header, tc, true)}),
    BigTruncated = <<BigTruncatedBytes:(512 * 8)>>,
    ?assertEqual(BigTruncated, dns_server:encode_udp(BigDNSRec, undefined)),
    ?assertEqual(BigTruncated, dns_server:encode_udp(BigDNSRec, 10)),
    ?assertEqual(BigTruncated, dns_server:encode_udp(BigDNSRec, 512)),
    ?assertEqual(BigWhole, dns_server:encode_udp(BigDNSRec, 1080)),

    % BigDNSRec is 1153 Bytes. Compute how it should be truncated (set TC bit to 1), for 512 or 720.
    % The reponse should be truncated for first three cases, to 512 and for foutrh to 720 (server's max UDP).
    <<HugeTruncated512Bytes:(512 * 8), _/binary>> = inet_dns:encode(HugeDNSRec#dns_rec{header = inet_dns:make_header(HugeDNSRec#dns_rec.header, tc, true)}),
    HugeTruncated512 = <<HugeTruncated512Bytes:(512 * 8)>>,
    <<HugeTruncated720Bytes:(720 * 8), _/binary>> = inet_dns:encode(HugeDNSRec#dns_rec{header = inet_dns:make_header(HugeDNSRec#dns_rec.header, tc, true)}),
    HugeTruncated720 = <<HugeTruncated720Bytes:(720 * 8)>>,
    ?assertEqual(HugeTruncated512, dns_server:encode_udp(HugeDNSRec, undefined)),
    ?assertEqual(HugeTruncated512, dns_server:encode_udp(HugeDNSRec, 10)),
    ?assertEqual(HugeTruncated512, dns_server:encode_udp(HugeDNSRec, 512)),
    ?assertEqual(HugeTruncated720, dns_server:encode_udp(HugeDNSRec, 1080)),

    % Set server's max edns udp to more than client's
    dns_server:set_max_edns_udp_size(1720),
    % now, the response should be truncated to 1080 (Client's max UDP) fot the fourth response.
    <<HugeTruncated1080Bytes:(1080 * 8), _/binary>> = inet_dns:encode(HugeDNSRec#dns_rec{header = inet_dns:make_header(HugeDNSRec#dns_rec.header, tc, true)}),
    HugeTruncated1080 = <<HugeTruncated1080Bytes:(1080 * 8)>>,
    ?assertEqual(HugeTruncated512, dns_server:encode_udp(HugeDNSRec, undefined)),
    ?assertEqual(HugeTruncated512, dns_server:encode_udp(HugeDNSRec, 10)),
    ?assertEqual(HugeTruncated512, dns_server:encode_udp(HugeDNSRec, 512)),
    ?assertEqual(HugeTruncated1080, dns_server:encode_udp(HugeDNSRec, 1080)).


answer_sections() ->
    Domain = "some.domain.com",
    TTL = 3600,
    Type = ?S_NS,
    Data = "ns1.some.domain.com",
    AuthoritativeFlag = false,
    ?assertEqual({aa, AuthoritativeFlag},
        dns_server:authoritative_answer_flag(AuthoritativeFlag)),
    ?assertEqual({answer, Domain, TTL, Type, Data},
        dns_server:answer_record(Domain, TTL, Type, Data)),
    ?assertEqual({authority, Domain, TTL, Type, Data},
        dns_server:authority_record(Domain, TTL, Type, Data)),
    ?assertEqual({additional, Domain, TTL, Type, Data},
        dns_server:additional_record(Domain, TTL, Type, Data)).


configuration() ->
    HandlerModule = some_module,
    MaxEdnsUdpSize = 123456,
    dns_server:set_handler_module(HandlerModule),
    ?assertEqual(HandlerModule, dns_server:get_handler_module()),
    dns_server:set_max_edns_udp_size(MaxEdnsUdpSize),
    ?assertEqual(MaxEdnsUdpSize, dns_server:get_max_edns_udp_size()).

-endif.