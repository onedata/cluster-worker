%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2014 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc: This behaviour defines API for a DNS query handler.
%%% Seems redundant as dns_worker_plugins does actual handling.
%%% Yet it provides valuable piece of documentation.
%%% @end
%%%-------------------------------------------------------------------
-module(dns_worker_plugin_behaviour).

-include_lib("kernel/src/inet_dns.hrl").

% Allowed query types
-type query_type() :: ?S_A | ?S_NS | ?S_CNAME | ?S_SOA | ?S_WKS | ?S_PTR | ?S_HINFO | ?S_MINFO | ?S_MX | ?S_TXT.

% Atoms for reply codes for cenvenience
-type reply_code() :: ok | serv_fail | nx_domain | not_impl | refused | form_error | bad_version.

% Records used internally to pass query results from handler to server, which then puts them in response.
-type answer_record() :: {answer, Type :: query_type(), Data :: term()}.
-type authority_record() :: {authority, Type :: query_type(), Data :: term()}.
-type additional_record() :: {additional, Type :: query_type(), Data :: term()}.
-type authoritative_answer_flag() :: {aa, Flag :: boolean()}.
-type reply_record_list() :: [answer_record() | authority_record () | additional_record() | authoritative_answer_flag()].
-type handler_reply() :: reply_code() | {reply_code(), reply_record_list()}.
-type handle_method() :: handle_a | handle_ns | handle_cname | handle_soa |
handle_wks | handle_ptr | handle_hinfo | handle_minfo | handle_mx | handle_txt.

-export_type([query_type/0, reply_code/0, handler_reply/0, authority_record/0, handle_method/0]).

%% Data types that should be returned for specific types of queries:
%% -----------------------------------------
%% Type A, identified by macro ?S_A (RFC1035 3.4.1)
%% Data :: {A :: byte(), B :: byte(), C :: byte(), D :: byte()}
%% IPv4 address(es) - IP address(es) of servers:
%% {A, B, C, D}: Bytes of IPv4 address

%% -----------------------------------------
%% Type NS, identified by macro ?S_NS (RFC1035 3.3.11)
%% Data :: string()
%% A <domain-name> which specifies a host which should be authoritative for the specified class and domain.

%% -----------------------------------------
%% Type CNAME, identified by macro ?S_CNAME (RFC1035 3.3.1)
%% Data :: string()
%% A <domain-name> which specifies the canonical or primary name for the owner.  The owner name is an alias.

%% -----------------------------------------
%% Type SOA, identified by macro ?S_SOA (RFC1035 3.3.13)
%% Data :: {MName :: string() , RName:: string(), Serial :: integer(), Refresh :: integer(), Retry :: integer(), Expiry :: integer(), Minimum :: integer()}
%% MName: The <domain-name> of the name server that was the original or primary source of data for this zone.
%% RName: A <domain-name> which specifies the mailbox of the person responsible for this zone.
%% Serial: The unsigned 32 bit version number of the original copy of the zone.  Zone transfers preserve this value.
%%    This value wraps and should be compared using sequence space arithmetic.
%% Refresh: A 32 bit time interval before the zone should be refreshed.
%% Retry: A 32 bit time interval that should elapse before a failed refresh should be retried.
%% Expiry: A 32 bit time value that specifies the upper limit on the time interval that can elapse before the zone
%%   is no longer authoritative.
%% Minimum: The unsigned 32 bit minimum TTL field that should be exported with any RR from this zone.

%% -----------------------------------------
%% Type WKS, identified by macro ?S_WKS (RFC1035 3.4.2)
%% Data :: {{A :: byte(), B :: byte(), C :: byte(), D :: byte()}, Proto :: string(), BitMap :: string()}
%% {A, B, C, D}: Bytes of IPv4 address
%% Proto: An 8 bit IP protocol number
%% BitMap: A variable length bit map. The bit map must be a multiple of 8 bits long.
%%   A positive bit means, that a port of number equal to its position in bitmap is open.
%%   BitMap representation as string in erlang is problematic. Example of usage:
%%   BitMap = [2#11111111, 2#00000000, 2#00000011]
%%   above bitmap will inform the client, that ports: 0 1 2 3 4 5 6 7 22 23 are open.

%% -----------------------------------------
%% Type PTR, identified by macro ?S_PTR (RFC1035 3.3.12)
%% Data :: string()
%% <domain-name> which points to some location in the domain name space.

%% -----------------------------------------
%% Type HINFO, identified by macro ?S_HINFO (RFC1035 3.3.2)
%% Data :: {CPU :: string(), OS :: string()}
%% CPU: A <character-string> which specifies the CPU type.
%% OS: A <character-string> which specifies the operatings system type.

%% -----------------------------------------
%% Type MINFO, identified by macro ?S_MINFO (RFC1035 3.3.7)
%% Data :: {RM :: string(), EM :: string()}
%% RM: A <domain-name> which specifies a mailbox which is responsible for the mailing list or mailbox.  If this
%%   domain name names the root, the owner of the MINFO RR is responsible for itself.  Note that many existing mailing
%%   lists use a mailbox X-request for the RMAILBX field of mailing list X, e.g., Msgroup-request for Msgroup.  This
%%   field provides a more general mechanism.
%% EM: A <domain-name> which specifies a mailbox which is to receive error messages related to the mailing list or
%%   mailbox specified by the owner of the MINFO RR (similar to the ERRORS-TO: field which has been proposed).  If
%%   this domain name names the root, errors should be returned to the sender of the message.
%% -----------------------------------------

%% Type MX, identified by macro ?S_MX (RFC1035 3.3.9)
%% Data :: {Pref :: integer(), Exch :: string()}
%% Pref: A 16 bit integer which specifies the preference given to this RR among others at the same owner.
%%   Lower values are preferred.
%% Exch: A <domain-name> which specifies a host willing to act as a mail exchange for the owner name.
%% -----------------------------------------

%% Type TXT, identified by macro ?S_TXT (RFC1035 3.3.14)
%% text data - one or more <character-string>s:
%% TXT RRs are used to hold descriptive text. The semantics of the text depends on the domain where it is found.
%% NOTE: Should return list of strings for every record, for example:
%% [["string_1_1", "string_1_2"], ["string_2_1", "string_2_2"]] - two records, each with two strings.
%% -----------------------------------------


%%%===================================================================
%%% Callbacks API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Callback below handle specific types od DNS queries, in accordance to RFC1035:
%% {@link https://tools.ietf.org/html/rfc1035#section-3.2.2}
%% The first argument specifies handled method. The third supplies with load balancing advice.
%% The second argument in every function is Domain that was queried for, as a lower-case string.
%% On success, the callback must return {ok, List}, where List consists of terms created with functions:
%% dns_server:answer_record/2, dns_server:authority_record/2, dns_server:additional_record/2, dns_server:authoritative_answer_flag/1.
%% Those terms will be put in proper sections of DNS response.
%% See {@link dns.hrl} for reference and data types that should be returned for specific types of queries.
%% @end
%%--------------------------------------------------------------------

-callback resolve(Method :: handle_method(), Domain :: string(), LbAdvice :: load_balancing:dns_lb_advice()) ->
    handler_reply().


