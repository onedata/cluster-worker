%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module manages GSI validation
%% @end
%% ===================================================================
-module(gsi_handler).

-include_lib("cluster_elements/request_dispatcher/gsi_handler.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("registered_names.hrl").
-include("logging.hrl").

-deprecated([proxy_subject/1]).

-export([init/0, verify_callback/3, load_certs/1, update_crls/1, proxy_subject/1, call/3, is_proxy_certificate/1, find_eec_cert/3]).
%% ===================================================================
%% API
%% ===================================================================


%% init/0
%% ====================================================================
%% @doc Initializes GSI Handler. This method should be called once, before using any other method from this module.
%% @end
-spec init() -> ok.
%% ====================================================================
init() ->
    case application:get_env(?APP_Name, node_type) of
        {ok, ccm} -> throw(ccm_node);                     %% ccm node doesn't have socket interface, so GSI would be useless
        _ -> ok
    end,
    lager:info("GSI Handler module is starting"),
    ets:new(gsi_state, [{read_concurrency, true}, public, ordered_set, named_table]),

    {ok, CADir1} = application:get_env(?APP_Name, ca_dir),
    CADir = atom_to_list(CADir1),

    {SSPid, _Ref} = spawn_monitor(fun() -> start_slaves(?GSI_SLAVE_COUNT) end), 

    case filelib:is_dir(CADir) of
        true ->
            load_certs(CADir),
            update_crls(CADir);
        false ->
            lager:error("Cannot find GSI CA certs dir (~p)", [CADir])
    end,

    receive 
        {'DOWN', _, process, SSPid, normal} -> lager:info("GSI Handler module successfully loaded");
        {'DOWN', _, process, SSPid, Reason} -> 
            lager:warning("GSI Handler: slave node loader unknown exit reason: ~p", [Reason]),
            lager:info("GSI Handler module partially loaded")
    after 5000 ->
        lager:error("GSI Handler: slave node loader execution timeout, state unknown"),
        lager:info("GSI Handler module partially loaded")
    end,

    ok.


%% verify_callback/3
%% ====================================================================
%% @doc This method is an registered callback, called foreach peer certificate. <br/>
%%      This callback saves whole certificate chain in GSI ETS based state for further use.
%% @end
-spec verify_callback(OtpCert :: #'OTPCertificate'{}, Status :: term(), Certs :: [#'OTPCertificate'{}]) ->
    {valid, UserState :: any()} | {fail, Reason :: term()}.
%% ====================================================================
verify_callback(OtpCert, valid_peer, Certs) ->
    Serial = save_cert_chain([OtpCert | Certs]),
    ?info("Peer ~p connected", [Serial]),
    {valid, []};
verify_callback(OtpCert, {bad_cert, unknown_ca}, Certs) ->
    save_cert_chain([OtpCert | Certs]),
    {valid, [OtpCert | Certs]};
verify_callback(OtpCert, _IgnoredError, Certs) ->
    case Certs of
        [OtpCert | _] -> {valid, Certs};
        _ -> {valid, [OtpCert | Certs]}
    end.


%% load_certs/1
%% ====================================================================
%% @doc Loads all PEM encoded CA certificates from given directory along with their CRL certificates (if any). <br/>
%%      Note that CRL certificates should also be PEM encoded and the CRL filename should match their CA filename but with '.crl' extension.
%% @end
-spec load_certs(CADir :: string()) -> ok | no_return().
%% ====================================================================
load_certs(CADir) ->
    lager:info("GSI Handler: Loading CA certs from dir ~p", [CADir]),
    {ok, Files} = file:list_dir(CADir),
    CA1 = [{strip_filename_ext(Name), file:read_file(filename:join(CADir, Name))} || Name <- Files, lists:suffix(".pem", Name)],
    CRL1 = [{strip_filename_ext(Name), file:read_file(filename:join(CADir, strip_filename_ext(Name) ++ ".crl"))} || Name <- Files, lists:suffix(".pem", Name)],
    CA2 = [ lists:map(fun(Y) -> {Name, Y} end, public_key:pem_decode(X)) || {Name, {ok, X}} <- CA1],
    CRL2 = [ lists:map(fun(Y) -> {Name, Y} end, public_key:pem_decode(X)) || {Name, {ok, X}} <- CRL1],

    {Len1, Len2} =
        lists:foldl(fun({Name, {Type, X, _}}, {CAs, CRLs}) ->
                    case Type of
                        'Certificate' -> ets:insert(gsi_state, {{ca, public_key:pkix_issuer_id(X, self)}, X, Name}), {CAs + 1, CRLs};
                        'CertificateList' -> ets:insert(gsi_state, {{crl, public_key:pkix_issuer_id(X, self)}, X, Name}), {CAs, CRLs + 1};
                        _ -> {CAs, CRLs}
                    end end, {0, 0}, lists:flatten(CA2 ++ CRL2)),
    lager:info("GSI Handler: ~p CA and ~p CRL certs successfully loaded", [Len1, Len2]),
    ok.


%% update_crls/1
%% ====================================================================
%% @doc Updates CRL certificates based on their distribution point (x509 CA extension). <br/>
%%      Not yet fully implemented.
%% @end
-spec update_crls(CADir :: string()) -> ok | no_return().
%% ====================================================================
update_crls(CADir) ->
    CAs = [{public_key:pkix_decode_cert(X, otp), Name} || [X, Name] <- ets:match(gsi_state, {{ca, '_'}, '$1', '$2'})],
    CAsAndDPs = [{OtpCert, get_dp_url(OtpCert), Name} || {OtpCert, Name} <- CAs],
    lists:foreach(fun(X) -> update_crl(CADir, X) end, CAsAndDPs),
    ok.


%% proxy_subject/1
%% ====================================================================
%% @doc Returns subject of given certificate.
%% If proxy certificate is given, EEC subject is returned.
%% @deprecated The function shall not be used when proxy is not directly signed by EEC (which you can't be sure about without x509 chain). <br/>
%%             Use {@link gsi_handler:find_eec_cert/3} instead and get EEC's subject (if you have the whole certificate chain available).
%% @end
-spec proxy_subject(OtpCert :: #'OTPCertificate'{}) -> {rdnSequence, [#'AttributeTypeAndValue'{}]}.
%% ====================================================================
proxy_subject(OtpCert = #'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{} = TbsCert}) ->
    Subject = TbsCert#'OTPTBSCertificate'.subject,
    case is_proxy_certificate(OtpCert) of
        true -> %% Delete last 'common name' attribute, because its proxy-specific
            {rdnSequence, Attrs} = Subject,
            Attrs1 = lists:keydelete(?'id-at-commonName', 2, lists:reverse(Attrs)),
            {rdnSequence, lists:reverse(Attrs1)};
        false ->
            Subject
    end.


%% ===================================================================
%% Internal Methods
%% ===================================================================


%% save_cert_chain/1
%% ====================================================================
%% @doc Saves whole given certificate chain in GSI ETS based state for further use. <br/>
%%      EEC certificate pkix_issuer_id is used as ETS key for new entry. <br/>
%%      Saved chain will is scheduled to removal when EEC certificate expires
%% @end
-spec save_cert_chain([OtpCert :: #'OTPCertificate'{}]) -> Serial :: integer().
%% ====================================================================
save_cert_chain([OtpCert | Certs]) ->
    {ok, {Serial, Issuer}} = public_key:pkix_issuer_id(OtpCert, self),
    case ets:lookup(gsi_state, {Serial, Issuer}) of
        [{_, _, TRef1}]     -> timer:cancel(TRef1);
        _                   -> ok
    end,
    TBSCert = OtpCert#'OTPCertificate'.tbsCertificate,
    {'Validity', _NotBeforeStr, NotAfterStr} = TBSCert#'OTPTBSCertificate'.validity,
    Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
    NotAfter = time_str_2_gregorian_sec(NotAfterStr),
    {ok, TRef} = timer:apply_after(timer:seconds(NotAfter - Now), ets, delete, [gsi_state, {Serial, Issuer}]),
    ets:insert(gsi_state, {{Serial, Issuer}, [OtpCert | Certs], TRef}),
    Serial.

%% start_slaves/1
%% ====================================================================
%% @doc ttializes Count slave nodes. See {@link initialize_node/1}.
%% @end
-spec start_slaves(Count :: non_neg_integer()) -> [any()].
%% ====================================================================
start_slaves(Count) when Count >= 0 ->
    [initialize_node(list_to_atom(atom_to_list(get_node_name()) ++ "_gsi" ++ integer_to_list(N))) || N <- lists:seq(1, Count)].


%% initialize_node/1
%% ====================================================================
%% @doc Initializes slave node with given NodeName. Starts it and loads NIF library. <br/>
%%      If NIF load fails, slave node is stopped.
%% @end
-spec initialize_node(NodeName :: atom()) -> any().
%% ====================================================================
initialize_node(NodeName) when is_atom(NodeName) ->
    lager:info("Trying to start GSI slave node: ~p @ ~p", [NodeName, get_host()]),
    NodeRes1 =
        case slave:start(get_host(), NodeName, make_code_path() ++ " -setcookie \"" ++ atom_to_list(erlang:get_cookie()) ++ "\"", no_link, erl) of
            {error, {already_running, Node}} ->
                lager:info("GSI slave node ~p is already running", [Node]),
                Node;
            {ok, Node} ->
                lager:info("GSI slave node ~p started", [Node]),
                Node;
            {error, Reason} ->
                lager:error("Could not start GSI slave node ~p @ ~p due to error: ~p", [NodeName, get_host(), Reason]),
                'nonode@nohost'
        end,
    case NodeRes1 of 
        'nonode@nohost' -> {error, cannot_start_node};
        NodeRes ->
            {ok, Prefix} = application:get_env(?APP_Name, nif_prefix),
            case rpc:call(NodeRes, gsi_nif, start, [atom_to_list(Prefix)]) of
                ok ->
                    lager:info("NIF lib on node ~p was successfully loaded", [NodeRes]),
                    ets:insert(gsi_state, {node, NodeName});
                {error,{reload, _}} ->
                    lager:info("NIF lib on node ~p is already loaded", [NodeRes]),
                    ets:insert(gsi_state, {node, NodeName});
                {error, Reason1} ->
                    lager:error("Could not load NIF lib on node ~p due to: ~p. Killing node", [NodeRes, Reason1]),
                    slave:stop(NodeRes);
                {badrpc, Reason2} ->
                    lager:error("Could not load NIF lib on node ~p due to: ~p. Ignoring", [NodeRes, Reason2])
            end
    end.


%% call/3
%% ====================================================================
%% @doc Calls apply(Module, Method, Args) on one of started slave nodes.
%%      If slave node is down, initializes restart procedure and tries to use another node. <br/>
%%      However is all nodes are down, error is returned and GSI action is interrupted (e.g. peer verification fails).
%% @end
-spec call(Module :: atom(), Method :: atom(), Args :: [term()]) -> ok | no_return().
%% ====================================================================
call(Module, Method, Args) ->
    case ets:info(gsi_state) of 
        undefined -> error(gsi_handler_not_loaded);
        _ -> ok 
    end,
    Nodes = ets:lookup(gsi_state, node),
    call(Module, Method, Args, Nodes).


%% call/4
%% ====================================================================
%% @doc See {@link call/3}
%% @end
-spec call(Module :: atom(), Method :: atom(), Args :: [term()], [Node :: atom()]) -> Response :: term().
%% ====================================================================
call(_Module, _Method, _Args, []) ->
    spawn(fun() -> start_slaves(?GSI_SLAVE_COUNT) end),
    lager:error("No GSI slave nodes. Trying to reinitialize module"),
    {error, verification_nodes_down};
call(Module, Method, Args, [{node, NodeName} | OtherNodes]) ->
    case rpc:call(get_node(NodeName), Module, Method, Args) of
        {badrpc, {'EXIT', Exit}} -> {'EXIT', Exit};
        {badrpc, Reason} ->
            spawn(fun() -> initialize_node(NodeName) end),
            lager:error("GSI slave node ~p is down (reason ~p). Trying to reinitialize node", [get_node(NodeName), Reason]),
            call(Module, Method, Args, OtherNodes);
        Res -> Res
    end.


%% update_crl/1
%% ====================================================================
%% @doc Handles CRL update process for given CRL certificate. <br/>
%%      This method gets already prepared URLs and destination file name.
%% @end
-spec update_crl(CADir :: string(), {OtpCert :: #'OTPCertificate'{}, [URLs :: string()], Name :: string()}) -> not_yet_implemented.
%% ====================================================================
update_crl(_CADir, {_OtpCert, [], _Name}) ->
    no_dp;  
update_crl(_CADir, {_OtpCert, [_URL | _URLs], _Name}) ->
    not_yet_implemented.                                    %% TODO: implement CRL update via http (httpc module?)


%% get_host/0
%% ====================================================================
%% @doc Returns current erlang VM host name (as atom).
%% @end
-spec get_host() -> Host :: atom().
%% ====================================================================
get_host() ->
    Node = atom_to_list(node()),
    [_, Host] = string:tokens(Node, "@"),
    list_to_atom(Host).


%% get_node_name/0
%% ====================================================================
%% @doc Returns current erlang VM node name (as atom).
%% @end
-spec get_node_name() -> NodeName :: atom().
%% ====================================================================
get_node_name() ->
    Node = atom_to_list(node()),
    [Name, _] = string:tokens(Node, "@"),
    list_to_atom(Name).


%% get_node/1
%% ====================================================================
%% @doc Returns 'NodeName@get_host()' atom. Basically it uses given NodeName to generate full node spec (using current host name)>
%% @end
-spec get_node(NodeName :: atom()) -> Node :: atom().
%% ====================================================================
get_node(NodeName) ->
    list_to_atom(atom_to_list(NodeName) ++ "@" ++ atom_to_list(get_host())).


%% make_code_path/0
%% ====================================================================
%% @doc Returns current code path string, formatted as erlang slave node argument.
%% @end
-spec make_code_path() -> string().
%% ====================================================================
make_code_path() ->
    lists:foldl(fun(Node, Path) -> " -pa " ++ Node ++ Path end,
        [], code:get_path()).


%% get_dp_url/1
%% ====================================================================
%% @doc Extracts from given OTP certificate list of distribution point's URLs (based on x509 DP extension)
%% @end
-spec get_dp_url(OtpCert :: #'OTPCertificate'{}) -> [URL :: string()].
%% ====================================================================
get_dp_url(OtpCert = #'OTPCertificate'{}) ->
    Ext = OtpCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.extensions,
    DPs = lists:flatten([X || #'Extension'{extnValue = X} <- Ext, is_list(X)]),
    GNames = [GenNames || #'DistributionPoint'{distributionPoint = {fullName, GenNames}} <- DPs],
    [URL || {uniformResourceIdentifier, URL} <- lists:flatten(GNames)].


%% strip_filename_ext/1
%% ====================================================================
%% @doc Strips extension from given filename.
%% @end
-spec strip_filename_ext(FileName :: string()) -> FileName :: string().
%% ====================================================================
strip_filename_ext(FileName) when is_list(FileName) ->
    filename:rootname(FileName).


%% is_proxy_certificate/1
%% ====================================================================
%% @doc Checks is given OTP Certificate has an proxy extension or looks like legacy proxy.
%%      'maybe' is returned for proxy legacy proxy certificates since, there's no way to be sure about it
%%      'true' is returned only for RFC compliant Proxy Certificates.
%% @end
-spec is_proxy_certificate(OtpCert :: #'OTPCertificate'{}) -> boolean() | maybe.
%% ====================================================================
is_proxy_certificate(OtpCert = #'OTPCertificate'{}) ->
    Ext = OtpCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.extensions,
    Subject = OtpCert#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject,
    case Ext of
        Exts when is_list(Exts) ->
            lists:foldl(fun(#'Extension'{extnID = ?PROXY_CERT_EXT}, _) -> true;
                    (_, AccIn) -> AccIn end, false, Ext);
        _ ->
            {rdnSequence, Attrs} = Subject,
            FlatAttrs = [Attr || #'AttributeTypeAndValue'{} = Attr <- lists:flatten(Attrs)],
            ReversedAttrs = lists:reverse(FlatAttrs),
            case ReversedAttrs of
                [#'AttributeTypeAndValue'{type = ?'id-at-commonName', value = {_, "proxy"}} | _] ->
                    maybe;
                _ -> false
            end
    end.


%% find_eec_cert/3
%% ====================================================================
%% @doc For given proxy certificate returns its EEC 
%% @end
-spec find_eec_cert(CurrentOtp :: #'OTPCertificate'{}, Chain :: [#'OTPCertificate'{}], IsProxy :: boolean()) -> {ok, #'OTPCertificate'{}} | no_return().
%% ====================================================================
find_eec_cert(CurrentOtp, Chain, maybe) ->
    ?warning("Processing non RFC compliant Proxy Certificate with subject: ~p", [CurrentOtp#'OTPCertificate'.tbsCertificate#'OTPTBSCertificate'.subject]),
    find_eec_cert(CurrentOtp, Chain, true);
find_eec_cert(CurrentOtp, Chain, true) ->
    false = public_key:pkix_is_self_signed(CurrentOtp),
    {ok, NextCert} = 
        lists:foldl(fun(_, {ok, Found}) -> {ok, Found};
                    (Cert, NotFound)-> case public_key:pkix_is_issuer(CurrentOtp, Cert) of 
                                           true -> {ok, Cert};
                                           false -> NotFound
                                        end end,    
                no_cert, Chain), 
    find_eec_cert(NextCert, Chain, is_proxy_certificate(NextCert));
find_eec_cert(CurrentOtp, _Chain, false) ->
    {ok, CurrentOtp}.


%% time_str_2_gregorian_sec/1
%% ====================================================================
%% @doc See pubkey_cert:time_str_2_gregorian_sec/1  
%% @end
-spec time_str_2_gregorian_sec(TimeStr :: term()) -> integer().
%% ====================================================================
time_str_2_gregorian_sec({utcTime, [Y1,Y2,M1,M2,D1,D2,H1,H2,M3,M4,S1,S2,Z]}) ->
    case list_to_integer([Y1,Y2]) of
        N when N >= 70 ->
            time_str_2_gregorian_sec({generalTime,
            [$1,$9,Y1,Y2,M1,M2,D1,D2,
            H1,H2,M3,M4,S1,S2,Z]});
        _ ->
            time_str_2_gregorian_sec({generalTime,
            [$2,$0,Y1,Y2,M1,M2,D1,D2,
            H1,H2,M3,M4,S1,S2,Z]})
    end;
time_str_2_gregorian_sec({_,[Y1,Y2,Y3,Y4,M1,M2,D1,D2,H1,H2,M3,M4,S1,S2,$Z]}) ->
    Year = list_to_integer([Y1, Y2, Y3, Y4]),
    Month = list_to_integer([M1, M2]),
    Day = list_to_integer([D1, D2]),
    Hour = list_to_integer([H1, H2]),
    Min = list_to_integer([M3, M4]),
    Sec = list_to_integer([S1, S2]),
    calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Min, Sec}}).
