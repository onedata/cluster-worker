%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This is a simple library used to establish an OpenID authentication.
%% It needs nitrogen and ibrowse to run.
%% @end
%% ===================================================================

-module(openid_utils).

-include_lib("xmerl/include/xmerl.hrl").
-include_lib("ibrowse/include/ibrowse.hrl").
-include("veil_modules/control_panel/openid_utils.hrl").
-include("logging.hrl").


%% ====================================================================
%% API functions
%% ====================================================================
-export([get_login_url/2, nitrogen_prepare_validation_parameters/0, validate_openid_login/1, nitrogen_retrieve_user_info/0]).


%% get_login_url/0
%% ====================================================================
%% @doc
%% Produces an URL with proper GET parameters,
%% used to redirect the user to OpenID Provider login page.
%% RedirectParams are parameters concatenated to return_to field.
%% @end
-spec get_login_url(HostName :: string(), RedirectParams :: string()) -> Result when
    Result :: ok | {error, endpoint_unavailable}.
%% ====================================================================
get_login_url(HostName, RedirectParams) ->
    try
        discover_op_endpoint(?xrds_url) ++
            "?" ++ ?openid_checkid_mode ++
            "&" ++ ?openid_ns ++
            "&" ++ ?openid_return_to_prefix ++ HostName ++ ?openid_return_to_suffix ++ RedirectParams ++
            "&" ++ ?openid_claimed_id ++
            "&" ++ ?openid_identity ++
            "&" ++ ?openid_realm_prefix ++ HostName ++
            "&" ++ ?openid_sreg_required ++
            "&" ++ ?openid_ns_ext1 ++
            "&" ++ ?openid_ext1_mode ++
            "&" ++ ?openid_ext1_type_dn1 ++
            "&" ++ ?openid_ext1_type_dn2 ++
            "&" ++ ?openid_ext1_type_dn3 ++
            "&" ++ ?openid_ext1_type_teams ++
            "&" ++ ?openid_ext1_if_available

    catch Type:Message ->
        lager:error("Unable to resolve OpenID Provider endpoint.~n~p: ~p~n~p", [Type, Message, erlang:get_stacktrace()]),
        {error, endpoint_unavailable}
    end.


%% nitrogen_prepare_validation_parameters/0
%% ====================================================================
%% @doc
%% This function retrieves endpoint URL and parameters from redirection URL created by OpenID provider.
%% They are later used as arguments to validate_openid_login() function.
%% Must be called from within nitrogen page context to work, precisely 
%% from openid redirection page.
%% @end
-spec nitrogen_prepare_validation_parameters() -> Result when
    Result :: {string(), string()} | {error, Error},
    Error :: invalid_request.
%% ====================================================================
nitrogen_prepare_validation_parameters() ->
    try
        % 'openid.signed' contains parameters that must be contained in validation request
        SignedArgsNoPrefix = string:tokens(wf:q(list_to_atom(?openid_signed_key)), ","),

        % Add 'openid.' prefix to all parameters
        % And add 'openid.sig' and 'openid.signed' params which are required for validation
        SignedArgs = lists:map(fun(X) ->
            "openid." ++ X end, SignedArgsNoPrefix) ++ [?openid_sig_key, ?openid_signed_key],

        % Create a POST request body
        RequestParameters = lists:foldl(
            fun(Key, Acc) ->
                Value = case wf:qs(list_to_atom(Key)) of
                            [] -> throw("Value for " ++ Key ++ " not found");
                            List -> lists:nth(1, List)
                        end,
                % Safely URL-decode params
                Acc ++ "&" ++ Key ++ "=" ++ wf:url_encode(Value)
            end, "", SignedArgs),
        ValidationRequestBody = ?openid_check_authentication_mode ++ RequestParameters,
        EndpointURL = wf:q(list_to_atom(?openid_op_endpoint_key)),
        {EndpointURL, ValidationRequestBody}

    catch Type:Message ->
        lager:error("Failed to process login validation request.~n~p: ~p~n~p", [Type, Message, erlang:get_stacktrace()]),
        {error, invalid_request}
    end.


%% validate_openid_login/0
%% ====================================================================
%% @doc
%% Checks if parameters returned from OP were really generated by them.
%% Upon success, returns a proplist with information about the user.
%% Args must be properly prepared, eg. as in nitrogen_prepare_validation_parameters() function.
%% @end
-spec validate_openid_login({EndpointURL, ValidationRequestBody}) -> Result when
    EndpointURL :: string(),
    ValidationRequestBody :: string(),
    Result :: ok | {error, Error},
    Error :: auth_invalid | no_connection.
%% ====================================================================
validate_openid_login({EndpointURL, ValidationRequestBody}) ->
    try
        Raw = ibrowse:send_req(EndpointURL, [{content_type, "application/x-www-form-urlencoded"}], post, ValidationRequestBody),
        {ok, 200, _, Response} = normalise_response(Raw),
        case Response of
            ?valid_auth_info -> ok;
            _ ->
                lager:alert("Security breach attempt spotted. Invalid redirect URL contained: ~p", [string:tokens(ValidationRequestBody, "&")]),
                {error, auth_invalid}
        end

    catch Type:Message ->
        lager:error("Failed to connect to OpenID provider.~n~p: ~p~n~p", [Type, Message, erlang:get_stacktrace()]),
        {error, no_connection}
    end.


%% nitrogen_retrieve_user_info/0
%% ====================================================================
%% @doc
%% This function retrieves user info from parameters of redirection URL created by OpenID provider.
%% They are returned as a proplist and later used to authenticate a user in the system.
%% Must be called from within nitrogen page context to work, precisely 
%% from openid redirection page.
%% @end
-spec nitrogen_retrieve_user_info() -> Result when
    Result :: {ok, list()} | {error, Error},
    Error :: invalid_request.
%% ====================================================================
nitrogen_retrieve_user_info() ->
    try
        Login = wf:q(?openid_login_key),
        Name = wf:q(?openid_name_key),
        Teams = parse_teams(wf:q(?openid_teams_key)),
        Email = wf:q(?openid_email_key),
        DN1 = wf:q(?openid_dn1_key),
        DN2 = wf:q(?openid_dn2_key),
        DN3 = wf:q(?openid_dn3_key),
        DnList = lists:filter(
            fun(X) ->
                (X /= undefined)
            end, [DN1, DN2, DN3]),
        {ok, [
            {login, Login},
            {name, Name},
            {teams, Teams},
            {email, Email},
            {dn_list, lists:usort(DnList)}
        ]}
    catch Type:Message ->
        lager:error("Failed to retrieve user info.~n~p: ~p~n~p", [Type, Message, erlang:get_stacktrace()]),
        {error, invalid_request}
    end.


%% ====================================================================
%% Internal functions
%% ====================================================================

%% discover_op_endpoint/1
%% ====================================================================
%% @doc
%% Retrieves an XRDS document from given endpoint URL and parses out the URI which will
%% be used for OpenID login redirect.
%% @end
-spec discover_op_endpoint(string()) -> string().
%% ====================================================================
discover_op_endpoint(EndpointURL) ->
    XRDS = get_xrds(EndpointURL),
    {Xml, _} = xmerl_scan:string(XRDS),
    xml_extract_value("URI", Xml).


%% xml_extract_value/2
%% ====================================================================
%% @doc
%% Extracts value from under a certain key
%% @end
-spec xml_extract_value(string(), #xmlElement{}) -> string().
%% ====================================================================
xml_extract_value(KeyName, Xml) ->
    [#xmlElement{content = [#xmlText{value = Value} | _]}] = xmerl_xpath:string("//" ++ KeyName, Xml),
    Value.


%% get_xrds/1
%% ====================================================================
%% @doc
%% Downloads an XRDS document from given URL.
%% @end
-spec get_xrds(string()) -> string().
%% ====================================================================
get_xrds(URL) ->
    % Maximum redirection count = 5
    {ok, 200, _, Body} = get_xrds(URL, 5),
    Body.


%% get_xrds/2
%% ====================================================================
%% @doc
%% Downloads xrds file performing GET on provided URL. Supports redirects.
%% @end
-spec get_xrds(string(), integer()) -> string().
%% ====================================================================
get_xrds(URL, Redirects) ->
    ReqHeaders =
        [
            {"Accept", "application/xrds+xml;level=1, */*"},
            {"Connection", "close"}
        ],
    ResponseRaw = ibrowse:send_req(URL, ReqHeaders, get),
    Response = normalise_response(ResponseRaw),
    case Response of
        {ok, Rcode, RespHeaders, _Body} when Rcode > 300 andalso Rcode < 304 andalso Redirects > 0 ->
            case get_redirect_url(URL, RespHeaders) of
                undefined -> Response;
                URL -> Response;
                NewURL -> get_xrds(NewURL, Redirects - 1)
            end;
        Response -> Response
    end.


%% get_redirect_url/1
%% ====================================================================
%% @doc
%% Retrieves redirect URL from a HTTP response.
%% @end
-spec get_redirect_url(string(), list()) -> string().
%% ====================================================================
get_redirect_url(OldURL, Headers) ->
    Location = proplists:get_value("location", Headers),
    case Location of
        "http://" ++ _ -> Location;
        "https://" ++ _ -> Location;
        [$/ | _] = Location ->
            #url{protocol = Protocol, host = Host, port = Port} = ibrowse_lib:parse_url(OldURL),
            PortFrag = case {Protocol, Port} of
                           {http, 80} -> "";
                           {https, 443} -> "";
                           _ -> ":" ++ integer_to_list(Port)
                       end,
            atom_to_list(Protocol) ++ "://" ++ Host ++ PortFrag ++ Location;
        _ -> undefined
    end.


%% normalise_response/1
%% ====================================================================
%% @doc
%% Standarizes HTTP response, e.g. transforms header names to lower case.
%% @end
-spec normalise_response({ok, string(), list(), list()}) -> {ok, integer(), list(), list()}.
%% ====================================================================
normalise_response({ok, RcodeList, Headers, Body}) ->
    RcodeInt = list_to_integer(RcodeList),
    LowHeaders = [{string:to_lower(K), V} || {K, V} <- Headers],
    {ok, RcodeInt, LowHeaders, Body};

normalise_response(X) -> X.


%% parse_teams/1
%% ====================================================================
%% @doc
%% Parses user's teams from XML to a list of strings.
%% @end
-spec parse_teams(string()) -> [string()].
%% ====================================================================
parse_teams(XMLContent) ->
    {XML, _} = xmerl_scan:string(XMLContent),
    #xmlElement{content = TeamList} = find_XML_node(teams, XML),
    Res = lists:map(
        fun(#xmlElement{content = [#xmlText{value = Value}]}) ->
            Value
        end, TeamList).


%% find_XML_node/2
%% ====================================================================
%% @doc
%% Finds certain XML node. Assumes that node exists, and checks only
%% the first child of every node going deeper and deeper.
%% @end
-spec find_XML_node(atom(), #xmlElement{}) -> [string()].
%% ====================================================================
find_XML_node(NodeName, #xmlElement{name = NodeName} = XMLElement) ->
    XMLElement;

find_XML_node(NodeName, #xmlElement{} = XMLElement) ->
    [SubNode] = XMLElement#xmlElement.content,
    find_XML_node(NodeName, SubNode);

find_XML_node(NodeName, _) ->
    undefined.


