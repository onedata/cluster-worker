%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module implements worker_plugin_behaviour callbacks.
%% It is responsible for setting up cowboy listener and registering
%% handlers for n2o (GUI) and REST.
%% @end
%% ===================================================================

-module(control_panel).
-behaviour(worker_plugin_behaviour).

-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([init/1, handle/2, cleanup/0, gui_adjust_headers/1]).

% Paths in gui static directory
-define(static_paths, ["/css/", "/fonts/", "/images/", "/js/", "/n2o/"]).

% Cowboy listener reference
-define(https_listener, https).
-define(http_redirector_listener, http).
-define(rest_listener, rest).

%% ===================================================================
%% Behaviour callback functions
%% ===================================================================

%% init/1
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback init/1 <br />
%% Sets up cowboy handlers for GUI and REST.
%% @end
-spec init(Args :: term()) -> Result when
    Result :: ok | {error, Error},
    Error :: term().
%% ====================================================================
init(_Args) ->
    % Get params from env for gui
    {ok, DocRoot} = application:get_env(veil_cluster_node, control_panel_static_files_root),

    {ok, Cert} = application:get_env(veil_cluster_node, ssl_cert_path),
    CertString = atom_to_list(Cert),

    {ok, GuiPort} = application:get_env(veil_cluster_node, control_panel_port),
    {ok, GuiNbAcceptors} = application:get_env(veil_cluster_node, control_panel_number_of_acceptors),
    {ok, MaxKeepAlive} = application:get_env(veil_cluster_node, control_panel_max_keepalive),
    {ok, Timeout} = application:get_env(veil_cluster_node, control_panel_socket_timeout),

    % Setup GUI dispatch opts for cowboy
    GUIDispatch = [
        {'_', static_dispatches(atom_to_list(DocRoot), ?static_paths) ++ [
            {"/nagios/[...]", nagios_handler, []},
            {?user_content_download_path ++ "/:path", file_download_handler, [{type, ?user_content_request_type}]},
            {?shared_files_download_path ++ "/:path", file_download_handler, [{type, ?shared_files_request_type}]},
            {?file_upload_path, file_upload_handler, []},
            {"/ws/[...]", bullet_handler, [{handler, n2o_bullet}]},
            {'_', n2o_cowboy, []}
        ]}
    ],

    % Set envs needed by n2o
    % Transition port - the same as gui port
    ok = application:set_env(n2o, transition_port, GuiPort),
    % Custom route handler
    ok = application:set_env(n2o, route, gui_routes),

    % Ets tables needed by n2o
    ets:new(cookies, [set, named_table, {keypos, 1}, public]),
    ets:new(actions, [set, named_table, {keypos, 1}, public]),
    ets:new(globals, [set, named_table, {keypos, 1}, public]),
    ets:new(caching, [set, named_table, {keypos, 1}, public]),
    ets:insert(globals, {onlineusers, 0}),

    % Start the listener for web gui and nagios handler
    {ok, _} = cowboy:start_https(?https_listener, GuiNbAcceptors,
        [
            {port, GuiPort},
            {certfile, CertString},
            {keyfile, CertString},
            {cacerts, gsi_handler:strip_self_signed_ca(gsi_handler:get_ca_certs())},
            {password, ""}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(GUIDispatch)}]},
            {max_keepalive, MaxKeepAlive},
            {timeout, Timeout},
            {onrequest, fun control_panel:gui_adjust_headers/1}
        ]),


    {ok, RedirectPort} = application:get_env(veil_cluster_node, control_panel_redirect_port),
    {ok, RedirectNbAcceptors} = application:get_env(veil_cluster_node, control_panel_number_of_http_acceptors),
    % Start the listener that will redirect all requests of http to https
    RedirectDispatch = [
        {'_', [
            {'_', redirect_handler, []}
        ]}
    ],

    {ok, _} = cowboy:start_http(?http_redirector_listener, RedirectNbAcceptors,
        [
            {port, RedirectPort}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(RedirectDispatch)}]},
            {max_keepalive, 1},
            {timeout, Timeout}%,
            %{onrequest, fun control_panel:redirector_adjust_headers/1}
        ]),


    % Get REST port from env and setup dispatch opts for cowboy
    {ok, RestPort} = application:get_env(veil_cluster_node, rest_port),
    RestDispatch = [
        {'_', [
            {"/rest/:version/[...]", rest_handler, []}
        ]}
    ],
    % Start the listener for REST handler
    {ok, _} = cowboy:start_https(?rest_listener, GuiNbAcceptors,
        [
            {port, RestPort},
            {certfile, CertString},
            {keyfile, CertString},
            {cacerts, gsi_handler:strip_self_signed_ca(gsi_handler:get_ca_certs())},
            {password, ""},
            {verify, verify_peer}, {verify_fun, {fun gsi_handler:verify_callback/3, []}}
        ],
        [
            {env, [{dispatch, cowboy_router:compile(RestDispatch)}]},
            {max_keepalive, 1},
            {timeout, Timeout}%,
            %{onrequest, fun control_panel:rest_adjust_headers/1}
        ]),
    ok.


%% handle/2
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback handle/1
-spec handle(ProtocolVersion :: term(), Request) -> Result when
    Request :: ping | healthcheck | get_version,
    Result :: ok | {ok, Response} | {error, Error} | pong | Version,
    Response :: term(),
    Version :: term(),
    Error :: term().
%% ====================================================================
handle(_ProtocolVersion, ping) ->
    pong;

handle(_ProtocolVersion, healthcheck) ->
    ok;

handle(_ProtocolVersion, get_version) ->
    node_manager:check_vsn();

handle(_ProtocolVersion, _Msg) ->
    ok.

%% cleanup/0
%% ====================================================================
%% @doc {@link worker_plugin_behaviour} callback cleanup/0 <br />
%% Stops cowboy listener and terminates
%% @end
-spec cleanup() -> Result when
    Result :: ok | {error, Error},
    Error :: timeout | term().
%% ====================================================================
cleanup() ->
    cowboy:stop_listener(?https_listener),
    cowboy:stop_listener(?rest_listener),
    cowboy:stop_listener(?http_redirector_listener),
    ets:delete(cookies),
    ets:delete(actions),
    ets:delete(globals),
    ets:delete(caching),
    ok.


%% gui_adjust_headers/1
%% ====================================================================
%% @doc Callback hook for cowboy to modify response headers for HTTPS GUI.
%% @end
-spec gui_adjust_headers(Req :: req()) -> req().
%% ====================================================================
gui_adjust_headers(Req) ->
    Req2 = cowboy_req:set_resp_header(<<"Strict-Transport-Security">>, <<"max-age=31536000; includeSubDomains">>, Req),
    _Req3 = cowboy_req:set_resp_header(<<"X-Frame-Options">>, <<"SAMEORIGIN">>, Req2).


%% ====================================================================
%% Auxiliary functions
%% ====================================================================

%% Generates static file routing for cowboy.
static_dispatches(DocRoot, StaticPaths) ->
    _StaticDispatches = lists:map(fun(Dir) ->
        {Dir ++ "[...]", cowboy_static, {dir, DocRoot ++ Dir}}
    end, StaticPaths).
