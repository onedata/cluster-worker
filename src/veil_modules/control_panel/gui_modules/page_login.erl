%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page handles users' logging in.
%% @end
%% ===================================================================

-module(page_login).
-include("veil_modules/control_panel/common.hrl").
-include("logging.hrl").

% n2o API
-export([main/0, event/1]).

%% Template points to the template file, which will be filled with content
main() -> #dtl{file = "bare", app = veil_cluster_node, bindings = [{title, title()}, {body, body()}, {custom, <<"">>}]}.

%% Page title
title() -> <<"Login page">>.

%% This will be placed in the template instead of {{body}} tag
body() ->
    case gui_ctx:user_logged_in() of
        true -> gui_jq:redirect(<<"/">>);
        false ->
            ErrorPanelStyle = case gui_ctx:url_param(<<"x">>) of
                                  undefined -> <<"display: none;">>;
                                  _ -> <<"">>
                              end,
            #panel{style = <<"position: relative;">>, body = [
                #panel{id = <<"error_message">>, style = ErrorPanelStyle, class = <<"dialog dialog-danger">>, body = #p{
                    body = <<"No session or session expired. Please log in.">>}},
                #panel{class = <<"alert alert-success login-page">>, body = [
                    #h3{body = <<"Welcome to VeilFS">>},
                    #p{class = <<"login-info">>, body = <<"Logging in is handled by <b>PL-Grid OpenID</b>. ",
                    "You need to have an account and possibly VeilFS service enabled.">>},
                    #button{postback = login, class = <<"btn btn-primary btn-block">>, body = <<"Log in via PL-Grid OpenID">>}
                ]},
                gui_utils:cookie_policy_popup_body(?privacy_policy_url)
            ] ++ vcn_gui_utils:logotype_footer(120)
                % Logout from PLGrid if there is no active session - the user might still have a session there
                ++ [#p{body = <<"<iframe src=\"https://openid.plgrid.pl/logout\" style=\"display:none\"></iframe>">>}]}
    end.


event(init) -> ok;
% Login event handling
event(login) ->
    % Collect redirect param if present
    RedirectParam = case gui_ctx:url_param(<<"x">>) of
                        undefined -> <<"">>;
                        Val -> <<"?x=", Val/binary>>
                    end,
    % Resolve hostname, which was requested by a client
    Hostname = gui_ctx:get_requested_hostname(),
    case Hostname of
        undefined ->
            gui_jq:update(<<"error_message">>, <<"Cannot establish requested hostname. Please contact the site administrator.">>),
            gui_jq:fade_in(<<"error_message">>, 300);
        Host ->
            % Get redirect URL and redirect to OpenID login
            case openid_utils:get_login_url(Host, RedirectParam) of
                {error, _} ->
                    gui_jq:update(<<"error_message">>, <<"Unable to reach OpenID Provider. Please try again later.">>),
                    gui_jq:fade_in(<<"error_message">>, 300);
                URL ->
                    ?dump(URL),
                    gui_jq:redirect(URL)
            end
    end;

event(terminate) -> ok.