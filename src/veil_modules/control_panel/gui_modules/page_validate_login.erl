%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page handles user validation via OpenID.
%% @end
%% ===================================================================

-module(page_validate_login).
-include("veil_modules/control_panel/common.hrl").
-include_lib("ctool/include/logging.hrl").

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
            LoginMessage = case plgrid_openid_utils:prepare_validation_parameters() of
                               {error, invalid_request} -> {error, invalid_request};
                               {EndpointURL, RequestBody} ->
                                   plgrid_openid_utils:validate_openid_login({EndpointURL, RequestBody})
                           end,

            case LoginMessage of
                {error, invalid_request} ->
                    page_error:redirect_with_error(?error_openid_invalid_request);

                {error, auth_invalid} ->
                    page_error:redirect_with_error(?error_openid_auth_invalid);

                {error, no_connection} ->
                    page_error:redirect_with_error(?error_openid_no_connection);

                ok ->
                    try
                        case plgrid_openid_utils:retrieve_user_info() of
                            {error, invalid_request} ->
                                page_error:redirect_with_error(?error_openid_login_error);
                            {ok, Proplist} ->
                                {Login, UserDoc} = user_logic:sign_in(Proplist, <<"">>),
                                LogoutToken = vcn_gui_utils:gen_logout_token(),
                                gui_ctx:create_session(),
                                gui_ctx:set_user_id(Login),
                                vcn_gui_utils:set_user_fullname(user_logic:get_name(UserDoc)),
                                vcn_gui_utils:set_user_role(user_logic:get_role(UserDoc)),
                                vcn_gui_utils:set_logout_token(LogoutToken),
                                gui_jq:redirect_from_login(),
                                ?debug("User ~p logged in", [Login])
                        end
                    catch
                        throw:dir_creation_error ->
                            ?error_stacktrace("Error in validate_login - ~p:~p", [throw, dir_creation_error]),
                            page_error:redirect_with_error(?error_login_dir_creation_error);
                        throw:dir_chown_error ->
                            ?error_stacktrace("Error in validate_login - ~p:~p", [throw, dir_chown_error]),
                            page_error:redirect_with_error(?error_login_dir_chown_error);
                        Type:Message ->
                            ?error_stacktrace("Error in validate_login - ~p:~p", [Type, Message]),
                            page_error:redirect_with_error(?error_internal_server_error)
                    end
            end,
            <<"">>
    end.

event(init) -> ok;
event(terminate) -> ok.