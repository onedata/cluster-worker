%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains mocks for callbacks used by gs_server.
%%% @end
%%%-------------------------------------------------------------------
-module(graph_sync_mocks).
-author("Lukasz Opiola").

-include_lib("ctool/include/api_errors.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("graph_sync/graph_sync.hrl").
-include("graph_sync_mocks.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    mock_callbacks/1,
    unmock_callbacks/1
]).
-export([
    authorize/1,
    client_to_identity/1,
    client_connected/2,
    client_disconnected/2,
    verify_auth_override/1,
    is_authorized/5,
    root_client/0,
    guest_client/0,
    handle_rpc/4,
    handle_graph_request/6,
    subscribable_resources/1
]).
-export([
    translate_resource/3,
    translate_value/3
]).


mock_callbacks(Config) ->
    Nodes = ?config(cluster_worker_nodes, Config),

    ok = test_utils:mock_new(Nodes, ?GS_LOGIC_PLUGIN, [non_strict]),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, authorize, fun authorize/1),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, client_to_identity, fun client_to_identity/1),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, client_connected, fun client_connected/2),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, client_disconnected, fun client_disconnected/2),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, verify_auth_override, fun verify_auth_override/1),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, is_authorized, fun is_authorized/5),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, root_client, fun root_client/0),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, guest_client, fun guest_client/0),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, handle_rpc, fun handle_rpc/4),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, handle_graph_request, fun handle_graph_request/6),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, subscribable_resources, fun subscribable_resources/1),

    ok = test_utils:mock_new(Nodes, ?GS_EXAMPLE_TRANSLATOR, [non_strict]),
    ok = test_utils:mock_expect(Nodes, ?GS_EXAMPLE_TRANSLATOR, handshake_attributes, fun handshake_attributes/1),
    ok = test_utils:mock_expect(Nodes, ?GS_EXAMPLE_TRANSLATOR, translate_resource, fun translate_resource/3),
    ok = test_utils:mock_expect(Nodes, ?GS_EXAMPLE_TRANSLATOR, translate_value, fun translate_value/3),

    ok = test_utils:mock_new(Nodes, datastore_config_plugin, [non_strict]),
    ok = test_utils:mock_expect(Nodes, datastore_config_plugin, get_throttled_models, fun get_throttled_models/0),

    ok.


unmock_callbacks(Config) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    test_utils:mock_unload(Nodes, ?GS_LOGIC_PLUGIN),
    test_utils:mock_unload(Nodes, ?GS_EXAMPLE_TRANSLATOR),
    test_utils:mock_unload(Nodes, datastore_config_plugin).


authorize(Req) ->
    case parse_macaroons_from_headers(Req) of
        {undefined, _} ->
            case proplists:get_value(?SESSION_COOKIE_NAME, cowboy_req:parse_cookies(Req)) of
                undefined ->
                    {ok, ?NOBODY_AUTH};
                ?USER_1_COOKIE ->
                    {ok, ?USER_AUTH(?USER_1)};
                ?USER_2_COOKIE ->
                    {ok, ?USER_AUTH(?USER_2)};
                _ ->
                    ?ERROR_UNAUTHORIZED
            end;
        {Macaroon, DischMacaroons} ->
            case verify_auth_override({macaroon, Macaroon, DischMacaroons}) of
                {ok, Client} ->
                    {ok, Client};
                Error ->
                    Error
            end
    end.


verify_auth_override({macaroon, ?PROVIDER_1_MACAROON, []}) ->
    {ok, ?PROVIDER_AUTH(?PROVIDER_1)};
verify_auth_override(_) ->
    ?ERROR_UNAUTHORIZED.


client_to_identity(?NOBODY_AUTH) -> nobody;
client_to_identity(?USER_AUTH(UId)) -> {user, UId};
client_to_identity(?PROVIDER_AUTH(PId)) -> {provider, PId}.


root_client() -> ?ROOT_AUTH.


guest_client() -> ?NOBODY_AUTH.


client_connected(_, _) -> ok.


client_disconnected(_, _) -> ok.


is_authorized(?USER_AUTH(UserId), _AuthHint, #gri{type = od_user, id = UserId}, _Operation, _Entity) ->
    true;
is_authorized(?USER_AUTH(_OtherUserId), ?THROUGH_SPACE(?SPACE_1), #gri{type = od_user, id = _UserId}, get, UserData) ->
    case UserData of
        % Used to test nosub push message
        #{<<"name">> := ?USER_NAME_THAT_CAUSES_NO_ACCESS_THROUGH_SPACE} ->
            false;
        _ ->
            true
    end;
is_authorized(_, _, _, _, _) ->
    false.


handle_rpc(_, ?USER_AUTH(?USER_1), <<"user1Fun">>, Args) ->
    {ok, Args};
handle_rpc(_, _, <<"user1Fun">>, _Args) ->
    ?ERROR_FORBIDDEN;
handle_rpc(_, ?USER_AUTH(?USER_2), <<"user2Fun">>, Args) ->
    {ok, Args};
handle_rpc(_, _, <<"user2Fun">>, _Args) ->
    ?ERROR_FORBIDDEN;
handle_rpc(_, _, _, _) ->
    ?ERROR_RPC_UNDEFINED.


handle_graph_request(Client, AuthHint, #gri{type = od_user, id = UserId, aspect = instance}, get, _Data, Entity) ->
    UserData = case Entity of
        undefined ->
            ?USER_DATA_WITHOUT_GRI(UserId);
        Fetched ->
            % Used in gs_server:updated
            Fetched
    end,
    case Client of
        ?ROOT_AUTH -> {ok, UserData};
        ?USER_AUTH(UserId) -> {ok, UserData};
        ?USER_AUTH(_OtherUser) ->
            case AuthHint of
                undefined -> ?ERROR_FORBIDDEN;
                ?THROUGH_SPACE(?SPACE_1) -> {ok, UserData}
            end
    end;
handle_graph_request(Client, _, #gri{type = od_user, id = UserId, aspect = instance}, update, Data, _Entity) ->
    case Client of
        ?USER_AUTH(UserId) ->
            case Data of
                #{<<"name">> := NewName} when is_binary(NewName) ->
                    % Updates are typically asynchronous
                    spawn(fun() ->
                        gs_server:updated(
                            od_user, UserId, #{<<"name">> => NewName}
                        )
                    end),
                    ok;
                #{<<"name">> := _} ->
                    ?ERROR_BAD_VALUE_BINARY(<<"name">>);
                _ ->
                    ?ERROR_MISSING_REQUIRED_VALUE(<<"name">>)
            end;
        _ ->
            ?ERROR_FORBIDDEN
    end;
handle_graph_request(Client, _, #gri{type = od_user, id = UserId, aspect = instance}, delete, _Data, _Entity) ->
    case Client of
        ?USER_AUTH(UserId) ->
            gs_server:deleted(
                od_user, UserId
            ),
            ok;
        _ ->
            ?ERROR_FORBIDDEN
    end;

handle_graph_request(?USER_AUTH(UserId), AuthHint, #gri{type = od_group, id = undefined, aspect = instance}, create, Data, _Entity) ->
    #{<<"name">> := ?GROUP_1_NAME} = Data,
    case AuthHint of
        ?AS_USER(UserId) ->
            {ok, resource, {#gri{type = od_group, id = ?GROUP_1, aspect = instance}, #{<<"name">> => ?GROUP_1_NAME}}};
        _ ->
            ?ERROR_FORBIDDEN
    end;

handle_graph_request(?USER_AUTH(UserId), AuthHint, #gri{type = od_space, id = undefined, aspect = instance}, create, Data, _Entity) ->
    #{<<"name">> := ?SPACE_1_NAME} = Data,
    case AuthHint of
        ?AS_USER(UserId) ->
            {ok, resource, {#gri{type = od_space, id = ?SPACE_1, aspect = instance}, #{<<"name">> => ?SPACE_1_NAME}}};
        _ ->
            ?ERROR_FORBIDDEN
    end;
handle_graph_request(?USER_AUTH(?USER_1), _, #gri{type = od_space, id = ?SPACE_1, aspect = instance}, get, _Data, _Entity) ->
    {ok, #{<<"name">> => ?SPACE_1_NAME}};

handle_graph_request(_, _, _, _, _, _) ->
    ?ERROR_NOT_FOUND.


subscribable_resources(od_user) -> [
    {instance, private}
];
subscribable_resources(_) -> [
].


handshake_attributes(_) ->
    #{}.


translate_resource(_, _GRI, Data) ->
    Data.


translate_value(_, _GRI, Data) ->
    Data.

get_throttled_models() ->
    [].


-spec parse_macaroons_from_headers(Req :: cowboy_req:req()) ->
    {Macaroon :: binary() | undefined, DischargeMacaroons :: [binary()]} |
    no_return().
parse_macaroons_from_headers(Req) ->
    MacaroonHeader = cowboy_req:header(<<"macaroon">>, Req),
    XAuthTokenHeader = cowboy_req:header(<<"x-auth-token">>, Req),
    % X-Auth-Token is an alias for macaroon header, check if any of them
    % is given.
    SerializedMacaroon = case MacaroonHeader of
        <<_/binary>> ->
            MacaroonHeader;
        _ ->
            XAuthTokenHeader % binary() or undefined
    end,

    DischargeMacaroons = case cowboy_req:header(<<"discharge-macaroons">>, Req) of
        undefined ->
            [];
        <<"">> ->
            [];
        SerializedDischarges ->
            binary:split(SerializedDischarges, <<" ">>, [global])
    end,

    {SerializedMacaroon, DischargeMacaroons}.
