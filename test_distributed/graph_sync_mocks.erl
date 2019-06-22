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

-include("graph_sync_mocks.hrl").
-include("graph_sync/graph_sync.hrl").
-include("global_definitions.hrl").
-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/api_errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    mock_callbacks/1,
    unmock_callbacks/1,
    mock_max_scope_towards_handle_service/3
]).
-export([
    verify_handshake_auth/1,
    client_to_identity/1,
    client_connected/3,
    client_disconnected/3,
    verify_auth_override/2,
    is_authorized/5,
    root_client/0,
    encode_entity_type/1,
    decode_entity_type/1,
    guest_client/0,
    handle_rpc/4,
    handle_graph_request/6,
    is_subscribable/1
]).
-export([
    translate_resource/3,
    translate_value/3
]).


mock_callbacks(Config) ->
    Nodes = ?config(cluster_worker_nodes, Config),

    ok = test_utils:mock_new(Nodes, ?GS_LOGIC_PLUGIN, [non_strict]),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, verify_handshake_auth, fun verify_handshake_auth/1),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, client_to_identity, fun client_to_identity/1),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, client_connected, fun client_connected/3),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, client_disconnected, fun client_disconnected/3),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, verify_auth_override, fun verify_auth_override/2),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, is_authorized, fun is_authorized/5),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, root_client, fun root_client/0),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, guest_client, fun guest_client/0),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, handle_rpc, fun handle_rpc/4),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, handle_graph_request, fun handle_graph_request/6),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, is_subscribable, fun is_subscribable/1),

    ok = test_utils:mock_new(Nodes, ?GS_EXAMPLE_TRANSLATOR, [non_strict]),
    ok = test_utils:mock_expect(Nodes, ?GS_EXAMPLE_TRANSLATOR, handshake_attributes, fun handshake_attributes/1),
    ok = test_utils:mock_expect(Nodes, ?GS_EXAMPLE_TRANSLATOR, translate_resource, fun translate_resource/3),
    ok = test_utils:mock_expect(Nodes, ?GS_EXAMPLE_TRANSLATOR, translate_value, fun translate_value/3),

    ok = test_utils:mock_new(Nodes, datastore_config_plugin, [non_strict]),
    ok = test_utils:mock_expect(Nodes, datastore_config_plugin, get_throttled_models, fun get_throttled_models/0),

    ok = test_utils:mock_new(Nodes, ?GS_PROTOCOL_PLUGIN, [non_strict]),
    ok = test_utils:mock_expect(Nodes, ?GS_PROTOCOL_PLUGIN, encode_entity_type, fun encode_entity_type/1),
    ok = test_utils:mock_expect(Nodes, ?GS_PROTOCOL_PLUGIN, decode_entity_type, fun decode_entity_type/1),

    % GS_LOGIC_PLUGIN is called in gs_client on the testmaster node
    ok = test_utils:mock_new([node()], ?GS_PROTOCOL_PLUGIN, [non_strict]),
    ok = test_utils:mock_expect([node()], ?GS_PROTOCOL_PLUGIN, encode_entity_type, fun encode_entity_type/1),
    ok = test_utils:mock_expect([node()], ?GS_PROTOCOL_PLUGIN, decode_entity_type, fun decode_entity_type/1),

    ok.


unmock_callbacks(Config) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    test_utils:mock_unload(Nodes, ?GS_LOGIC_PLUGIN),
    test_utils:mock_unload(Nodes, ?GS_PROTOCOL_PLUGIN),
    test_utils:mock_unload(Nodes, ?GS_EXAMPLE_TRANSLATOR),
    test_utils:mock_unload(Nodes, datastore_config_plugin),

    test_utils:mock_unload([node()], ?GS_LOGIC_PLUGIN).


verify_handshake_auth({macaroon, ?USER_1_MACAROON, []}) ->
    {ok, ?USER_AUTH(?USER_1), ?CONNECTION_INFO(?USER_AUTH(?USER_1))};
verify_handshake_auth({macaroon, ?USER_2_MACAROON, []}) ->
    {ok, ?USER_AUTH(?USER_2), ?CONNECTION_INFO(?USER_AUTH(?USER_2))};
verify_handshake_auth({macaroon, ?PROVIDER_1_MACAROON, []}) ->
    {ok, ?PROVIDER_AUTH(?PROVIDER_1), ?CONNECTION_INFO(?PROVIDER_AUTH(?PROVIDER_1))};
verify_handshake_auth(undefined) ->
    {ok, ?NOBODY_AUTH, ?CONNECTION_INFO(?NOBODY_AUTH)};
verify_handshake_auth(_) ->
    ?ERROR_UNAUTHORIZED.


verify_auth_override(_Client, {macaroon, ?USER_1_MACAROON, []}) ->
    {ok, ?USER_AUTH(?USER_1)};
verify_auth_override(_Client, {macaroon, ?USER_2_MACAROON, []}) ->
    {ok, ?USER_AUTH(?USER_2)};
verify_auth_override(_Client, {macaroon, ?PROVIDER_1_MACAROON, []}) ->
    {ok, ?PROVIDER_AUTH(?PROVIDER_1)};
verify_auth_override(_Client, _) ->
    ?ERROR_UNAUTHORIZED.


client_to_identity(?NOBODY_AUTH) -> nobody;
client_to_identity(?USER_AUTH(UId)) -> {user, UId};
client_to_identity(?PROVIDER_AUTH(PId)) -> {provider, PId}.


root_client() -> ?ROOT_AUTH.


guest_client() -> ?NOBODY_AUTH.


client_connected(Client, ?CONNECTION_INFO(Client), _) -> ok.


client_disconnected(Client, ?CONNECTION_INFO(Client), _) -> ok.


mock_max_scope_towards_handle_service(Config, UserId, MaxScope) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    CurrentMock = rpc:call(hd(Nodes), application, get_env, [?CLUSTER_WORKER_APP_NAME, mock_hservice_scope, #{}]),
    test_utils:set_env(Nodes, ?CLUSTER_WORKER_APP_NAME, mock_hservice_scope, CurrentMock#{UserId => MaxScope}).


is_authorized(?ROOT_AUTH, _, GRI, _, _) ->
    {true, GRI};
is_authorized(?USER_AUTH(UserId), _AuthHint, GRI = #gri{type = od_user, id = UserId}, _Operation, _Entity) ->
    {true, GRI};
is_authorized(?USER_AUTH(_OtherUserId), ?THROUGH_SPACE(?SPACE_1), GRI = #gri{type = od_user, id = _UserId}, get, UserData) ->
    case UserData of
        % Used to test nosub push message
        #{<<"name">> := ?USER_NAME_THAT_CAUSES_NO_ACCESS_THROUGH_SPACE} ->
            false;
        _ ->
            {true, GRI}
    end;
is_authorized(?USER_AUTH(UserId), _, GRI = #gri{type = od_handle_service, id = ?HANDLE_SERVICE, aspect = instance}, _, _) ->
    MaxScope = maps:get(
        UserId,
        application:get_env(?CLUSTER_WORKER_APP_NAME, mock_hservice_scope, #{}),
        none
    ),
    case GRI of
        #gri{scope = auto} ->
            case MaxScope of
                none -> false;
                Scope -> {true, GRI#gri{scope = Scope}}
            end;
        #gri{scope = RequestedScope} ->
            case scope_to_int(RequestedScope) =< scope_to_int(MaxScope) of
                true -> {true, GRI};
                false -> false
            end
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

handle_graph_request(?USER_AUTH(?USER_1), _AuthHint, #gri{type = od_group, id = ?GROUP_1, aspect = int_value}, create, Data, _Entity) ->
    #{<<"value">> := Value} = Data,
    {ok, value, binary_to_integer(Value)};

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

handle_graph_request(Client, _AuthHint, #gri{type = od_user, id = UserId, aspect = {name_substring, LenBin}}, get, _Data, Entity) ->
    Len = binary_to_integer(LenBin),
    UserName = case Entity of
        undefined ->
            maps:get(<<"name">>, ?USER_DATA_WITHOUT_GRI(UserId));
        Fetched ->
            % Used in gs_server:updated
            maps:get(<<"name">>, Fetched)
    end,
    NameSubstring = binary:part(UserName, 0, Len),
    case Client of
        ?ROOT_AUTH -> {ok, #{<<"nameSubstring">> => NameSubstring}};
        ?USER_AUTH(UserId) -> {ok, #{<<"nameSubstring">> => NameSubstring}};
        _ -> ?ERROR_FORBIDDEN
    end;

handle_graph_request(Client, _, GRI = #gri{type = od_handle_service, id = ?HANDLE_SERVICE, aspect = instance}, create, _, _) ->
    case is_authorized(Client, undefined, GRI, create, #{}) of
        false ->
            ?ERROR_FORBIDDEN;
        {true, #gri{scope = ResScope}} ->
            Data = ?HANDLE_SERVICE_DATA(<<"pub1">>, <<"sha1">>, <<"pro1">>, <<"pri1">>),
            {ok, resource, {GRI#gri{scope = ResScope}, ?LIMIT_HANDLE_SERVICE_DATA(ResScope, Data)}}
    end;

handle_graph_request(Client, _, GRI = #gri{type = od_handle_service, id = ?HANDLE_SERVICE, aspect = instance}, get, _, Entity) ->
    Data = case Entity of
        undefined ->
            ?HANDLE_SERVICE_DATA(<<"pub1">>, <<"sha1">>, <<"pro1">>, <<"pri1">>);
        Fetched ->
            % Used in gs_server:updated
            Fetched
    end,
    case is_authorized(Client, undefined, GRI, get, #{}) of
        false ->
            ?ERROR_FORBIDDEN;
        {true, GRI} ->
            {ok, ?LIMIT_HANDLE_SERVICE_DATA(GRI#gri.scope, Data)};
        {true, ResultGRI = #gri{scope = Scope}} ->
            {ok, ResultGRI, ?LIMIT_HANDLE_SERVICE_DATA(Scope, Data)}
    end;

handle_graph_request(_, _, _, _, _, _) ->
    ?ERROR_NOT_FOUND.


is_subscribable(#gri{type = od_user, aspect = instance, scope = private}) ->
    true;
is_subscribable(#gri{type = od_user, aspect = {name_substring, _}, scope = private}) ->
    true;
is_subscribable(#gri{type = od_handle_service, aspect = instance}) ->
    true;
is_subscribable(_) ->
    false.


handshake_attributes(_) ->
    #{}.


% Test both ways of implementing the translator - returning exact value or a fun
% to evaluate.
translate_resource(_, _GRI, Data) ->
    case rand:uniform(2) of
        1 -> Data;
        2 -> fun(_Client) -> Data end
    end.


% Test both ways of implementing the translator - returning exact value or a fun
% to evaluate.
translate_value(_, _GRI, Data) ->
    case rand:uniform(2) of
        1 -> Data;
        2 -> fun(_Client) -> Data end
    end.

get_throttled_models() ->
    [].


encode_entity_type(Atom) -> atom_to_binary(Atom, utf8).


decode_entity_type(Bin) -> binary_to_atom(Bin, utf8).


scope_to_int(public) -> 1;
scope_to_int(shared) -> 2;
scope_to_int(protected) -> 3;
scope_to_int(private) -> 4;
scope_to_int(_) -> 0.
