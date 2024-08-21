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
-include_lib("ctool/include/aai/aai.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/test/test_utils.hrl").

-export([
    mock_callbacks/1,
    unmock_callbacks/1,
    mock_max_scope_towards_handle_service/3
]).
-export([
    verify_handshake_auth/3,
    client_connected/2,
    client_heartbeat/2,
    client_disconnected/2,
    verify_auth_override/2,
    is_authorized/5,
    encode_entity_type/1,
    decode_entity_type/1,
    handle_rpc/4,
    handle_graph_request/6,
    is_subscribable/1,
    simulate_service_availability/2
]).
-export([
    translate_resource/3,
    translate_value/3
]).

-define(GS_LOGIC_PLUGIN, (gs_server:gs_logic_plugin_module())).

mock_callbacks(Config) ->
    Nodes = ?config(cluster_worker_nodes, Config),

    ok = test_utils:mock_new(Nodes, ?GS_LOGIC_PLUGIN, [non_strict]),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, assert_service_available, fun assert_service_available/0),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, verify_handshake_auth, fun verify_handshake_auth/3),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, client_connected, fun client_connected/2),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, client_heartbeat, fun client_heartbeat/2),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, client_disconnected, fun client_disconnected/2),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, verify_auth_override, fun verify_auth_override/2),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, is_authorized, fun is_authorized/5),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, handle_rpc, fun handle_rpc/4),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, handle_graph_request, fun handle_graph_request/6),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, is_subscribable, fun is_subscribable/1),
    ok = test_utils:mock_expect(Nodes, ?GS_LOGIC_PLUGIN, is_type_supported, fun is_type_supported/1),

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
    test_utils:mock_unload(Nodes, datastore_config_plugin),

    test_utils:mock_unload([node()], ?GS_LOGIC_PLUGIN).


assert_service_available() ->
    case application:get_env(?CLUSTER_WORKER_APP_NAME, mocked_service_availability, true) of
        false -> throw(?ERROR_SERVICE_UNAVAILABLE);
        true -> ok
    end.


verify_handshake_auth({token, ?USER_1_TOKEN}, _, _) ->
    {ok, ?USER(?USER_1)};
verify_handshake_auth({token, ?USER_2_TOKEN}, _, _) ->
    {ok, ?USER(?USER_2)};
verify_handshake_auth({token, ?PROVIDER_1_TOKEN}, _, _) ->
    {ok, ?PROVIDER(?PROVIDER_1)};
verify_handshake_auth({token, ?USER_1_TOKEN_REQUIRING_COOKIES}, _, ?DUMMY_COOKIES) ->
    {ok, ?USER(?USER_1)};
verify_handshake_auth(undefined, _, _) ->
    {ok, ?NOBODY};
verify_handshake_auth(_, _, _) ->
    ?ERROR_UNAUTHORIZED.


% Proto version 3 does not support additional auth override options
% (just the client_auth), so all fields are undefined.
% Check if none of the fields are blacklisted - this way both versions can be tested.
verify_auth_override(_Auth, AuthOverride) ->
    #auth_override{
        client_auth = ClientAuth,
        peer_ip = PeerIp,
        interface = Interface,
        consumer_token = ConsumerToken
    } = AuthOverride,

    CorrectData = (PeerIp /= ?BLACKLISTED_IP) andalso
        (Interface /= ?BLACKLISTED_INTERFACE) andalso
        (ConsumerToken /= ?BLACKLISTED_CONSUMER_TOKEN),

    case CorrectData of
        false ->
            ?ERROR_UNAUTHORIZED;
        true ->
            try
                {ok, client_auth_to_auth(ClientAuth)}
            catch _:_ ->
                ?ERROR_UNAUTHORIZED
            end
    end.


client_auth_to_auth({token, ?USER_1_TOKEN}) -> ?USER(?USER_1);
client_auth_to_auth({token, ?USER_2_TOKEN}) -> ?USER(?USER_2);
client_auth_to_auth({token, ?PROVIDER_1_TOKEN}) -> ?PROVIDER(?PROVIDER_1);
client_auth_to_auth(nobody) -> ?NOBODY.


client_connected(_Auth, _) -> ok.


client_heartbeat(_Auth, _) -> ok.


client_disconnected(_Auth, _) -> ok.


mock_max_scope_towards_handle_service(Config, UserId, MaxScope) ->
    Nodes = ?config(cluster_worker_nodes, Config),
    CurrentMock = rpc:call(hd(Nodes), application, get_env, [?CLUSTER_WORKER_APP_NAME, mock_hservice_scope, #{}]),
    test_utils:set_env(Nodes, ?CLUSTER_WORKER_APP_NAME, mock_hservice_scope, CurrentMock#{UserId => MaxScope}).


is_authorized(?ROOT, _, GRI, _, _) ->
    {true, GRI};
is_authorized(?USER(UserId), _AuthHint, GRI = #gri{type = od_user, id = UserId}, _Operation, _Entity) ->
    {true, GRI};
is_authorized(?USER(_OtherUserId), ?THROUGH_SPACE(?SPACE_1), GRI = #gri{type = od_user, id = _UserId}, get, {UserData, _}) ->
    case UserData of
        % Used to test nosub push message
        #{<<"name">> := ?USER_NAME_THAT_CAUSES_NO_ACCESS_THROUGH_SPACE} ->
            false;
        _ ->
            {true, GRI}
    end;
is_authorized(?USER(UserId), _, GRI = #gri{type = od_handle_service, id = ?HANDLE_SERVICE, aspect = instance}, _, _) ->
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


handle_rpc(_, ?USER(?USER_1), <<"user1Fun">>, Args) ->
    {ok, Args};
handle_rpc(_, _, <<"user1Fun">>, _Args) ->
    ?ERROR_FORBIDDEN;
handle_rpc(_, ?USER(?USER_2), <<"user2Fun">>, Args) ->
    {ok, Args};
handle_rpc(_, _, <<"user2Fun">>, _Args) ->
    ?ERROR_FORBIDDEN;
handle_rpc(_, _, <<"veryLongOperation">>, Args) ->
    timer:sleep(15000 + rand:uniform(10000)),
    {ok, Args};
handle_rpc(_, _, _, _) ->
    ?ERROR_RPC_UNDEFINED.


handle_graph_request(Auth, AuthHint, #gri{type = od_user, id = UserId, aspect = instance}, get, _Data, VersionedEntity) ->
    Result = case VersionedEntity of
        {undefined, _} ->
            {?USER_DATA_WITHOUT_GRI(UserId), 1};
        {_, _} ->
            % Used in gs_server:updated
            VersionedEntity
    end,
    case Auth of
        ?ROOT -> {ok, Result};
        ?USER(UserId) -> {ok, Result};
        ?USER(_OtherUser) ->
            case AuthHint of
                undefined -> ?ERROR_FORBIDDEN;
                ?THROUGH_SPACE(?SPACE_1) -> {ok, Result}
            end
    end;
handle_graph_request(Auth, _, #gri{type = od_user, id = UserId, aspect = instance}, update, Data, _Entity) ->
    case Auth of
        ?USER(UserId) ->
            case Data of
                #{<<"name">> := NewName} when is_binary(NewName) ->
                    % Updates are typically asynchronous
                    spawn(fun() ->
                        timer:sleep(100),
                        gs_server:updated(
                            od_user, UserId, {#{<<"name">> => NewName}, 2}
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
handle_graph_request(Auth, _, #gri{type = od_user, id = UserId, aspect = instance}, delete, _Data, _Entity) ->
    case Auth of
        ?USER(UserId) ->
            % Updates are typically asynchronous
            spawn(fun() ->
                timer:sleep(100),
                gs_server:deleted(
                    od_user, UserId
                )
            end),
            ok;
        _ ->
            ?ERROR_FORBIDDEN
    end;

handle_graph_request(?USER(UserId), AuthHint, #gri{type = od_group, id = undefined, aspect = instance}, create, Data, _Entity) ->
    #{<<"name">> := ?GROUP_1_NAME} = Data,
    case AuthHint of
        ?AS_USER(UserId) ->
            {ok, resource, {#gri{type = od_group, id = ?GROUP_1, aspect = instance}, {#{<<"name">> => ?GROUP_1_NAME}, 1}}};
        _ ->
            ?ERROR_FORBIDDEN
    end;

handle_graph_request(?USER(?USER_1), _AuthHint, #gri{type = od_group, id = ?GROUP_1, aspect = int_value}, create, Data, _Entity) ->
    #{<<"value">> := Value} = Data,
    {ok, value, binary_to_integer(Value)};

handle_graph_request(?USER(UserId), AuthHint, #gri{type = od_space, id = undefined, aspect = instance}, create, Data, _Entity) ->
    #{<<"name">> := ?SPACE_1_NAME} = Data,
    case AuthHint of
        ?AS_USER(UserId) ->
            {ok, resource, {#gri{type = od_space, id = ?SPACE_1, aspect = instance}, {#{<<"name">> => ?SPACE_1_NAME}, 1}}};
        _ ->
            ?ERROR_FORBIDDEN
    end;
handle_graph_request(?USER(?USER_1), _, #gri{type = od_space, id = ?SPACE_1, aspect = instance}, get, _Data, _Entity) ->
    {ok, {#{<<"name">> => ?SPACE_1_NAME}, 1}};

handle_graph_request(Auth, _AuthHint, #gri{type = od_user, id = UserId, aspect = {name_substring, LenBin}}, get, _Data, VersionedEntity) ->
    Len = binary_to_integer(LenBin),
    {UserName, Revision} = case VersionedEntity of
        {undefined, _} ->
            {maps:get(<<"name">>, ?USER_DATA_WITHOUT_GRI(UserId)), 1};
        {Entity, Rev} ->
            % Used in gs_server:updated
            {maps:get(<<"name">>, Entity), Rev}
    end,
    NameSubstring = binary:part(UserName, 0, Len),
    case Auth of
        ?ROOT -> {ok, {#{<<"nameSubstring">> => NameSubstring}, Revision}};
        ?USER(UserId) -> {ok, {#{<<"nameSubstring">> => NameSubstring}, Revision}};
        _ -> ?ERROR_FORBIDDEN
    end;

handle_graph_request(Auth, _, GRI = #gri{type = od_handle_service, id = ?HANDLE_SERVICE, aspect = instance}, create, _, _) ->
    case is_authorized(Auth, undefined, GRI, create, {#{}, 1}) of
        false ->
            ?ERROR_FORBIDDEN;
        {true, #gri{scope = ResScope}} ->
            Data = ?HANDLE_SERVICE_DATA(<<"pub1">>, <<"sha1">>, <<"pro1">>, <<"pri1">>),
            {ok, resource, {GRI#gri{scope = ResScope}, {?LIMIT_HANDLE_SERVICE_DATA(ResScope, Data), 1}}}
    end;

handle_graph_request(Auth, _, GRI = #gri{type = od_handle_service, id = ?HANDLE_SERVICE, aspect = instance}, get, _, VersionedEntity) ->
    {Data, Revision} = case VersionedEntity of
        {undefined, _} ->
            {?HANDLE_SERVICE_DATA(<<"pub1">>, <<"sha1">>, <<"pro1">>, <<"pri1">>), 1};
        {_, _} ->
            % Used in gs_server:updated
            VersionedEntity
    end,
    case is_authorized(Auth, undefined, GRI, get, {#{}, Revision}) of
        false ->
            ?ERROR_FORBIDDEN;
        {true, GRI} ->
            {ok, {?LIMIT_HANDLE_SERVICE_DATA(GRI#gri.scope, Data), Revision}};
        {true, ResultGRI = #gri{scope = Scope}} ->
            {ok, ResultGRI, {?LIMIT_HANDLE_SERVICE_DATA(Scope, Data), Revision}}
    end;

handle_graph_request(Auth, _, #gri{type = od_share, id = ?SHARE, aspect = instance, scope = RequestedScope}, get, _, _) ->
    Scope = case {Auth, RequestedScope} of
        {?USER(_), auto} -> private;
        {?PROVIDER(_), auto} -> private;
        {?NOBODY, auto} -> public;
        {_, Other} -> Other
    end,
    Data = ?SHARE_DATA(atom_to_binary(Scope, utf8)),
    Revision = 1,
    case {Auth, Scope} of
        {?USER(_), _} ->
            {ok, {Data, Revision}};
        {?PROVIDER(_), _} ->
            {ok, {Data, Revision}};
        {?NOBODY, public} ->
            {ok, {Data, Revision}};
        {?NOBODY, _} ->
            ?ERROR_FORBIDDEN
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


simulate_service_availability(Nodes, IsServiceAvailable) ->
    test_utils:set_env(Nodes, ?CLUSTER_WORKER_APP_NAME, mocked_service_availability, IsServiceAvailable).


is_type_supported(#gri{type = od_user}) -> true;
is_type_supported(#gri{type = od_group}) -> true;
is_type_supported(#gri{type = od_space}) -> true;
is_type_supported(#gri{type = od_share}) -> true;
is_type_supported(#gri{type = od_handle_service}) -> true;
is_type_supported(#gri{type = _}) -> false.


handshake_attributes(_) ->
    #{}.


% Test both ways of implementing the translator - returning exact value or a fun
% to evaluate.
translate_resource(_, _GRI, Data) ->
    case rand:uniform(2) of
        1 -> Data;
        2 -> fun(_Auth) -> Data end
    end.


% Test both ways of implementing the translator - returning exact value or a fun
% to evaluate.
translate_value(_, _GRI, Data) ->
    case rand:uniform(2) of
        1 -> Data;
        2 -> fun(_Auth) -> Data end
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
