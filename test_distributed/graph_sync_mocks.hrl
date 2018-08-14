%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common macros for gs_channel tests.
%%% @end
%%%-------------------------------------------------------------------
-author("Lukasz Opiola").

-ifndef(GS_CHANNEL_MOCKS_HRL).
-define(GS_CHANNEL_MOCKS_HRL, 1).

-include("graph_sync/graph_sync.hrl").

-define(GS_PORT, 8443).
-define(GS_LISTENER_ID, "gs_listener_id").
-define(GS_HTTPS_ACCEPTORS, 100).

% Current protocol version
-define(SUPPORTED_PROTO_VERSIONS, [1, 2]).

-define(GS_EXAMPLE_TRANSLATOR, gs_example_translator).

-define(USER_AUTH(__UserId), {user_auth, __UserId}).
-define(PROVIDER_AUTH(__ProviderId), {provider_auth, __ProviderId}).
-define(NOBODY_AUTH, nobody).
-define(ROOT_AUTH, root).

-define(USER_1, <<"user1Id">>).
-define(USER_2, <<"user2Id">>).

-define(USER_1_COOKIE, <<"user1Cookie">>).
-define(USER_2_COOKIE, <<"user2Cookie">>).

-define(USER_DATA_WITHOUT_GRI(__UserId), case __UserId of
    ?USER_1 -> #{<<"name">> => <<"mockUser1Name">>};
    ?USER_2 -> #{<<"name">> => <<"mockUser2Name">>}
end).

-define(USER_NAME_THAT_CAUSES_NO_ACCESS_THROUGH_SPACE, <<"noAccessName">>).

-define(GROUP_1, <<"group1Id">>).
-define(GROUP_1_NAME, <<"group1Name">>).

-define(SPACE_1, <<"space1Id">>).
-define(SPACE_1_NAME, <<"space1Name">>).

-define(PROVIDER_1, <<"provider1Id">>).

-define(PROVIDER_1_MACAROON, <<"provider1macaroon">>).





-endif.