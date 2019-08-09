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
-define(SUPPORTED_PROTO_VERSIONS, [3, 4]).

-define(GS_EXAMPLE_TRANSLATOR, gs_example_translator).

-define(USER_1, <<"user1Id">>).
-define(USER_2, <<"user2Id">>).

-define(USER_1_TOKEN, <<"user1Token">>).
-define(USER_2_TOKEN, <<"user2Token">>).

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

-define(PROVIDER_1_TOKEN, <<"provider1token">>).

% Used for auto scope tests
-define(HANDLE_SERVICE, <<"handleService">>).

% Used for nobody auth override tests
-define(SHARE, <<"share">>).

-define(HANDLE_SERVICE_DATA(__Public, __Shared, __Protected, __Private), #{
    <<"public">> => __Public,
    <<"shared">> => __Shared,
    <<"protected">> => __Protected,
    <<"private">> => __Private
}).

-define(LIMIT_HANDLE_SERVICE_DATA(__Scope, __Data), case __Scope of
    private -> __Data;
    protected -> maps:without([<<"private">>], __Data);
    shared -> maps:without([<<"private">>, <<"protected">>], __Data);
    public -> maps:without([<<"private">>, <<"protected">>, <<"shared">>], __Data)
end).

-define(SHARE_DATA(ScopeBin), #{
    <<"scope">> => ScopeBin
}).
-define(SHARE_DATA_MATCHER(ScopeBin), #{
    <<"scope">> := ScopeBin
}).

-endif.