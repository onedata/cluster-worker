%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides and manages fslogic context information
%%       such as protocol version or user's DN
%% @end
%% ===================================================================
-module(fslogic_context).
-author("Rafal Slota").

-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao.hrl").

%% API
-export([get_fuse_id/0, set_fuse_id/1, get_user_dn/0, set_user_dn/1, set_protocol_version/1, get_protocol_version/0, get_user_id/0]).

%% ====================================================================
%% API functions
%% ====================================================================

%% get_user_dn/0
%% ====================================================================
%% @doc Gets user's DN for current request or 'undefined' when there is none.
-spec get_user_dn() -> Result when Result :: atom() | undefined.
%% ====================================================================
get_user_dn() ->
    get(user_dn).


%% set_user_dn/1
%% ====================================================================
%% @doc Sets user's DN for current request.
-spec set_user_dn(UserDN :: term()) -> Result when Result :: term().
%% ====================================================================
set_user_dn(UserDN) ->
    put(user_dn, UserDN).


%% get_fuse_id/0
%% ====================================================================
%% @doc Gets Fuse ID for current request or 'undefined' when there is none.
-spec get_fuse_id() -> Result when Result :: term() | undefined.
%% ====================================================================
get_fuse_id() ->
    get(fuse_id).


%% set_fuse_id/1
%% ====================================================================
%% @doc Sets Fuse ID for current request.
-spec set_fuse_id(FuseID :: term()) -> Result when Result :: term().
%% ====================================================================
set_fuse_id(FuseID) ->
    put(fuse_id, FuseID).


%% set_protocol_version/1
%% ====================================================================
%% @doc Sets protocol version for current request.
-spec set_protocol_version(ProtocolVersion :: term()) -> OldValue when OldValue :: term().
%% ====================================================================
set_protocol_version(PVers) ->
    put(protocol_version, PVers).


%% get_protocol_version/0
%% ====================================================================
%% @doc Gets protocol version for current request or 'undefined' when there is none.
-spec get_protocol_version() -> Result when Result :: term() | undefined.
%% ====================================================================
get_protocol_version() ->
    get(protocol_version).


%% get_user_id/0
%% ====================================================================
%% @doc Gets user's id. If there's no user DN in current context, ?CLUSTER_USER_ID is returned
%% @end
-spec get_user_id() -> Result when
    Result :: {ok, UserID} | {error, ErrorDesc},
    UserID :: term(),
    ErrorDesc :: atom.
%% ====================================================================
get_user_id() ->
    UserDN = get_user_dn(),
    case UserDN of
        undefined -> {ok, ?CLUSTER_USER_ID};
        DN ->
            case fslogic_objects:get_user({dn, DN}) of
                {ok, #veil_document{uuid = UID}} -> {ok, UID};
                Error -> Error
            end
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
