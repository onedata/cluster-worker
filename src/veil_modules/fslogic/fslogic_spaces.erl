%%%-------------------------------------------------------------------
%%% @author RoXeon
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Jul 2014 13:46
%%%-------------------------------------------------------------------
-module(fslogic_spaces).
-author("RoXeon").

-include("veil_modules/dao/dao.hrl").
-include("files_common.hrl").

%% API
-export([initialize/1, map_to_grp_owner/1]).

initialize(#space_info{uuid = SpaceId, name = SpaceName} = SpaceInfo) ->
    case user_logic:create_space_dir(SpaceInfo) of
        {ok, SpaceUUID} ->
            try user_logic:create_dirs_at_storage(non, [SpaceInfo]) of
                ok -> {ok, SpaceInfo};
                {error, Reason} ->
                    throw(Reason)
            catch
                Type:Error ->
                    dao_lib:apply(dao_vfs, remove_file, [{uuid, SpaceUUID}], 1),
                    {error, {Type, Error}}
            end,
            {ok, SpaceInfo};
        {error, dir_exists} ->
            {ok, #veil_document{record = #file{extensions = Ext} = File} = FileDoc} = dao_lib:apply(dao_vfs, get_space_file, [{uuid, SpaceId}], 1),
            NewExt = lists:keyreplace(?file_space_info_extestion, 1, Ext, {?file_space_info_extestion, SpaceInfo}),
            NewFile = File#file{extensions = NewExt},
            {ok, _} = dao_lib:apply(vfs, save_file, [FileDoc#veil_document{record = NewFile}], 1),
            {ok, SpaceInfo};
        {error, Reason} ->
            {error, Reason}
    end;
initialize(SpaceId) ->
    case registry_spaces:get_space_info(SpaceId) of
        {ok, #space_info{} = SpaceInfo} ->
            initialize(SpaceInfo);
        {error, Reason} ->
            {error, Reason}
    end.

map_to_grp_owner([]) ->
    [];
map_to_grp_owner([SpaceInfo | T]) ->
    [map_to_grp_owner(SpaceInfo)] ++ map_to_grp_owner(T);
map_to_grp_owner(#space_info{name = SpaceName, uuid = SpaceId}) ->
    case os:cmd("getent group " ++ SpaceName ++ " | cut -d: -f3") -- [10, 13] of
        "" ->
            <<GID0:16/big-unsigned-integer-unit:8>> = crypto:hash(md5, SpaceId),
            70000 + GID0 rem 1000000;
        StrGID ->
            list_to_integer(StrGID)
    end.