%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module consists of DAO objects/records accessors. <br/>
%%       All methods wrap DAO operations while adding some fslogic-specific logic
%%       and/or error translations.
%% @end
%% ===================================================================
-module(fslogic_objects).
-author("Rafal Slota").

-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao_types.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_users.hrl").

%% API
-export([get_file/1, get_waiting_file/1, get_file/3, get_waiting_file/3]).
-export([save_file_descriptor/3, save_file_descriptor/4, save_new_file_descriptor/4, update_file_descriptor/2, delete_old_descriptors/2]).
-export([get_user/0, get_user/1]).
-export([save_file/1, get_storage/1]).
-export([get_space/1]).

%% ====================================================================
%% API functions
%% ====================================================================


%% get_space/1
%% ====================================================================
%% @doc Convenience wrapper for dao_vfs:get_space_file that accepts wider range of arguments
%%      and returns errors compatible with fslogic's error handler. Also if requested space does not
%%      exist, this method tries to initialize one using GlobarRegistry thus error is returned only if
%%      space does not exists or there was an error during its initialization.
%% @end
-spec get_space(Query) -> {ok, SpaceInfo :: #space_info{}} | {error, {unknown_space_error | initialize_error, Reason :: any()}}
    when Query :: #veil_document{}
                | #file{}
                | #space_info{}
                | {uuid, SpaceId}
                | SpaceName,
         SpaceName :: binary(),
         SpaceId :: uuid().
%% ====================================================================
get_space(#veil_document{record = Record}) ->
    get_space(Record);
get_space(#file{extensions = Ext}) ->
    {_, #space_info{} = SpaceInfo} = lists:keyfind(?file_space_info_extestion, 1, Ext),
    get_space(SpaceInfo);
get_space(#space_info{} = SpaceInfo) ->
    {ok, SpaceInfo};
get_space({uuid, SpaceId}) ->
    case dao_lib:apply(vfs, get_space_file, [{uuid, SpaceId}], 1) of
        {ok, #veil_document{record = #file{} = File}} ->
            get_space(File);
        {error, file_not_found} ->
            try fslogic_spaces:initialize(SpaceId) of
                {ok, #space_info{} = SpaceInfo} ->
                    {ok, SpaceInfo};
                {error, InitReason} ->
                    ?error("Cannot initialize space ~p due to: ~p", [SpaceId, InitReason]),
                    {error, {initialize_error, InitReason}}
            catch
                _Type:Except ->
                    ?error_stacktrace("Cannot initialize space ~p due to: ~p", [SpaceId, Except]),
                    {error, {initialize_error, Except}}
            end;
        {error, Reason} ->
            ?error("Unknown space ~p", [SpaceId]),
            {error, {unknown_space_error, Reason}}
    end;
get_space(SpaceName) ->
    {ok, FileDoc} = dao_lib:apply(vfs, get_space_file, [filename:join(?SPACES_BASE_DIR_NAME, unicode:characters_to_list(SpaceName))], 1),
    get_space(FileDoc).




%% save_file/1
%% ====================================================================
%% @doc Updates given #file{} to DB.
%% @end
-spec save_file(FileDoc :: file_doc()) ->
    {ok, UUID :: uuid()} |
    {error, {failed_to_save_file, {Reason :: any(), FileDoc :: file_doc()}}}.
%% ====================================================================
save_file(FileDoc = #veil_document{record = #file{}}) ->
    case dao_lib:apply(dao_vfs, save_file, [FileDoc], fslogic_context:get_protocol_version()) of
        {ok, UUID}      -> {ok, UUID};
        {error, Reason} -> {error, {failed_to_save_file, {Reason, FileDoc}}}
    end.


%% get_storage/1
%% ====================================================================
%% @doc Gets storage document from DB by ID or UUID of document).
%% @end
-spec get_storage({Type :: id | uuid, StorageID :: integer() | uuid()}) ->
    {ok, StorageDoc :: storage_doc()} |
    {error, {failed_to_get_storage, {Reason :: any(), {storage, Type :: atom, StorageID :: integer()}}}}.
%% ====================================================================
get_storage({Type, StorageID}) ->
    case dao_lib:apply(dao_vfs, get_storage, [{Type, StorageID}], fslogic_context:get_protocol_version()) of
        {ok, #veil_document{record = #storage_info{}} = SInfo} ->
            {ok, SInfo};
        {error, Reason} ->
            {error, {failed_to_get_storage, {Reason, {storage, Type, StorageID}}}}
    end.


%% get_user/1
%% ====================================================================
%% @doc Gets user associated with given DN
%%      If DN is 'undefined', ROOT user is returned.
%% @end
-spec get_user({dn, DN :: string()} | user_doc()) -> {ok, UserDoc :: user_doc()} | {error, any()}.
%% ====================================================================
get_user(#veil_document{record = #user{}} = UserDoc) ->
    {ok, UserDoc};
get_user({Key, Value}) ->
    get_user2({Key, Value}, true).
get_user2({Key, Value}, Retry) ->
    case Value of
        undefined -> {ok, #veil_document{uuid = ?CLUSTER_USER_ID, record = #user{login = "root", role = admin}}};
        Value ->
            case user_logic:get_user({Key, Value}) of
                {ok, #veil_document{}} = OKRet -> OKRet;
                {error, user_not_found} when Key =:= global_id, Retry ->
                    cluster_manager_lib:sync_all_spaces(),
                    {ok, SpaceFiles} = dao_lib:apply(vfs, get_space_files, [{gruid, vcn_utils:ensure_binary(Value)}], fslogic_context:get_protocol_version()),
                    Spaces = [fslogic_utils:file_to_space_info(SpaceFile) || #veil_document{record = #file{}} = SpaceFile <- SpaceFiles],
                    ?info("Spaces =============> ~p ~p", [Spaces, SpaceFiles]),
                    [#space_info{space_id = SpaceId} = SpaceInfo | _] = Spaces,
                    {ok, #user_details{name = Name}} = gr_spaces:get_user_details(provider, SpaceId, Value),
                    user_logic:sign_in([{global_id, vcn_utils:ensure_list(Value)}, {name, Name}, {login, openid_utils:get_user_login(Value)}], undefined),
                    get_user2({Key, Value}, false);
                {error, Reason} ->
                    {error, {get_user_error, {Reason, {key, Key}, {value, Value}}}}
            end
    end.


%% get_user/0
%% ====================================================================
%% @doc Gets user associated with current session from DB
%%      If there is no user associated with current session, ROOT user is returned.
%% @end
-spec get_user() -> {ok, UserDoc :: user_doc()} | {error, any()}.
%% ====================================================================
get_user() ->
    case fslogic_context:get_access_token() of
        {GRUID, _} when GRUID =/= undefined ->
            get_user({global_id, GRUID});
        {_, _} ->
            get_user({dn, fslogic_context:get_user_dn()})
    end.


%% get_file/1
%% ====================================================================
%% @doc Gets file info from DB
%% @end
-spec get_file(FullFileName :: file() | file_doc()) -> Result when
    Result :: {ok, FileDoc :: file_doc()} | {error, file_not_found} | {error, any()}.
%% ====================================================================
get_file(#veil_document{record = #file{}} = FileDoc) ->
    {ok, FileDoc};
get_file(FullFileName) ->
    get_file(fslogic_context:get_protocol_version(), FullFileName, fslogic_context:get_fuse_id()).


%% get_file/3
%% ====================================================================
%% @doc Gets file info from DB. Context independent version of get_file/1
%% @end
-spec get_file(ProtocolVersion :: term(), FullFileName :: file() | file_doc(), FuseID :: term()) -> Result when
    Result :: {ok, FileDoc :: file_doc()} | {error, file_not_found} | {error, any()}.
%% ====================================================================
get_file(ProtocolVersion, File, _FuseID) when is_tuple(File) ->
    dao_lib:apply(dao_vfs, get_file, [File], ProtocolVersion);
get_file(_ProtocolVersion, FullFileName, _FuseID) ->
    get_file_helper(FullFileName, get_file).


%% get_waiting_file/3
%% ====================================================================
%% @doc Gets file info about file that waits to be created at storage from DB
%% @end
-spec get_waiting_file(FullFileName :: string()) -> Result when
    Result :: term().
%% ====================================================================
get_waiting_file(FullFileName) ->
    get_waiting_file(fslogic_context:get_protocol_version(), FullFileName, fslogic_context:get_fuse_id()).

get_waiting_file(_ProtocolVersion, FullFileName, _FuseID) ->
    get_file_helper(FullFileName, get_waiting_file).

get_file_helper(File, Fun) ->
    get_file_helper(fslogic_context:get_protocol_version(), File, fslogic_context:get_fuse_id(), Fun).

%% get_file_helper/4
%% ====================================================================
%% @doc Gets file info from DB
%% @end
-spec get_file_helper(ProtocolVersion :: term(), File :: string(), FuseID :: string(), Fun :: atom()) -> Result when
    Result :: term().
%% ====================================================================
get_file_helper(ProtocolVersion, File, FuseID, Fun) ->
    ?debug("get_file(File: ~p, FuseID: ~p)", [File, FuseID]),
    case string:tokens(File, "/") of
        [?SPACES_BASE_DIR_NAME, GroupName | _] -> %% Check if group that user is tring to access is avaliable to him
            case fslogic_context:get_user_dn() of %% Internal call, allow all group access
                undefined   -> dao_lib:apply(dao_vfs, Fun, [File], ProtocolVersion);
                UserDN      -> %% Check if user has access to this group
                    Teams = user_logic:get_space_names({dn, UserDN}),
                    case lists:member(GroupName, Teams) of %% Does the user belong to the group?
                        true  -> dao_lib:apply(dao_vfs, Fun, [File], ProtocolVersion);
                        false -> {error, file_not_found} %% Assume that this file does not exists
                    end
            end;
        _ ->
            dao_lib:apply(dao_vfs, Fun, [File], ProtocolVersion)
    end.


%% save_file_descriptor/3
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_file_descriptor(ProtocolVersion :: term(), File :: record(), Validity :: integer()) -> Result when
    Result :: term().
%% ====================================================================
save_file_descriptor(ProtocolVersion, File, Validity) ->
    Descriptor = update_file_descriptor(File#veil_document.record, Validity),
    case dao_lib:apply(dao_vfs, save_descriptor, [File#veil_document{record = Descriptor}], ProtocolVersion) of
        {error, Reason} ->
            {error, {save_file_descriptor, {Reason, Descriptor}}};
        Other -> Other
    end.


%% save_file_descriptor/4
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_file_descriptor(ProtocolVersion :: term(), Uuid::uuid(), FuseID :: string(), Validity :: integer()) -> Result when
    Result :: term().
%% ====================================================================
save_file_descriptor(ProtocolVersion, Uuid, FuseID, Validity) ->
    case FuseID of
        ?CLUSTER_FUSE_ID -> {ok, ok};
        _ ->
            Status = dao_lib:apply(dao_vfs, list_descriptors, [{by_uuid_n_owner, {Uuid, FuseID}}, 10, 0], ProtocolVersion),
            case Status of
                {ok, TmpAns} ->
                    case length(TmpAns) of
                        0 ->
                            save_new_file_descriptor(ProtocolVersion, Uuid, FuseID, Validity);
                        1 ->
                            [VeilDoc | _] = TmpAns,
                            case save_file_descriptor(ProtocolVersion, VeilDoc, Validity) of
                                {ok,Uid} -> {ok,Uid};
                                {error, {save_file_descriptor, {conflict,_}}} -> {ok,VeilDoc#veil_document.uuid};
                                Other -> Other
                            end;
                        _Many ->
                            ?error("Error: to many file descriptors for file uuid: ~p", [Uuid]),
                            {error, "Error: too many file descriptors"}
                    end;
                _Other -> _Other
            end
    end.

%% save_new_file_descriptor/4
%% ====================================================================
%% @doc Saves in db information that a file is used by FUSE.
%% @end
-spec save_new_file_descriptor(ProtocolVersion :: term(), Uuid::uuid(), FuseID :: string(), Validity :: integer()) -> Result when
    Result :: term().
%% ====================================================================
save_new_file_descriptor(ProtocolVersion, Uuid, FuseID, Validity) ->
    Descriptor = update_file_descriptor(#file_descriptor{file = Uuid, fuse_id = FuseID}, Validity),
    case dao_lib:apply(dao_vfs, save_descriptor, [Descriptor], ProtocolVersion) of
        {error, Reason} ->
            {error, {save_new_file_descriptor, {Reason, Descriptor}}};
        Other -> Other
    end.

%% update_file_descriptor/2
%% ====================================================================
%% @doc Updates descriptor (record, not in DB)
%% @end
-spec update_file_descriptor(Descriptor :: record(),  Validity :: integer()) -> Result when
    Result :: record().
%% ====================================================================
update_file_descriptor(Descriptor, Validity) ->
    {Megaseconds,Seconds, _Microseconds} = os:timestamp(),
    Time = 1000000*Megaseconds + Seconds,
    Descriptor#file_descriptor{create_time = Time, validity_time = Validity}.


%% delete_old_descriptors/2
%% ====================================================================
%% @doc Deletes old descriptors (older than Time)
%% @end
-spec delete_old_descriptors(ProtocolVersion :: term(), Time :: integer()) -> Result when
    Result :: term().
%% ====================================================================
delete_old_descriptors(ProtocolVersion, Time) ->
    Status = dao_lib:apply(dao_vfs, remove_descriptor, [{by_expired_before, Time}], ProtocolVersion),
    case Status of
        ok ->
            ?info("Old descriptors cleared"),
            ok;
        Other ->
            ?error("Error during clearing old descriptors: ~p", [Other]),
            Other
    end.

%% ====================================================================
%% Internal functions
%% ====================================================================
