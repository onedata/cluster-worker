%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%% @doc This module performs file-related operations of lfm_submodules.
%%% @end
%%%-------------------------------------------------------------------
-module(lfm_files).

-include("types.hrl").
-include("errors.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("modules/datastore/datastore.hrl").

%% API
%% Functions operating on directories or files
-export([exists/1, mv/2, cp/2, rm/1]).
%% Functions operating on files
-export([create/3, open/2, write/3, read/3, truncate/2, get_block_map/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Checks if a file or directory exists.
%%
%% @end
%%--------------------------------------------------------------------
-spec exists(FileKey :: file_key()) -> {ok, boolean()} | error_reply().
exists(_FileKey) ->
    {ok, false}.


%%--------------------------------------------------------------------
%% @doc
%% Moves a file or directory to a new location.
%%
%% @end
%%--------------------------------------------------------------------
-spec mv(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
mv(_FileKeyFrom, _PathTo) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Copies a file or directory to given location.
%%
%% @end
%%--------------------------------------------------------------------
-spec cp(FileKeyFrom :: file_key(), PathTo :: file_path()) -> ok | error_reply().
cp(_PathFrom, _PathTo) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Removes a file or an empty directory.
%%
%% @end
%%--------------------------------------------------------------------
-spec rm(FileKey :: file_key()) -> ok | error_reply().
rm(_FileKey) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Creates a new file.
%%
%% @end
%%--------------------------------------------------------------------
-spec create(SessId :: session:id(), Path :: file_path(), Mode :: file_meta:posix_permissions()) ->
    {ok, file_id()} | error_reply().
create(SessId, Path, Mode) ->
    CTX = fslogic_context:new(SessId),
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    CanonicalFileEntry = fslogic_path:get_canonical_file_entry(CTX, Tokens),
    {ok, CanonicalPath} = file_meta:gen_path(CanonicalFileEntry),
    {Name, ParentPath} = fslogic_path:basename_and_parent(CanonicalPath),
    {ok, {#document{key = ParentUUID}, _}} = file_meta:resolve_path(ParentPath),
    case worker_proxy:call(fslogic_worker, {fuse_request, SessId, #get_new_file_location{name = Name, parent_uuid = ParentUUID, mode = Mode}}) of
        #fuse_response{status = #status{code = ?OK}, fuse_response = #file_location{uuid = UUID, file_id = FileId, storage_id = StorageId}} ->
            %% @todo: handle different cluster_ids (via cluster proxy)
            {ok, #document{value = Storage}} = storage:get(StorageId),
            case storage_file_manager:create(Storage, FileId, Mode, true) of
                ok ->
                    %% @todo: remove file watchers
                    {ok, UUID};
                {error, Reason} ->
                    file_meta:delete({uuid, UUID}),
                    %% @todo: cleanup
                    {error, Reason}
            end;
        #fuse_response{status = #status{code = Code}} ->
            {error, Code}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Opens a file in selected mode and returns a file handle used to read or write.
%%
%% @end
%%--------------------------------------------------------------------
-spec open(FileKey :: file_id_or_path(), OpenType :: open_type()) -> {ok, file_handle()} | error_reply().
open(_FileKey, _OpenType) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Writes data to a file. Returns number of written bytes.
%%
%% @end
%%--------------------------------------------------------------------
-spec write(FileHandle :: file_handle(), Offset :: integer(), Buffer :: binary()) -> {ok, integer()} | error_reply().
write(_FileHandle, _Offset, _Buffer) ->
    {ok, 0}.


%%--------------------------------------------------------------------
%% @doc
%% Reads requested part of a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec read(FileHandle :: file_handle(), Offset :: integer(), MaxSize :: integer()) -> {ok, binary()} | error_reply().
read(_FileHandle, _Offset, _MaxSize) ->
    {ok, <<"">>}.


%%--------------------------------------------------------------------
%% @doc
%% Truncates a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec truncate(FileKey :: file_key(), Size :: integer()) -> ok | error_reply().
truncate(_FileKey, _Size) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% Returns block map for a file.
%%
%% @end
%%--------------------------------------------------------------------
-spec get_block_map(FileKey :: file_key()) -> {ok, [block_range()]} | error_reply().
get_block_map(_FileKey) ->
    {ok, []}.
