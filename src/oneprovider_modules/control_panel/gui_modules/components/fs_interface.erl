%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2014 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains functions for operating on file system. This is for page modules to
%% be able to use the file system API without knowledge of underlying structures.
%% @end
%% ===================================================================
-module(fs_interface).

-include("oneprovider_modules/dao/dao_vfs.hrl").
-include("oneprovider_modules/dao/dao.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/fslogic/fslogic_available_blocks.hrl").
-include("oneprovider_modules/fslogic/ranges_struct.hrl").
-include("fuse_messages_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("registered_names.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/global_registry/gr_providers.hrl").

%% API
-export([get_file_block_map/1, get_full_file_path/1, get_file_uuid/1, get_provider_name/1]).
-export([issue_remote_file_synchronization/3]).


%% get_all_available_blocks/1
%% ====================================================================
%% @doc Lists all available blocks for given file and returns them in simplified format.
%% The first tuple element is file size, the seconds is list of {ProviderID, ProviderBlocks} tuples.
%% @end
-spec get_file_block_map(FullFilePath :: string()) -> {integer(), [{ProviderID :: string(), [integer()]}]}.
%% ====================================================================
get_file_block_map(FullFilePath) ->
    {ok, AvailableBlocks} = logical_files_manager:get_file_block_map(FullFilePath),
    {_, FileSize} = lists:foldl(
        fun(#available_blocks{file_size = {Timestamp, Size}}, {AccTmstp, AccSize}) ->
            case Timestamp > AccTmstp of
                true -> {Timestamp, Size};
                false -> {AccTmstp, AccSize}
            end
        end, {-1, 0}, AvailableBlocks),
    Blocks = lists:map(
        fun(#available_blocks{provider_id = ProviderID, file_parts = FileParts}) ->
            {ProvBytes, BlockList} = lists:foldl(
                fun(#block_range{from = From, to = To}, {AccBytes, AccBlocks}) ->
                    FromBytes = From * ?remote_block_size,
                    ToBytes = min(FileSize - 1, To * ?remote_block_size + ?remote_block_size - 1),
                    {AccBytes + ToBytes - FromBytes + 1, AccBlocks ++ [FromBytes, ToBytes]}
                end, {0, []}, FileParts),
            {ProviderID, ProvBytes, BlockList}
        end, AvailableBlocks),
    {FileSize, Blocks}.


%% get_full_file_path/1
%% ====================================================================
%% @doc Converts file path in binary to string representing the full file name (one that starts with spaces dir).
%% @end
-spec get_full_file_path(FilePath :: binary()) -> string().
%% ====================================================================
get_full_file_path(FilePath) ->
    {ok, FullPath} = fslogic_path:get_full_file_name(gui_str:binary_to_unicode_list(FilePath)),
    FullPath.


%% get_file_uuid/1
%% ====================================================================
%% @doc Retrieves file document's UUID for given filepath. the filepath must be
%% a string representing the full file path.
%% @end
-spec get_file_uuid(FullFilePath :: string()) -> string().
%% ====================================================================
get_file_uuid(FullFilePath) ->
    {ok, #db_document{uuid = FileID}} = fslogic_objects:get_file(FullFilePath),
    FileID.


%% get_provider_name/1
%% ====================================================================
%% @doc Returns provider's name based on its id.
%% @end
-spec get_provider_name(ProviderID :: binary()) -> binary().
%% ====================================================================
% TODO for now, simple cache in application env. Need to find a better solution.
get_provider_name(ProviderID) ->
    CacheContent = case application:get_env(?APP_Name, provider_names) of
                       {ok, Content} -> Content;
                       _ -> []
                   end,
    case proplists:get_value(ProviderID, CacheContent) of
        undefined ->
            {ok, #provider_details{name = Name}} = gr_providers:get_details(provider, ProviderID),
            application:set_env(?APP_Name, provider_names, [{ProviderID, Name} | CacheContent]),
            Name;
        ExistingName ->
            ExistingName
    end.


%% issue_remote_file_synchronization/3
%% ====================================================================
%% @doc Issues full block synchronization on remote provider.
%% @end
-spec issue_remote_file_synchronization(FullPath :: string(), ProviderID :: binary(), Size::integer()) -> binary().
%% ====================================================================
issue_remote_file_synchronization(FullPath, ProviderID, Size) ->
    % TODO not yet implemented
    ok.
%%     Res = provider_proxy:reroute_pull_message(gui_str:to_binary(ProviderID), fslogic_context:get_gr_auth(),
%%         ?CLUSTER_FUSE_ID, #synchronizefileblock{logical_name = FullPath, offset = 0, size = Size}).