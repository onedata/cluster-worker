%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module responsible for pushing new file's information to sessions.
%%%      @todo: Should be replaced with event system
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_notify).
-author("Rafal Slota").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([attributes/2, blocks/3]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%%  Sends current attributes for given file to all sessions that are watching this file.
%% @end
%%--------------------------------------------------------------------
-spec attributes(fslogic_worker:file(), [session:id()]) -> ok | {error, Reason :: term()}.
attributes(FileEntry, ExcludedSessions) ->
    case file_manager:stat(?ROOT_SESS_ID, FileEntry) of
        {ok, #file_attr{uuid = FileUUID} = Attrs} ->
            try
                SessionIds = file_watcher:get_attr_watchers(FileUUID) -- ExcludedSessions,
                ToRemove =
                    lists:foldl(
                      fun(SessionId, AccIn) ->
                              try
                                  case session:get(SessionId) of
                                      {ok, _} ->
                                          ?info("Sending new attributes for file ~p to session ~p", [FileEntry, SessionId]),
                                          communicator:send(#fuse_response{status = #status{code = ?OK}, fuse_response = Attrs}, SessionId);
                                      {error, {not_found, _}} ->
                                          [SessionId | AccIn];
                                      {error, Reason3} ->
                                          ?error("Unable to notify session ~p due to: ~p", [SessionId, Reason3]),
                                          AccIn
                                  end
                              catch
                                  _:Reason2 ->
                                      ?error("Unable to notify session ~p due to: ~p", [SessionId, Reason2]),
                                      AccIn
                              end
                      end, [], SessionIds),
                %% @todo: remove ToRemove sessions from watchers
                ok
            catch
                _:Reason1  ->
                    ?error_stacktrace("Unable to push new attributes for file ~p due to: ~p", [FileEntry, Reason1]),
                    {error, Reason1}
            end;
        {error, Reason} ->
            ?error("Unable to get new attributes for file ~p due to: ~p", [FileEntry, Reason]),
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @doc
%%  Sends current locally available blocks for given file to all sessions that are watching this file.
%% @end
%%--------------------------------------------------------------------
-spec blocks(fslogic_worker:file(), fslogic_blocks:blocks(), [session:id()]) -> ok | {error, Reason :: term()}.
blocks(FileEntry, _Blocks, ExcludedSessions) ->
    try
        {ok, #document{key = FileUUID} = File} = file_meta:get(FileEntry),
        #document{value = #file_location{} = Location} = fslogic_utils:get_local_file_location(File),
        SessionIds = file_watcher:get_open_watchers(FileUUID) -- ExcludedSessions,
        _ToRemove =
            lists:foldl(
              fun(SessionId, AccIn) ->
                      try
                          case session:get(SessionId) of
                              {ok, _} ->
                                  ?info("Sending new location for file ~p to session ~p", [FileEntry, SessionId]),
                                  communicator:send(#fuse_response{status = #status{code = ?OK}, fuse_response = Location}, SessionId);
                              {error, {not_found, _}} ->
                                  [SessionId | AccIn];
                              {error, Reason3} ->
                                  ?error("Unable to notify session ~p due to: ~p", [SessionId, Reason3]),
                                  AccIn
                          end
                      catch
                          _:Reason2 ->
                              ?error("Unable to notify session ~p due to: ~p", [SessionId, Reason2]),
                              AccIn
                      end
              end, [], SessionIds),
        %% @todo: remove ToRemove sessions from watchers
        ok
    catch
        _:Reason1  ->
            ?error_stacktrace("Unable to push new attributes for file ~p due to: ~p", [FileEntry, Reason1]),
            {error, Reason1}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================