%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Translations between protobuff and internal protocol
%%% @end
%%%-------------------------------------------------------------------
-module(translator).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/stream_messages.hrl").
-include("proto/oneclient/handshake_messages.hrl").
-include("proto/oneclient/event_messages.hrl").
-include("proto/oneclient/diagnostic_messages.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("clproto/include/messages.hrl").

%% API
-export([translate_from_protobuf/1, translate_to_protobuf/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% traslate protobuf record to internal record
%% @end
%%--------------------------------------------------------------------
-spec translate_from_protobuf(tuple() | undefined) -> tuple() | undefined.
translate_from_protobuf(#'Status'{code = Code, description = Desc}) ->
    #status{code = Code, description = Desc};
translate_from_protobuf(#'Event'{counter = Counter, type = {_, Record}}) ->
    #event{counter = Counter, type = translate_from_protobuf(Record)};
translate_from_protobuf(#'ReadEvent'{} = Record) ->
    #read_event{
        file_uuid = Record#'ReadEvent'.file_uuid,
        size = Record#'ReadEvent'.size,
        blocks = [translate_from_protobuf(B) || B <- Record#'ReadEvent'.blocks]
    };
translate_from_protobuf(#'WriteEvent'{} = Record) ->
    #write_event{
        file_uuid = Record#'WriteEvent'.file_uuid,
        size = Record#'WriteEvent'.size,
        file_size = Record#'WriteEvent'.file_size,
        blocks = [translate_from_protobuf(B) || B <- Record#'WriteEvent'.blocks]
    };
translate_from_protobuf(#'Subscription'{} = Record) ->
    #subscription{
        id = Record#'Subscription'.id,
        type = translate_from_protobuf(Record#'Subscription'.type)
    };
translate_from_protobuf(#'FileAttrSubscription'{} = Record) ->
    #file_attr_subscription{
        file_uuid = Record#'FileAttrSubscription'.file_uuid,
        counter_threshold = Record#'FileAttrSubscription'.counter_threshold,
        time_threshold = Record#'FileAttrSubscription'.time_threshold
    };
translate_from_protobuf(#'FileLocationSubscription'{} = Record) ->
    #file_location_subscription{
        file_uuid = Record#'FileLocationSubscription'.file_uuid,
        counter_threshold = Record#'FileLocationSubscription'.counter_threshold,
        time_threshold = Record#'FileLocationSubscription'.time_threshold
    };
translate_from_protobuf(#'SubscriptionCancellation'{id = Id}) ->
    #subscription_cancellation{id = Id};
translate_from_protobuf(#'FileBlock'{offset = Off, size = S}) ->
    #file_block{offset = Off, size = S};
translate_from_protobuf(#'HandshakeRequest'{token = Token, session_id = SessionId}) ->
    #handshake_request{auth = translate_from_protobuf(Token), session_id = SessionId};
translate_from_protobuf(#'MessageStream'{stream_id = StmId, sequence_number = SeqNum}) ->
    #message_stream{stream_id = StmId, sequence_number = SeqNum};
translate_from_protobuf(#'EndOfMessageStream'{}) ->
    #end_of_message_stream{};
translate_from_protobuf(#'MessageStreamReset'{stream_id = StmId}) ->
    #message_stream_reset{stream_id = StmId};
translate_from_protobuf(#'MessageRequest'{} = Record) ->
    #message_request{
        stream_id = Record#'MessageRequest'.stream_id,
        lower_sequence_number = Record#'MessageRequest'.lower_sequence_number,
        upper_sequence_number = Record#'MessageRequest'.upper_sequence_number
    };
translate_from_protobuf(#'MessageAcknowledgement'{} = Record) ->
    #message_acknowledgement{
        stream_id = Record#'MessageAcknowledgement'.stream_id,
        sequence_number = Record#'MessageAcknowledgement'.sequence_number
    };
translate_from_protobuf(#'Token'{value = Val}) ->
    #auth{macaroon = Val};
translate_from_protobuf(#'Ping'{data = Data}) ->
    #ping{data = Data};
translate_from_protobuf(#'GetProtocolVersion'{}) ->
    #get_protocol_version{};
translate_from_protobuf(#'FuseRequest'{fuse_request = {_, Record}}) ->
    #fuse_request{fuse_request = translate_from_protobuf(Record)};
translate_from_protobuf(#'GetFileAttr'{entry_type = 'PATH', entry = Path}) ->
    #get_file_attr{entry = {path, Path}};
translate_from_protobuf(#'GetFileAttr'{entry_type = 'UUID', entry = UUID}) ->
    #get_file_attr{entry = {uuid, UUID}};
translate_from_protobuf(#'GetFileChildren'{uuid = UUID, offset = Offset, size = Size}) ->
    #get_file_children{uuid = UUID, offset = Offset, size = Size};
translate_from_protobuf(#'CreateDir'{parent_uuid = ParentUUID, name = Name, mode = Mode}) ->
    #create_dir{parent_uuid = ParentUUID, name = Name, mode = Mode};
translate_from_protobuf(#'DeleteFile'{uuid = UUID}) ->
    #delete_file{uuid = UUID};
translate_from_protobuf(#'UpdateTimes'{uuid = UUID, atime = ATime, mtime = MTime,
    ctime = CTime}) ->
    #update_times{uuid = UUID, atime = ATime, mtime = MTime, ctime = CTime};
translate_from_protobuf(#'ChangeMode'{uuid = UUID, mode = Mode}) ->
    #change_mode{uuid = UUID, mode = Mode};
translate_from_protobuf(#'Rename'{uuid = UUID, target_path = TargetPath}) ->
    #rename{uuid = UUID, target_path = TargetPath};
translate_from_protobuf(undefined) ->
    undefined.

%%--------------------------------------------------------------------
%% @doc
%% translate internal record to protobuf record
%% @end
%%--------------------------------------------------------------------
-spec translate_to_protobuf(tuple() | undefined) -> tuple() | undefined.
translate_to_protobuf(#status{code = Code, description = Desc}) ->
    {status, #'Status'{code = Code, description = Desc}};
translate_to_protobuf(#event{counter = Counter, type = Type}) ->
    {event, #'Event'{counter = Counter, type = translate_to_protobuf(Type)}};
translate_to_protobuf(#update_event{type = Type}) ->
    {update_event, #'UpdateEvent'{type = translate_to_protobuf(Type)}};
translate_to_protobuf(#subscription{id = Id, type = Type}) ->
    {subscription, #'Subscription'{id = Id, type = translate_to_protobuf(Type)}};
translate_to_protobuf(#read_subscription{} = Sub) ->
    {read_subscription, #'ReadSubscription'{
        counter_threshold = Sub#read_subscription.counter_threshold,
        time_threshold = Sub#read_subscription.time_threshold,
        size_threshold = Sub#read_subscription.size_threshold
    }};
translate_to_protobuf(#write_subscription{} = Sub) ->
    {write_subscription, #'ReadSubscription'{
        counter_threshold = Sub#write_subscription.counter_threshold,
        time_threshold = Sub#write_subscription.time_threshold,
        size_threshold = Sub#write_subscription.size_threshold
    }};
translate_to_protobuf(#subscription_cancellation{id = Id}) ->
    {subscription_cancellation, #'SubscriptionCancellation'{id = Id}};
translate_to_protobuf(#handshake_response{session_id = Id}) ->
    {handshake_response, #'HandshakeResponse'{session_id = Id}};
translate_to_protobuf(#message_stream{stream_id = StmId, sequence_number = SeqNum}) ->
    #'MessageStream'{stream_id = StmId, sequence_number = SeqNum};
translate_to_protobuf(#end_of_message_stream{}) ->
    {end_of_stream, #'EndOfMessageStream'{}};
translate_to_protobuf(#message_stream_reset{stream_id = StmId}) ->
    {message_stream_reset, #'MessageStreamReset'{stream_id = StmId}};
translate_to_protobuf(#message_request{stream_id = StmId,
    lower_sequence_number = LowerSeqNum, upper_sequence_number = UpperSeqNum}) ->
    {message_request, #'MessageRequest'{stream_id = StmId,
        lower_sequence_number = LowerSeqNum, upper_sequence_number = UpperSeqNum}};
translate_to_protobuf(#message_acknowledgement{stream_id = StmId, sequence_number = SeqNum}) ->
    {message_acknowledgement,
        #'MessageAcknowledgement'{stream_id = StmId, sequence_number = SeqNum}};
translate_to_protobuf(#pong{data = Data}) ->
    {pong, #'Pong'{data = Data}};
translate_to_protobuf(#protocol_version{major = Major, minor = Minor}) ->
    {protocol_version, #'ProtocolVersion'{major = Major, minor = Minor}};
translate_to_protobuf(#fuse_response{status = Status, fuse_response = FuseResponse}) ->
    {fuse_response, #'FuseResponse'{
        status = #'Status'{
            code = Status#status.code,
            description = Status#status.description
        },
        fuse_response = translate_to_protobuf(FuseResponse)
    }};
translate_to_protobuf(#child_link{uuid = UUID, name = Name}) ->
    #'ChildLink'{uuid = UUID, name = Name};
translate_to_protobuf(#file_block{offset = Offset, size = Size}) ->
    #'FileBlock'{offset = Offset, size = Size};
translate_to_protobuf(#file_attr{} = FileAttr) ->
    {file_attr, #'FileAttr'{
        uuid = FileAttr#file_attr.uuid,
        name = FileAttr#file_attr.name,
        mode = FileAttr#file_attr.mode,
        uid = FileAttr#file_attr.uid,
        gid = FileAttr#file_attr.gid,
        atime = FileAttr#file_attr.atime,
        mtime = FileAttr#file_attr.mtime,
        ctime = FileAttr#file_attr.ctime,
        type = FileAttr#file_attr.type,
        size = FileAttr#file_attr.size
    }};
translate_to_protobuf(#file_location{} = FileLocation) ->
    {file_attr, #'FileLocation'{
        uuid = FileLocation#file_location.uuid,
        provider_id = FileLocation#file_location.provider_id,
        storage_id = FileLocation#file_location.storage_id,
        file_id = FileLocation#file_location.file_id,
        blocks = [translate_to_protobuf(B) || B <- FileLocation#file_location.blocks]
    }};
translate_to_protobuf(#file_children{child_links = FileEntries}) ->
    {file_children, #'FileChildren'{child_links = lists:map(fun(ChildLink) ->
        translate_to_protobuf(ChildLink)
    end, FileEntries)}};
translate_to_protobuf(undefined) ->
    undefined.

%%%===================================================================
%%% Internal functions
%%%===================================================================
