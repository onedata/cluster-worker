%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module is able to do additional translation of record
%% decoded using protocol buffer e.g. it can change record "atom" to
%% Erlang atom type.
%% @end
%% ===================================================================

-module(records_translator).
-include("communication_protocol_pb.hrl").
-include("logging.hrl").

%% ====================================================================
%% API
%% ====================================================================
-export([translate/2, translate_to_record/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% translate/2
%% ====================================================================
%% @doc Translates record to simpler terms if possible.
-spec translate(Record :: tuple(), DecoderName :: string()) -> Result when
  Result ::  term().
%% ====================================================================
translate(Record, _DecoderName) when is_record(Record, atom) ->
  try
    list_to_existing_atom(Record#atom.value)
  catch
    _:_ ->
      ?warning("Unsupported atom: ~p", [Record#atom.value]),
      throw(message_not_supported)
  end;

translate(Record, DecoderName) when is_tuple(Record) ->
  RecordList = lists:reverse(tuple_to_list(Record)),
  [End | Rest] = RecordList,
  RecordList2 = case is_binary(End) of
    true ->
      try
        [Type | Rest2] = Rest,
        DecodedEnd = erlang:apply(list_to_existing_atom(DecoderName ++ "_pb"), list_to_existing_atom("decode_" ++ Type), [End]),
        [DecodedEnd | [list_to_existing_atom(Type) | Rest2]]
      catch
        _:_ ->
          ?warning("Can not translate record: ~p, using decoder: ~p", [Record, DecoderName]),
          RecordList
      end;
    false -> RecordList
  end,
  TmpAns = lists:foldl(fun(E, Sum) -> [translate(E, DecoderName) | Sum] end, [], RecordList2),
  list_to_tuple(TmpAns);

translate(Record, _DecoderName) ->
  Record.

%% translate_to_record/1
%% ====================================================================
%% @doc Translates term to record if possible.
-spec translate_to_record(Value :: term()) -> Result when
  Result ::  tuple() | term().
%% ====================================================================
translate_to_record(Value) when is_atom(Value) ->
  #atom{value = atom_to_list(Value)};

translate_to_record(Value) ->
  Value.