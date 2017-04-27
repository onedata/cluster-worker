%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This module is responsible for encoding/decoding datastore document
%%% to/from EJSON format.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_json2).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models_def.hrl").

%% API
-export([encode/1, decode/1]).

-type field_name() :: atom().
-type record_key() :: atom | boolean | binary | float | integer | string | term.
-type record_value() :: record_key() | json | record_custom() |
                        record_struct() | [record_value()] | {record_value()} |
                        #{record_key() => record_value()}.
%% encoder Mod:Encoder(Term) or Mod:Encoder(Type, Term) shell return JSON binary
%% decoder Mod:Decoder(JSON) or Mod:Encoder(Type, JSON) shell return Erlang term
%% Encoder defaults to 'encode_value' and Decoder defaults to 'decode_value'
-type encoder() :: atom().
-type decoder() :: atom().
-type record_custom() :: {custom, module()} |
                         {custom, {module(), encoder(), decoder()}} |
                         {custom, atom(), module()} |
                         {custom, atom(), {module(), encoder(), decoder()}}.
-type record_struct() :: {record, [{field_name(), record_value()}]}.
-type ejson() :: jiffy:json_value().

-export_type([record_struct/0, ejson/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Encodes a document to erlang json format with given structure.
%% @end
%%--------------------------------------------------------------------
-spec encode(datastore:doc()) -> ejson().
encode(#document2{value = Value, version = Version} = Doc) ->
    Model = element(1, Value),
    {Props} = encode_term(Value, Model:record_struct(Version)),
    {[
        {<<"_key">>, Doc#document2.key},
        {<<"_scope">>, Doc#document2.scope},
        {<<"_mutator">>, Doc#document2.mutator},
        {<<"_rev">>, Doc#document2.rev},
        {<<"_seq">>, Doc#document2.seq},
        {<<"_deleted">>, Doc#document2.deleted},
        {<<"_version">>, Version} |
        Props
    ]}.

%%--------------------------------------------------------------------
%% @doc
%% Decodes a document from erlang json format with given structure.
%% @end
%%--------------------------------------------------------------------
-spec decode(ejson()) -> datastore:doc().
decode({Term} = EJson) when is_list(Term) ->
    {<<"_key">>, Key} = lists:keyfind(<<"_key">>, 1, Term),
    {<<"_scope">>, Scope} = lists:keyfind(<<"_scope">>, 1, Term),
    {<<"_mutator">>, Mutator} = lists:keyfind(<<"_mutator">>, 1, Term),
    {<<"_rev">>, Rev} = lists:keyfind(<<"_rev">>, 1, Term),
    {<<"_seq">>, Seq} = lists:keyfind(<<"_seq">>, 1, Term),
    {<<"_deleted">>, Deleted} = lists:keyfind(<<"_deleted">>, 1, Term),
    {<<"_record">>, RecordName} = lists:keyfind(<<"_record">>, 1, Term),
    {<<"_version">>, Version} = lists:keyfind(<<"_version">>, 1, Term),
    ModelName = decode_term(RecordName, atom),
    ModelName2 = datastore_version2:rename_model(Version, ModelName),
    Record = decode_term(EJson, ModelName2:record_struct(Version)),
    {Version2, Record2} = datastore_version2:upgrade_model(
        Version, ModelName2, Record),
    #document2{
        key = Key,
        value = Record2,
        scope = Scope,
        mutator = Mutator,
        rev = Rev,
        seq = Seq,
        deleted = Deleted,
        version = Version2
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Encodes an erlang term to erlang json format with given structure.
%% @end
%%--------------------------------------------------------------------
-spec encode_term(term(), record_value()) -> ejson() | no_return().
encode_term(undefined, _) ->
    null;
encode_term(Term, atom) when is_atom(Term) ->
    atom_to_binary(Term, utf8);
encode_term(Term, boolean) when is_boolean(Term) ->
    Term;
encode_term(Term, binary) when is_binary(Term) ->
    base64:encode(Term);
encode_term(Term, float) when is_integer(Term) orelse is_float(Term) ->
    Term;
encode_term(Term, integer) when is_integer(Term) ->
    Term;
encode_term(Term, json) when is_binary(Term) ->
    jiffy:decode(Term);
encode_term(Term, string) when is_binary(Term) ->
    Term;
encode_term(Term, term) ->
    base64:encode(term_to_binary(Term));
encode_term(Term, {custom, {Mod, Encoder, _Decoder}}) ->
    encode_term(Mod:Encoder(Term), json);
encode_term(Term, {custom, Mod}) ->
    encode_term(Term, {custom, {Mod, encode_value, decode_value}});
encode_term(Term, {custom, Type, {Mod, Encoder, _Decoder}}) ->
    encode_term(Mod:Encoder(Term, Type), json);
encode_term(Term, {custom, Type, Mod}) ->
    encode_term(Term, {custom, Type, {Mod, encode_value, decode_value}});
encode_term(Term, {record, Fields}) when is_tuple(Term), is_list(Fields) ->
    Values = tuple_to_list(Term),
    {Keys, Types} = lists:unzip(Fields),
    {
        lists:map(fun({Key, Value, Type}) ->
            EncodedKey = encode_term(Key, atom),
            EncodedValue = encode_term(Value, Type),
            {EncodedKey, EncodedValue}
        end, lists:zip3(['_record' | Keys], Values, [atom | Types]))
    };
encode_term(Term, {set, Type}) ->
    lists:map(fun(Value) -> encode_term(Value, Type) end, sets:to_list(Term));
encode_term(Term, [Type]) when is_list(Term) ->
    lists:map(fun(Value) -> encode_term(Value, Type) end, Term);
encode_term(Term, Types) when is_list(Term), is_list(Types) ->
    lists:map(fun({Value, Type}) ->
        encode_term(Value, Type)
    end, lists:zip(Term, Types));
encode_term(Term, {Type}) when is_tuple(Term) ->
    lists:map(fun(Value) ->
        encode_term(Value, Type)
    end, tuple_to_list(Term));
encode_term(Term, Types) when is_tuple(Term), is_tuple(Types) ->
    lists:map(fun({Value, Type}) ->
        encode_term(Value, Type)
    end, lists:zip(tuple_to_list(Term), tuple_to_list(Types)));
encode_term(Term, Type) when is_map(Term), is_map(Type) ->
    [{KeyType, ValueType}] = maps:to_list(Type),
    {
        maps:fold(fun(Key, Value, Acc) ->
            EncodedKey = encode_key(Key, KeyType),
            EncodedValue = encode_term(Value, ValueType),
            [{EncodedKey, EncodedValue} | Acc]
        end, [], Term)
    };
encode_term(Term, Type) ->
    error({invalid_term_structure, Term, Type}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Decodes an erlang term from erlang json format with given structure.
%% @end
%%--------------------------------------------------------------------
-spec decode_term(ejson(), record_value()) -> term() | no_return().
decode_term(null, _) ->
    undefined;
decode_term(Term, atom) when is_binary(Term) ->
    binary_to_atom(Term, utf8);
decode_term(Term, boolean) when is_boolean(Term) ->
    Term;
decode_term(Term, binary) when is_binary(Term) ->
    base64:decode(Term);
decode_term(Term, float) when is_integer(Term) orelse is_float(Term) ->
    Term;
decode_term(Term, integer) when is_integer(Term) ->
    Term;
decode_term(Term, json) ->
    jiffy:encode(Term);
decode_term(Term, string) when is_binary(Term) ->
    Term;
decode_term(Term, term) when is_binary(Term) ->
    binary_to_term(base64:decode(Term));
decode_term(Term, {custom, {Mod, _Encoder, Decoder}}) ->
    Mod:Decoder(decode_term(Term, json));
decode_term(Term, {custom, Mod}) ->
    decode_term(Term, {custom, {Mod, encode_value, decode_value}});
decode_term(Term, {custom, Type, {Mod, _Encoder, Decoder}}) ->
    Mod:Decoder(decode_term(Term, json), Type);
decode_term(Term, {custom, Type, Mod}) ->
    decode_term(Term, {custom, Type, {Mod, encode_value, decode_value}});
decode_term({Term}, {record, Fields}) when is_list(Term), is_list(Fields) ->
    {<<"_record">>, RecordName} = lists:keyfind(<<"_record">>, 1, Term),
    list_to_tuple(lists:reverse(lists:foldl(fun({Key, Type}, Values) ->
        EncodedKey = encode_term(Key, atom),
        {EncodedKey, Value} = lists:keyfind(EncodedKey, 1, Term),
        [decode_term(Value, Type) | Values]
    end, [decode_term(RecordName, atom)], Fields)));
decode_term(Term, {set, Type}) when is_list(Term) ->
    sets:from_list(lists:map(fun(Value) ->
        decode_term(Value, Type)
    end, Term));
decode_term(Term, [Type]) when is_list(Term) ->
    lists:map(fun(Value) -> decode_term(Value, Type) end, Term);
decode_term(Term, Types) when is_list(Term), is_list(Types) ->
    lists:map(fun({Value, Type}) ->
        decode_term(Value, Type)
    end, lists:zip(Term, Types));
decode_term(Term, {Type}) when is_list(Term) ->
    list_to_tuple(lists:map(fun(Value) ->
        decode_term(Value, Type)
    end, Term));
decode_term(Term, Types) when is_list(Term), is_tuple(Types) ->
    list_to_tuple(lists:map(fun({Value, Type}) ->
        decode_term(Value, Type)
    end, lists:zip(Term, tuple_to_list(Types))));
decode_term({Term}, Type) when is_list(Term), is_map(Type) ->
    [{KeyType, ValueType}] = maps:to_list(Type),
    lists:foldl(fun({Key, Value}, Map) ->
        DecodedKey = decode_key(Key, KeyType),
        DecodedValue = decode_term(Value, ValueType),
        maps:put(DecodedKey, DecodedValue, Map)
    end, #{}, Term);
decode_term(Term, Type) ->
    error({invalid_json_structure, Term, Type}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Encodes a record key to erlang json format with given structure.
%% @end
%%--------------------------------------------------------------------
-spec encode_key(term(), record_key()) -> binary().
encode_key(Key, atom) when is_atom(Key) ->
    atom_to_binary(Key, utf8);
encode_key(Key, boolean) when is_boolean(Key) ->
    atom_to_binary(Key, utf8);
encode_key(Key, binary) when is_binary(Key) ->
    base64:encode(Key);
encode_key(Key, float) when is_integer(Key) ->
    integer_to_binary(Key);
encode_key(Key, float) when is_float(Key) ->
    float_to_binary(Key);
encode_key(Key, integer) when is_integer(Key) ->
    integer_to_binary(Key);
encode_key(Key, string) when is_binary(Key) ->
    Key;
encode_key(Key, term) ->
    base64:encode(term_to_binary(Key));
encode_key(Key, Type) ->
    error({invalid_key_type, Key, Type}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Decodes a record key from erlang json format with given structure.
%% @end
%%--------------------------------------------------------------------
-spec decode_key(binary(), record_key()) -> term().
decode_key(Key, atom) when is_binary(Key) ->
    binary_to_atom(Key, utf8);
decode_key(Key, boolean) when is_binary(Key) ->
    binary_to_atom(Key, utf8);
decode_key(Key, binary) when is_binary(Key) ->
    base64:decode(Key);
decode_key(Key, float) when is_binary(Key) ->
    case binary:split(Key, <<".">>) of
        [_] -> binary_to_integer(Key);
        [_ | _] -> binary_to_float(Key)
    end;
decode_key(Key, integer) when is_binary(Key) ->
    binary_to_integer(Key);
decode_key(Key, string) when is_binary(Key) ->
    Key;
decode_key(Key, term) when is_binary(Key) ->
    binary_to_term(base64:decode(Key));
decode_key(Key, Type) ->
    error({invalid_key_type, Key, Type}).