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
-module(datastore_json).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([encode/1, decode/1]).

-type value() :: datastore_doc:value().
-type doc() :: datastore_doc:doc(value()).
-type record_field() :: atom().
-type record_key() :: atom | boolean | binary | float | integer | string | term.
-type record_value() :: record_key() | json | custom_coder() |
                        record_struct() | [record_value()] | {record_value()} |
                        #{record_key() => record_value()}.
%% encoder Mod:Encoder(Term) or Mod:Encoder(Type, Term) shell return JSON binary
%% decoder Mod:Decoder(JSON) or Mod:Encoder(Type, JSON) shell return Erlang term
%% Encoder defaults to 'encode_value' and Decoder defaults to 'decode_value'
-type encoder() :: atom().
-type decoder() :: atom().
-type custom_coder() ::{custom, json | string, {module(), encoder(), decoder()}}.
-type record_struct() :: {record, [{record_field(), record_value()}]}.
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
-spec encode(doc() | ejson()) -> ejson() | no_return().
encode(#document{key = ?TEST_DOC_KEY} = Doc) -> % Test document
    {[
        {<<"_key">>, Doc#document.key},
        {<<"_scope">>, Doc#document.scope},
        {<<"_record">>, <<"test_doc">>}
    ]};
encode(#document{value = Value, version = Version} = Doc) ->
    Model = element(1, Value),
    case lists:member(Model, datastore_config:get_models()) of
        true ->
            {Props} = encode_term(Value, Model:get_record_struct(Version)),
            {[
                {<<"_key">>, Doc#document.key},
                {<<"_scope">>, Doc#document.scope},
                {<<"_mutators">>, Doc#document.mutators},
                {<<"_revs">>, Doc#document.revs},
                {<<"_seq">>, Doc#document.seq},
                {<<"_timestamp">>, Doc#document.timestamp},
                {<<"_deleted">>, Doc#document.deleted},
                {<<"_version">>, Version} |
                Props
            ]};
        false ->
            throw({invalid_doc, Doc})
    end;
encode(EJson) ->
    EJson.

%%--------------------------------------------------------------------
%% @doc
%% Decodes a document from erlang json format with given structure.
%% @end
%%--------------------------------------------------------------------
-spec decode(ejson()) -> ejson() | doc().
decode({Term} = EJson) when is_list(Term) ->
    case lists:keyfind(<<"_key">>, 1, Term) of
        {_, ?TEST_DOC_KEY = Key} ->
            {_, Scope} = lists:keyfind(<<"_scope">>, 1, Term),
            #document{
                key = Key,
                value = undefined,
                scope = Scope
            };
        {_, Key} ->
            RecordName2 = case lists:keyfind(<<"_record">>, 1, Term) of
                {_, RecordName} -> decode_term(RecordName, atom);
                false -> undefined
            end,
            case lists:member(RecordName2, datastore_config:get_models()) of
                true ->
                    {_, Scope} = lists:keyfind(<<"_scope">>, 1, Term),
                    {_, Mutators} = lists:keyfind(<<"_mutators">>, 1, Term),
                    {_, Revs} = lists:keyfind(<<"_revs">>, 1, Term),
                    {_, Seq} = lists:keyfind(<<"_seq">>, 1, Term),
                    {_, Deleted} = lists:keyfind(<<"_deleted">>, 1, Term),
                    {_, Version} = lists:keyfind(<<"_version">>, 1, Term),
                    Model = datastore_versions:rename_record(Version, RecordName2),
                    Record = decode_term(EJson, Model:get_record_struct(Version)),
                    {Version2, Record2} = datastore_versions:upgrade_record(
                        Version, Model, Record
                    ),

                    Timestamp = case lists:keyfind(<<"_timestamp">>, 1, Term) of
                        false -> null;
                        {_, Value} -> Value
                    end,

                    #document{
                        key = Key,
                        value = Record2,
                        scope = Scope,
                        mutators = Mutators,
                        revs = Revs,
                        seq = Seq,
                        timestamp = Timestamp,
                        deleted = Deleted,
                        version = Version2
                    };
                false ->
                    EJson
            end;
        _ ->
            EJson
    end;
decode(EJson) ->
    EJson.

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
encode_term(Term, string_or_integer) when is_binary(Term) ->
    Term;
encode_term(Term, string_or_integer) when is_integer(Term) ->
    Term;
encode_term(Term, term) ->
    base64:encode(term_to_binary(Term));
encode_term(Term, {custom, json, {Mod, Encoder, _Decoder}}) ->
    encode_term(Mod:Encoder(Term), json);
encode_term(Term, {custom, string, {Mod, Encoder, _Decoder}}) ->
    encode_term(Mod:Encoder(Term), string);
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
decode_term(Term, string_or_integer) when is_binary(Term) ->
    Term;
decode_term(Term, string_or_integer) when is_integer(Term) ->
    Term;
decode_term(Term, term) when is_binary(Term) ->
    binary_to_term(base64:decode(Term));
decode_term(Term, {custom, json, {Mod, _Encoder, Decoder}}) ->
    Mod:Decoder(decode_term(Term, json));
decode_term(Term, {custom, string, {Mod, _Encoder, Decoder}}) ->
    Mod:Decoder(decode_term(Term, string));
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