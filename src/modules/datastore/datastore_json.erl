%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc JSON encoding for datastore models
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_json).
-author("Rafal Slota").

-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_models_def.hrl").


%%%===================================================================
%%% Types
%%%===================================================================

-type field_name() :: atom().
-type record_key() :: binary | atom | integer | term | string.
-type record_value() :: json %% Raw JSON binary
%% or simple types
| record_key() | boolean | [record_struct()] | {record_struct()} | #{record_key() => record_struct()}
%% or custom value - executes Mod:Encoder(GivenTerm) while encoding and Mod:Decoder(SavedJSON) while decoding.
%% Encoder shall return JSON binary, Decoder shall decode JSON binary to original term.
| {custom_value, {Mod :: atom(), Encoder :: atom(), Decoder :: atom()}}
%% or custom value - executes Mod:Encoder(TypeName, GivenTerm) while encoding and Mod:Decoder(TypeName, SavedJSON) while decoding.
%% Encoder shall return JSON binary, Decoder shall decode JSON binary to original term.
%% You can specify only module name, Decoder defaults to 'encode_value', Decoder defaults to 'decode_value'
| {custom_type, TypeName :: atom(), Mod :: atom()} | {custom_type, TypeName :: atom(), {Mod :: atom(), Encoder :: atom(), Decoder :: atom()}}.
-type record_version() :: non_neg_integer().
-type record_struct() :: record_value()
| {record, record_version(), [{field_name(), record_value()}]} %% Used only internally
| {record, [{field_name(), record_value()}]} %% For defining model structure
| {record, model_behaviour:model_type()}. %% For referencing nasted model
-type ejson() :: term(). %% eJSON


%%%===================================================================
%%% Exports
%%%===================================================================

%% Types
-export_type([record_struct/0, record_version/0]).

%% API
-export([encode_record/1, decode_record/1, validate_struct/1]).
-export([encode_record/2, decode_record/2]).
-export([encode_record/3]).
-export([decode_record_vcs/1, record_upgrade/4]).
-export([get_renamed_models/0]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Maps old model name to new one.
%% @end
%%--------------------------------------------------------------------
-spec get_renamed_models() -> #{OldName :: model_behaviour:model_type() => {RenameVersion :: record_version(), NewName :: model_behaviour:model_type()}}.
get_renamed_models() ->
    maps:merge(plugins:apply(node_manager_plugin, renamed_models, []),
        #{}
    ).


%%--------------------------------------------------------------------
%% @doc
%% Encodes given datastore document to ejson.
%% @end
%%--------------------------------------------------------------------
-spec encode_record(datastore:document()) -> ejson() | no_return().
encode_record(#document{version = undefined, value = Value}) ->
    Type = element(1, Value),
    encode_record(Value, {record, Type});
encode_record(#document{version = Version, value = Value}) when is_integer(Version) ->
    Type = element(1, Value),
    {record, Fields} = Type:record_struct(Version),
    encode_record(Value, {record, Version, Fields}).


%%--------------------------------------------------------------------
%% @doc
%% Encodes given term to ejson with given structure.
%% @end
%%--------------------------------------------------------------------
-spec encode_record(term(), record_struct()) -> ejson() | no_return().
encode_record(Term, Struct) ->
    encode_record(value, Term, Struct).

%%--------------------------------------------------------------------
%% @doc
%% Decodes ejson to term with given structure.
%% @end
%%--------------------------------------------------------------------
-spec decode_record(ejson()) -> {record_version(), term()}.
decode_record({Term}) when is_list(Term) ->
    ModelName0 = decode_record(proplists:get_value(<<?RECORD_TYPE_MARKER>>, Term), atom),
    Version = decode_record(proplists:get_value(<<?RECORD_VERSION_MARKER>>, Term), integer),
    ModelName = maybe_rename_model(ModelName0, Version),
    {record, Fields} = ModelName:record_struct(Version),
    {Version, decode_record({Term}, {record, Version, Fields})}.

%%--------------------------------------------------------------------
%% @doc
%% Decodes ejson to term with given structure. Returns current version of the record.
%% @end
%%--------------------------------------------------------------------
-spec decode_record_vcs(ejson()) -> {WasUpdated :: boolean(), record_version(), term()}.
decode_record_vcs({Term}) when is_list(Term) ->
    {Version, Record} = decode_record({Term}),
    ModelName0 = element(1, Record),
    ModelName = maybe_rename_model(ModelName0, Version),
    #model_config{version = TargetVersion} = ModelName:model_init(),
    {NewVersion, NewRecord} = record_upgrade(ModelName, TargetVersion, Version, Record),
    {Version /= NewVersion, NewVersion, NewRecord}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades given datastore record to requested version.
%% @end
%%--------------------------------------------------------------------
-spec record_upgrade(model_behaviour:model_type(), record_version(), record_version(), term()) ->
    {record_version(), term()}.
record_upgrade(ModelName, TargetVersion, CurrentVersion, Record) when TargetVersion > CurrentVersion ->
    {NextVersion, NextRecord} = ModelName:record_upgrade(CurrentVersion, Record),
    case NextVersion > CurrentVersion of
        true ->
            record_upgrade(ModelName, TargetVersion, NextVersion, NextRecord);
        false ->
            error({record_not_upgraded, {ModelName, TargetVersion, CurrentVersion, Record}})
    end;
record_upgrade(_ModelName, _TargetVersion, CurrentVersion, Record) ->
    {CurrentVersion, Record}.


%%--------------------------------------------------------------------
%% @doc
%% Validates given record structure.
%% @end
%%--------------------------------------------------------------------
-spec validate_struct(record_struct()) -> ok | no_return().
validate_struct({record, Fields}) when is_list(Fields) ->
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Encodes given term to ejson with given structure. Given term can be either
%% encoded as json key or value. This distinction is required since JSON disallows keys with types other then string.
%% @end
%%--------------------------------------------------------------------
-spec encode_record(key | value, term(), record_struct()) -> ejson() | no_return().
encode_record(value, undefined, _) ->
    null;
encode_record(value, Term, {custom_value, {M, Encoder, _Decoder}}) ->
    encode_record(value, M:Encoder(Term), json);
encode_record(value, Term, {custom_type, TypeName, {Mod, Encoder, _Decoder}}) ->
    encode_record(value, Mod:Encoder(Term, TypeName), json);
encode_record(value, Term, {custom_type, TypeName, Mod}) ->
    encode_record(value, Term, {custom_type, TypeName, {Mod, encode_value, decode_value}});
encode_record(_, Term, {record, Type}) when is_atom(Type), is_tuple(Term) ->
    #model_config{version = Version} = Type:model_init(),
    {record, Fields} = Type:record_struct(Version),
    encode_record(value, Term, {record, Version, Fields});
encode_record(value, Term, {record, Version, Fields}) when is_list(Fields), is_tuple(Term) ->
    [RecordType | TupleList] = tuple_to_list(Term),
    {Names, ValueTypes} = lists:unzip(Fields),
    RawMap = lists:zip3(Names, ValueTypes, TupleList),
    {lists:foldl(
        fun({Name, Type, Value}, Map) ->
            [{encode_record(key, Name, atom), encode_record(value, Value, Type)} | Map]
        end,
        [
            {<<?RECORD_TYPE_MARKER>>, encode_record(value, RecordType, atom)},
            {<<?RECORD_VERSION_MARKER>>, encode_record(value, Version, integer)}
        ], RawMap)};
encode_record(_, Term, string) when is_binary(Term) ->
    Term;
encode_record(key, Term, integer) when is_integer(Term) ->
    integer_to_binary(Term);
encode_record(key, Term, integer) when is_float(Term) andalso (Term =:= float(round(Term))) ->
    integer_to_binary(round(Term));
encode_record(value, Term, integer) when is_integer(Term) ->
    Term;
encode_record(value, Term, integer) when is_float(Term) andalso (Term =:= float(round(Term))) ->
    round(Term);
encode_record(key, Term, float) when is_float(Term) ->
    float_to_binary(Term);
encode_record(key, Term, float) when is_integer(Term) ->
    float_to_binary(float(Term));
encode_record(value, Term, float) when is_float(Term) ->
    Term;
encode_record(value, Term, float) when is_integer(Term) ->
    float(Term);
encode_record(value, Term, #{} = Struct) when is_map(Term) ->
    [{KeyType, ValueType}] = maps:to_list(Struct),
    {maps:fold(
        fun(K, V, Acc) ->
            [{encode_record(key, K, KeyType), encode_record(value, V, ValueType)} | Acc]
        end, [], Term)};
encode_record(value, Term, [ValueType]) when is_list(Term) ->
    [encode_record(value, V, ValueType) || V <- Term];
encode_record(_, Term, atom) when is_atom(Term) ->
    atom_to_binary(Term, utf8);
encode_record(value, Term, {set, Type}) ->
    [encode_record(value, E, Type) || E <- sets:to_list(Term)];
encode_record(value, Term, Types) when is_tuple(Types), is_tuple(Term) ->
    Values = tuple_to_list(Term),
    [encode_record(value, V, Type) || {V, Type} <- lists:zip(Values, tuple_to_list(Types))];
encode_record(value, Term, boolean) when is_boolean(Term)  ->
    Term;
encode_record(_, Term, binary) when is_binary(Term)  ->
    base64:encode(Term);
encode_record(_, Term, term) ->
    base64:encode(term_to_binary(Term));
encode_record(value, Term, json) when is_binary(Term) ->
    jiffy:decode(Term);
encode_record(Context, Term, Type)  ->
    error({invalid_term_structure, Context, Term, Type}).


%%--------------------------------------------------------------------
%% @doc
%% Decodes ejson to term with given structure.
%% @end
%%--------------------------------------------------------------------
-spec decode_record(ejson(), record_struct()) -> term().
decode_record(null, _) ->
    undefined;
decode_record(Term, {custom_value, {M, _Encoder, Decoder}}) ->
    M:Decoder(decode_record(Term, json));
decode_record(Term, {custom_type, TypeName, {Mod, _Encoder, Decoder}}) ->
    Mod:Decoder(decode_record(Term, json), TypeName);
decode_record(Term, {custom_type, TypeName, Mod}) ->
    decode_record(Term, {custom_type, TypeName, {Mod, encode_value, decode_value}});
decode_record({Term}, {record, Version, Fields}) when is_list(Fields), is_list(Term) ->
    ModelName0 = decode_record(proplists:get_value(<<?RECORD_TYPE_MARKER>>, Term), atom),
    ModelName = maybe_rename_model(ModelName0, Version),
    list_to_tuple(lists:reverse(lists:foldl(
        fun({Name, Type}, RecordList) ->
            [decode_record(proplists:get_value(encode_record(key, Name, atom), Term), Type) | RecordList]
        end,
        [ModelName], Fields)));
decode_record(Term, string) when is_binary(Term) ->
    Term;
decode_record(Term, integer) when is_integer(Term) ->
    Term;
decode_record(Term, integer) when is_float(Term) andalso (Term =:= float(round(Term))) ->
    round(Term);
decode_record(Term, integer) when is_binary(Term) ->
    binary_to_integer(Term);
decode_record(Term, float) when is_float(Term) ->
    Term;
decode_record(Term, float) when is_integer(Term) ->
    float(Term);
decode_record(Term, float) when is_binary(Term) ->
    binary_to_float(Term);
decode_record({Term}, #{} = Struct) when is_list(Term) ->
    [{KeyType, ValueType}] = maps:to_list(Struct),
    lists:foldl(
        fun({K, V}, Acc) ->
            maps:put(decode_record(K, KeyType), decode_record(V, ValueType), Acc)
        end, #{}, Term);
decode_record(Term, [ValueType]) when is_list(Term) ->
    [decode_record(V, ValueType) || V <- Term];
decode_record(Term, atom) when is_binary(Term) ->
    binary_to_atom(Term, utf8);
decode_record(Term, {set, Type}) when is_list(Term) ->
    sets:from_list([decode_record(E, Type) || E <- Term]);
decode_record(Term, Types) when is_tuple(Types), is_list(Term) ->
    list_to_tuple([decode_record(V, Type) || {V, Type} <- lists:zip(Term, tuple_to_list(Types))]);
decode_record(Term, boolean) when is_boolean(Term)  ->
    Term;
decode_record(Term, boolean) when is_binary(Term)  ->
    binary_to_atom(Term, utf8);
decode_record(Term, binary) when is_binary(Term)  ->
    base64:decode(Term);
decode_record(Term, json) ->
    jiffy:encode(Term);
decode_record(Term, term) ->
    binary_to_term(base64:decode(Term));
decode_record(Term, Type) ->
    error({invalid_json_structure, Term, Type}).



%%--------------------------------------------------------------------
%% @doc
%% Returns current model name base on defined model-name changes.
%% @end
%%--------------------------------------------------------------------
-spec maybe_rename_model(RecordType :: model_behaviour:model_type(), RecordVersion :: record_version()) ->
    model_behaviour:model_type().
maybe_rename_model(RecordType, RecordVersion) ->
    case maps:get(RecordType, ?MODULE:get_renamed_models(), undefined) of
        {RenamedVersion, RenamedTargetModel} when RenamedVersion >= RecordVersion ->
            RenamedTargetModel;
        _ ->
            RecordType
    end.