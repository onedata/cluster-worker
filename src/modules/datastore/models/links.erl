%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Helper module for 'links' record.
%%% @end
%%%-------------------------------------------------------------------
-module(links).
-author("Rafal Slota").

-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_internal_model.hrl").

-export([model_init/0]).
-export([record_struct/1]).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(links, []).

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of the record in specified version.
%% @end
%%--------------------------------------------------------------------
-spec record_struct(datastore_json:record_version()) -> datastore_json:record_struct().
record_struct(1) ->
    {record, [
        {doc_key, term},
        {model, atom},
        {link_map, #{term => {integer, [{binary, term, term, atom}]}}},
        {children, #{integer => binary}},
        {origin, binary}
    ]}.

