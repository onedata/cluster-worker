%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% This module allows constructing critical sections.
%%% @end
%%%-------------------------------------------------------------------
-module(critical_section).
-author("Mateusz Paciorek").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").

%% API
-export([run/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv run(Key, Fun, false)
%% @end
%%--------------------------------------------------------------------
-spec run(Key :: term(), Fun :: fun (() -> Result :: term())) ->
    Result :: term().
run(RawKey, Fun) ->
    Key = couchdb_datastore_driver:to_binary(RawKey),
    global:trans({Key, self()}, Fun).
