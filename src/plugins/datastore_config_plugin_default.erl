%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module defines datastore config related to op_worker.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config_plugin_default).
-author("Michal Zmuda").

-behaviour(datastore_config_behaviour).

%% datastore_config_behaviour callbacks
-export([models/0, throttled_models/0, get_mutator/0]).

%%--------------------------------------------------------------------
%% @doc
%% {@link datastore_config_behaviour} callback models/0.
%% @end
%%--------------------------------------------------------------------
-spec models() -> Models :: [model_behaviour:model_type()].
models() -> [].

%%--------------------------------------------------------------------
%% @doc
%% {@link datastore_config_behaviour} callback throttled_models/0.
%% @end
%%--------------------------------------------------------------------
-spec throttled_models() -> Models :: [model_behaviour:model_type()].
throttled_models() -> [].

%%--------------------------------------------------------------------
%% @doc
%% {@link datastore_config_behaviour} callback get_mutator/0.
%% @end
%%--------------------------------------------------------------------
-spec get_mutator() -> datastore:mutator() | undefined.
get_mutator() ->
  undefined.