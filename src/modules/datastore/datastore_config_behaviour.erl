%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% His behaviour is used to inject datastore config related to use of
%%% specific models.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config_behaviour).
-author("Michal Zmuda").

%%--------------------------------------------------------------------
%% @doc
%% List of models to be used. Should not contain modules used by default.
%% @end
%%--------------------------------------------------------------------
-callback models() -> Models :: [model_behaviour:model_type()].

%%--------------------------------------------------------------------
%% @doc
%% List of models to be throttled.
%% @end
%%--------------------------------------------------------------------
-callback throttled_models() -> Models :: [model_behaviour:model_type()].

%%--------------------------------------------------------------------
%% @doc
%% Sets mutator of document.
%% @end
%%--------------------------------------------------------------------
-callback get_mutator() -> datastore:mutator() | undefined.