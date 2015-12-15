%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Contains common definitions of types and helper macros.
%%%      This header must be included by model definition files
%%%      and shall not be included anywhere else.
%%%      Intended for external applications use
%%%      (do not use datastore_internal_model file).
%%% @end
%%%-------------------------------------------------------------------
-ifndef(DATASTORE_MODEL_HRL).
-define(DATASTORE_MODEL_HRL, 1).

-include_lib("cluster_worker/include/modules/datastore/datastore_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model_macros.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_common_internal.hrl").

-endif.
