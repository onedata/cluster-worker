%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Definitions connected to map of running workers
%%% @end
%%%-------------------------------------------------------------------
-author("Tomasz Lichon").

-define(worker_map_ets, workers_ets).

-define(default_worker_selection_type, random).
-type(selection_type() :: random | prefere_local).