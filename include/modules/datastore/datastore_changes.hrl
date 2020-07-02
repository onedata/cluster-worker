%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header defines datastore changes macros.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_CHANGES_HRL).
-define(DATASTORE_CHANGES_HRL, 1).

-include("global_definitions.hrl").

% Option used to query views by changes streams
-define(CHANGES_STALE_OPTION,
    application:get_env(?CLUSTER_WORKER_APP_NAME, changes_stale_view_option, false)).

-endif.
