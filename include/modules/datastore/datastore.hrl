%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header defines datastore macros.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_HRL).
-define(DATASTORE_HRL, 1).

-include("global_definitions.hrl").

%% ETS for counters of changes streams
-define(CHANGES_COUNTERS, changes_counters).

% Key of document used during datastore initialization to test couchbase state
% (datastore init will wait until test document is successfully saved)
-define(TEST_DOC_KEY, <<"InitTestKey">>).

% Option used to query views by changes streams
-define(CHANGES_STALE_OPTION,
    application:get_env(?CLUSTER_WORKER_APP_NAME, changes_stale_view_option, false)).

-endif.
