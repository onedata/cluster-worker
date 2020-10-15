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

%% ETS for counters of changes streams
-define(CHANGES_COUNTERS, changes_counters).

% Key of document used by datastore to test couchbase state
% (datastore init will wait until test document is successfully saved,
% it will be also saved after all documents are saved to db
% to allow verification of database state)
-define(TEST_DOC_KEY_BEG, "NodeTestKey").
-define(TEST_DOC_KEY, <<?TEST_DOC_KEY_BEG, (atom_to_binary(node(), utf8))/binary>>).
-define(TEST_DOC_INIT_VALUE, <<"NodeInit">>).
-define(TEST_DOC_FINAL_VALUE, <<"NodeStopped">>).

-endif.
