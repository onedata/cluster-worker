%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header defines datastore macros and records.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_HRL).
-define(DATASTORE_HRL, 1).

%% ETS for counters of changes streams
-define(CHANGES_COUNTERS, changes_counters).

% Key of document used by datastore to test couchbase state
% (datastore init will wait until test document is successfully saved,
% it will be also saved during node stopping after all documents are
% saved to db to allow verification of database state)
-define(TEST_DOC_KEY_PREFIX, "NodeTestKey").
-define(TEST_DOC_KEY, <<?TEST_DOC_KEY_PREFIX, (atom_to_binary(node(), utf8))/binary>>).
-define(TEST_DOC_INIT_VALUE, <<"NodeInit">>).
-define(TEST_DOC_FINAL_VALUE, <<"NodeStopped">>).

% Atoms that define status of last closing procedure
% Procedure is considered successful if all documents created during
% cluster work have been saved to database before application stop.
-define(CLOSING_PROCEDURE_SUCCEEDED, last_closing_procedure_succeeded).
-define(CLOSING_PROCEDURE_FAILED, last_closing_procedure_failed).

% Record used to return additional information from datastore_doc:save_remote function.
% Describes change of sequence number performed by remote cluster.
% NOTE: each cluster has own counter used to set document's seq field value when document is saved
%       to database. As a result, saving of document to database results in override of document's
%       seq field set by remote cluster. Although, information about sequence number set by
%       remote cluster is persisted in `remote_sequences` document's field, the record is needed
%       to keep information about sequence number overridden during save_remote operation.
-record(remote_mutation_info, {
    key :: datastore_doc:key(),
    model :: datastore_model:model(),
    new_seq :: datastore_doc:remote_seq(),
    overridden_seq :: datastore_doc:overridden_seq()
}).

-endif.
