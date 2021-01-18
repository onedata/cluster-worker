%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header defines datastore errors.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_ERRORS_HRL).
-define(DATASTORE_ERRORS_HRL, 1).

-define(NOT_FOUND, not_found).
-define(PREDICATE_NOT_SATISFIED, not_satisfied).
-define(ALREADY_EXISTS, already_exists).
-define(IGNORED, ignored).
-define(REMOTE_DOC_ALREADY_EXISTS, remote_doc_already_exists).

-endif.
