%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This header defines records used by datastore_doc.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_DOC_HRL).
-define(DATASTORE_DOC_HRL, 1).

% data             - initial datastore document process data
% rev              - initial datastore document process revision
% idle_timeout     - how long datastore document process can be in committed
%                    state before termination
% min_commit_delay - minimal delay between consecutive commit operations
% max_commit_delay - minimal delay between consecutive commit operations
-record(datastore_doc_init, {
    data :: datastore_doc:data(),
    rev :: datastore_doc:rev(),
    idle_timeout :: timeout(),
    min_commit_delay :: timeout(),
    max_commit_delay :: timeout()
}).

-endif.
