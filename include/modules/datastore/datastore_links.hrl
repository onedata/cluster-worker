%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore links records definitions.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_LINKS_HRL).
-define(DATASTORE_LINKS_HRL, 1).

-record(link, {
    tree_id :: datastore_links:tree_id(),
    name :: datastore_links:link_name(),
    target :: datastore_links:link_target(),
    rev :: datastore_links:link_rev()
}).

-endif.
