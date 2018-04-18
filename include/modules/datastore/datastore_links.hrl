%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore links records definitions.
%%% For detailed description checkout {@link datastore_links} module.
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

-record(links_forest, {
    model :: datastore_model:model(),
    key :: datastore:key(),
    trees = #{} :: links_forest:trees()
}).

-record(links_node, {
    model :: datastore_model:model(),
    key :: datastore:key(),
    node :: undefined | links_node:links_node()
}).

-record(links_mask, {
    model :: datastore_model:model(),
    key :: datastore:key(),
    tree_id :: links_tree:id(),
    links = [] :: [{datastore_links:link_name(), datastore_links:link_rev()}],
    next = <<>> :: datastore:key()
}).

-record(links_mask_root, {
    heads :: #{links_tree:id() => datastore:key()},
    tails :: #{links_tree:id() => datastore:key()}
}).

-record(link_token, {
    restart_token :: undefined | datastore_links_iter:forest_it() |
        {cached_token, reference()},
    is_last = false :: boolean()
}).

-endif.
