%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore sliding proplist records definitions.
%%% For detailed description checkout {@link sliding_proplist} module.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(SLIDING_PROPLIST_HRL).
-define(SLIDING_PROPLIST_HRL, 1).

% This record holds pointers to the first and last node of list.
% Only one record of this type is persisted for each structure 
% so `id` of this record is equivalent to id of whole structure.
% Because this record represents whole structure it also holds information 
% describing given instance of the structure(e.g. `max_elements_per_node`).
-record(sentinel, {
    structure_id :: sliding_proplist:id(),
    max_elements_per_node :: pos_integer(),
    first = undefined :: sliding_proplist:id() | undefined,
    last = undefined :: sliding_proplist:id() | undefined
}).

-record(node, {
    structure_id :: sliding_proplist:id(),
    node_id :: sliding_proplist:node_id(),
    node_number :: sliding_proplist:node_number(),
    prev = undefined :: sliding_proplist:node_id() | undefined,
    next = undefined :: sliding_proplist:node_id() | undefined,
    elements = #{} :: sliding_proplist:elements_map(),
    max_in_node :: sliding_proplist:key() | undefined,
    min_in_node :: sliding_proplist:key() | undefined,
    min_in_newer_nodes :: sliding_proplist:key() | undefined,
    max_in_older_nodes :: sliding_proplist:key() | undefined
}).

-endif.
