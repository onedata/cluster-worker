%%%-------------------------------------------------------------------
%%% @author Michał Stanisz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains datastore append list records definitions.
%%% For detailed description checkout {@link append_list} module.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(APPEND_LIST_HRL).
-define(APPEND_LIST_HRL, 1).

% This record holds pointers to the first and last node of list.
% Only one record of this type is persisted for each structure 
% so `id` of this record is equivalent to id of whole structure.
% Because this record represents whole structure it also holds information 
% describing given instance of the structure(e.g. `max_elements_per_node`).
-record(sentinel, {
    structure_id :: append_list:id(),
    max_elements_per_node :: pos_integer(),
    first = undefined :: append_list:id() | undefined,
    last = undefined :: append_list:id() | undefined
}).

-record(node, {
    structure_id :: append_list:id(),
    node_id :: append_list:id(),
    prev = undefined :: append_list:id() | undefined,
    next = undefined :: append_list:id() | undefined,
    elements = #{} :: append_list:elements_map(),
    min_on_left :: append_list:key() | undefined,
    max_on_right :: append_list:key() | undefined,
    node_number :: append_list:node_number()
}).

-endif.
