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

% id that allows for finding entities (nodes, sentinel) in persistence.
-type id() :: binary().
% this type represents keys of elements which are stored as data in this structure.
-type key() :: integer().
% this type represents values of elements which are stored as data in this structure.
-type value() :: binary().
% representation of one element in batch of elements to add.
-type elem() :: {key(), value()}.

% This record holds pointers to the first and last node of list.
% Only one record of this type is persisted for each structure 
% so `id` of this record is equivalent to id of whole structure.
% Because this record represents whole structure it also holds information 
% describing given instance of the structure(e.g. `max_elems_per_node`).
-record(sentinel, {
    structure_id :: id(),
    max_elems_per_node :: pos_integer(),
    first = undefined :: id() | undefined,
    last = undefined :: id() | undefined
}).

-record(node, {
    sentinel_id :: id(),
    node_id :: id(),
    prev = undefined :: id() | undefined,
    next = undefined :: id() | undefined,
    elements = #{} :: #{key() => value()},
    min_on_left :: integer() | undefined,
    max_on_right :: integer() | undefined,
    % This value allows to continue listing after node, that previous listing finished on, was deleted.
    % Node num of prev node is always lower and similarly num of next node is always higher.
    % For more details consult `append_list_get:find_node/1`.
    node_num :: non_neg_integer()
}).

% This record is used as cache during listing. Holds information where listing should start.
% For more details consult `append_list_get:find_node/1` doc.
-record(internal_listing_data, {
    id :: id() | undefined,
    last_node_id :: id() | undefined,
    % Highest key already listed from last node.
    last_key :: integer() | undefined,
    % Lowest node number(node number of last node) that was encountered during elements listing.
    last_node_num :: integer() | undefined
}).

% Record returned along with listing result, allows to start listing elements 
% at the point where previous listing finished.
-record(listing_info, {
    finished = false ::  boolean() | undefined,
    internal_listing_data = #internal_listing_data{} :: #internal_listing_data{}
}).

-endif.
