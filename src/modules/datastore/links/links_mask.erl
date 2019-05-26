%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model of mask of datastore document links tree.
%%% It holds list of links that have been marked as deleted in given revision.
%%% @end
%%%-------------------------------------------------------------------
-module(links_mask).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type ctx() :: datastore:ctx().
-type mask() :: #links_mask{}.
-type link() :: {datastore_links:link_name(), datastore_links:link_rev()}.

-export_type([mask/0, link/0]).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> ctx().
get_ctx() ->
    #{
        model => ?MODULE,
        memory_driver => undefined,
        disc_driver => undefined
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns structure of model in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {model, atom},
        {key, string},
        {tree_id, string},
        {links, [{string_or_integer, string}]},
        {next, string}
    ]}.