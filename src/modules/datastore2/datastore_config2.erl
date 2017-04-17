%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides datastore configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config2).
-author("Krzysztof Trzepla").

-export([get_db_hosts/0]).

-type db_host() :: binary().

-export_type([db_host/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns list of database hosts.
%% @end
%%--------------------------------------------------------------------
-spec get_db_hosts() -> [db_host()].
get_db_hosts() ->
    {ok, Nodes} = plugins:apply(node_manager_plugin, db_nodes, []),
    lists:map(fun(Node) ->
        [Host, _Port] = binary:split(atom_to_binary(Node, utf8), <<":">>),
        Host
    end, Nodes).