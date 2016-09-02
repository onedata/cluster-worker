%%%-------------------------------------------------------------------
%%% @author Mateusz Paciorek
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc 
%%% This module allows constructing critical sections.
%%% @end
%%%-------------------------------------------------------------------
% TODO VFS-2371 - problem with nested critical sections (change to critical sections in couchdb_datastore_driver to check error).
-module(critical_section).
-author("Mateusz Paciorek").

%% API
-export([run/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Runs Fun in critical section locked on Key.
%% Guarantees that at most one function is running for selected Key in all
%% nodes in current cluster at any given moment.
%%
%% Keys are hierarchical, so eg. locking on [a , b] will implicitly
%% lock on each possible lock in form [a, b | _].
%% E.g. locking on [a, b] will allow to lock on [a, c], but not on [a, b, c].
%% It will also prevent from locking on [a], since locking on [a] would also
%% implicitly lock on [a, b].
%%
%% If Key is not a list, it is treated as top-level key, i.e. [Key].
%% @end
%%--------------------------------------------------------------------
-spec run(Key :: [term()] | term(), Fun :: fun (() -> Result :: term())) ->
    Result :: term().
run(Key, Fun) when is_list(Key) ->
    Nodes = gen_server2:call({global, cluster_manager}, get_nodes),
    {Agent, _} = locks:begin_transaction([{Key, write, Nodes, all}]),
    try
        Fun()
    after
        locks:end_transaction(Agent)
    end;
run(Key, Fun) ->
    run([Key], Fun).

%%%===================================================================
%%% Internal functions
%%%===================================================================
