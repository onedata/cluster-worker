%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions used to configure ha.
%%% @end
%%%-------------------------------------------------------------------
-module(ha_management).
-author("Michał Wrzeszcz").

-include("modules/datastore/ha.hrl").
-include("global_definitions.hrl").

%% API
-export([get_propagation_method/0, get_backup_nodes/0, get_slave_mode/0]).
-export([master_down/0, master_up/0, change_config/2]).

-type propagation_method() :: call | cast.
-type slave_mode() :: backup | processing. % Does slave processes only backup data or process requests when master is down

-export_type([propagation_method/0, slave_mode/0]).

% Envs used to configure ha
-define(HA_PROPAGATION_METHOD, ha_propagation_method).
-define(SLAVE_MODE, slave_mode).

%%%===================================================================
%%% API getters
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns propagation method used by datastore_cache_writer when working as ha master.
%% @end
%%--------------------------------------------------------------------
-spec get_propagation_method() -> propagation_method().
get_propagation_method() ->
    % TODO - kazdy proces ustawia numer sekwencji ha w ets taki ets jak mapowania kluczy na pidy to mapowanie kluczy na seq)
    % slave tez ustawia numer sekwencji mastera - mozemy w kazdej chwili przeczytac
    application:get_env(?CLUSTER_WORKER_APP_NAME, ?HA_PROPAGATION_METHOD, cast).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of nodes to be used for backup.
%% @end
%%--------------------------------------------------------------------
-spec get_backup_nodes() -> [node()].
get_backup_nodes() ->
    case consistent_hashing:get_key_connected_nodes() of
        1 ->
            [];
        BackupNodesNum ->
            Nodes = get_backup_nodes(node(), consistent_hashing:get_all_nodes()),
            lists:sublist(Nodes, min(BackupNodesNum, length(Nodes)))
    end.

%%--------------------------------------------------------------------
%% @doc
%% Returns working mode for all slaves.
%% @end
%%--------------------------------------------------------------------
-spec get_slave_mode() -> slave_mode().
get_slave_mode() ->
    application:get_env(?CLUSTER_WORKER_APP_NAME, ?SLAVE_MODE, backup).

%%%===================================================================
%%% API to configure processes
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sets information about master failure and sends it to all tp processes.
%% @end
%%--------------------------------------------------------------------
-spec master_down() -> ok.
master_down() ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, ?SLAVE_MODE, processing),
    tp_router:send_to_each(?MASTER_DOWN).

%%--------------------------------------------------------------------
%% @doc
%% Sets information that master is up and sends it to all tp processes.
%% @end
%%--------------------------------------------------------------------
-spec master_up() -> ok.
master_up() ->
    application:set_env(?CLUSTER_WORKER_APP_NAME, ?SLAVE_MODE, backup),
    tp_router:send_to_each(?MASTER_UP).

%%--------------------------------------------------------------------
%% @doc
%% Sets ha configs and sends information to all tp processes.
%% @end
%%--------------------------------------------------------------------
-spec change_config(non_neg_integer(), propagation_method()) -> ok.
change_config(NodesNumber, PropagationMethod) ->
    consistent_hashing:set_key_connected_nodes(NodesNumber),
    application:set_env(?CLUSTER_WORKER_APP_NAME, ?HA_PROPAGATION_METHOD, PropagationMethod),
    tp_router:send_to_each(?CONFIG_CHANGED).

%%add_node() ->
%%    % Przesylamy do procesow zajmujacych sie przenoszonymi kluczami ze staja sie slave'ami dla tych kluczy co sie przenosza
%%    % (wszelkie calle maja traktowac jak proxy - pytanie przez jaki czas bo moze nas to zabolec wydajnosciowo)
%%    % nastepnie dopiero dodajemy nowy node do ukladanki
%%    ok.
%%
%%delete_node() ->
%%    % zaznaczamy wszystkim wygaszzanym procesom ze maja proxowac na node slave'a jedoczenie zanczajac swoja aktywnosc (wchodzac w specjalny stan)
%%    % procesy czekaja tylko na flushe, a slave wie ze flushy nie moze robic dopuki glowny proces nie skonczy
%%    % UWAGA - ha jest obnizone w czasie dodawania/usuwania node'a o jeden poziom (tak jakby byl ustawiony jeden backup node mniej)
%%    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_backup_nodes(MyNode :: node(), AllNodes :: [node()]) -> BackupNodes :: [node()].
get_backup_nodes(MyNode, [MyNode | Nodes]) ->
    Nodes;
get_backup_nodes(MyNode, [Node | Nodes]) ->
    get_backup_nodes(MyNode, Nodes ++ [Node]).