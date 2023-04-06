%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for management of pes_servers and 
%%% pes_server_supervisors tree. pes_process_manager:
%%% - chooses pes_server for each key (see pes.erl),
%%% - chooses pes_server_supervisors for each pes_server.
%%%
%%% Each PES plug-in can control the number of servers and supervisors using
%%% optional callbacks in the pes_plugin_behaviour. The key space is
%%% distributed equally among the servers using a hash function. 
%%% If needed, servers can be linked to several pes_server_supervisors 
%%% for performance reasons.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_process_manager).
-author("Michal Wrzeszcz").


-include_lib("ctool/include/errors.hrl").
-include_lib("stdlib/include/ms_transform.hrl").


%% Plug-in Init/Teardown API
-export([init_for_plugin/1, teardown_for_plugin/1]).
%% Server management API
-export([start_server/2, get_server/2, get_server_if_initialized/2, delete_key_to_server_mapping/3, fold_servers/3]).
%%% API for pes_server
-export([register_server/3, report_server_initialized/3, deregister_server/2]).

%% Test API
-export([hash_key/2, get_supervisor_name/2]).


-type key_hash() :: non_neg_integer(). % hashes of keys are used to choose process for key
-export_type([key_hash/0]).


% Process states
-define(INITIALIZING, initializing).
-define(INITIALIZED, initialized).

-define(SERVER_SUPERVISOR_NAME(RootSupervisor, Num),
    list_to_atom(atom_to_list(RootSupervisor) ++ integer_to_list(Num))).


% Record storing key_hash -> pes_server mapping together with pes_server status
-record(entry, {
    key_hash :: key_hash(),
    server :: pid(),
    server_status :: ?INITIALIZING | ?INITIALIZED
}).


%%%===================================================================
%%% Plug-in Init/Teardown API
%%%===================================================================

-spec init_for_plugin(pes:plugin()) -> ok.
init_for_plugin(Plugin) ->
    {RootSupervisor, ChildSupervisors} = get_supervision_tree(Plugin),
    lists:foreach(fun(Name) ->
        ets:new(Name, [set, public, named_table, {read_concurrency, true}, {keypos, 2}])
    end, get_server_supervisors(RootSupervisor, ChildSupervisors)),

    lists:foreach(fun(Name) ->
        case supervisor:start_child(RootSupervisor, pes_sup_tree_supervisor:child_spec(Name)) of
            {ok, _} -> ok;
            {error, {already_started, _}} -> ok
        end
    end, ChildSupervisors).


-spec teardown_for_plugin(pes:plugin()) -> ok.
teardown_for_plugin(Plugin) ->
    {RootSupervisor, ChildSupervisors} = get_supervision_tree(Plugin),
    lists:foreach(fun(Name) ->
        ok = supervisor:terminate_child(RootSupervisor, Name),
        ok = supervisor:delete_child(RootSupervisor, Name)
    end, ChildSupervisors),

    lists:foreach(fun(Name) ->
        ets:delete(Name)
    end, get_server_supervisors(RootSupervisor, ChildSupervisors)).


%%%===================================================================
%%% Server management API
%%% NOTE: this API is used by calling process that uses keys
%%% to identify pes_servers.
%%%===================================================================

-spec start_server(pes:plugin(), pes:key()) -> supervisor:startchild_ret().
start_server(Plugin, Key) ->
    KeyHash = hash_key(Plugin, Key),
    SupName = get_supervisor_name(Plugin, KeyHash),
    supervisor:start_child(SupName, [Plugin, KeyHash]).


-spec get_server(pes:plugin(), pes:key()) -> {ok, pid()} | not_alive.
get_server(Plugin, Key) ->
    KeyHash = hash_key(Plugin, Key),
    SupName = get_supervisor_name(Plugin, KeyHash),
    case ets:lookup(SupName, KeyHash) of
        [] -> not_alive;
        [#entry{key_hash = KeyHash, server = Pid}] -> {ok, Pid}
    end.


-spec get_server_if_initialized(pes:plugin(), pes:key()) -> {ok, pid()} | not_alive.
get_server_if_initialized(Plugin, Key) ->
    KeyHash = hash_key(Plugin, Key),
    SupName = get_supervisor_name(Plugin, KeyHash),
    case ets:lookup(SupName, KeyHash) of
        [#entry{key_hash = KeyHash, server = Pid, server_status = ?INITIALIZED}] -> {ok, Pid};
        _ -> not_alive
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes key only if it is mapped to pid from 3rd argument. This protects against race when
%% other process already changed mapping so there is no need to use it when the key is deleted
%% from the inside of server (such race is impossible in such a case).
%% @end
%%--------------------------------------------------------------------
-spec delete_key_to_server_mapping(pes:plugin(), pes:key(), pid()) -> ok.
delete_key_to_server_mapping(Plugin, Key, Pid) ->
    KeyHash = hash_key(Plugin, Key),
    SupName = get_supervisor_name(Plugin, KeyHash),
    ets:select_delete(SupName, ets:fun2ms(
        fun(#entry{key_hash = KH, server = Server}) when KH =:= KeyHash, Server =:= Pid -> true end)),
    ok.


-spec fold_servers(pes:plugin(), fun((pid(), Acc) -> Acc), Acc) -> Acc when Acc :: term().
fold_servers(Plugin, Fun, InitialAcc) ->
    lists:foldl(fun(Name, Acc) ->
        ets:foldl(fun(#entry{server = Pid}, InternalAcc) ->
            Fun(Pid, InternalAcc)
        end, Acc, Name)
    end, InitialAcc, get_server_supervisors(Plugin)).


%%%===================================================================
%%% API for pes_server
%%% NOTE: this API is used by pes_server to report its state and uses
%%% hashes to identify pes_servers.
%%%===================================================================

-spec register_server(pes:plugin(), key_hash(), pid()) -> ok | {error, already_exists}.
register_server(Plugin, KeyHash, Pid) ->
    SupName = get_supervisor_name(Plugin, KeyHash),
    case ets:insert_new(SupName, #entry{key_hash = KeyHash, server = Pid, server_status = ?INITIALIZING}) of
        true -> ok;
        false -> {error, already_exists}
    end.


-spec report_server_initialized(pes:plugin(), key_hash(), pid()) -> ok.
report_server_initialized(Plugin, KeyHash, Pid) ->
    SupName = get_supervisor_name(Plugin, KeyHash),
    ets:insert(SupName, #entry{key_hash = KeyHash, server = Pid, server_status = ?INITIALIZED}),
    ok.


-spec deregister_server(pes:plugin(), key_hash()) -> ok.
deregister_server(Plugin, KeyHash) ->
    SupName = get_supervisor_name(Plugin, KeyHash),
    ets:delete(SupName, KeyHash),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec hash_key(pes:plugin(), pes:key()) -> key_hash().
hash_key(Plugin, KeyToHash) ->
    KeyBin = case is_binary(KeyToHash) of
        true -> KeyToHash;
        false -> crypto:hash(md5, term_to_binary(KeyToHash))
    end,

    Id = binary:decode_unsigned(KeyBin),
    Id rem pes_plugin:get_executor_count(Plugin).



%% @private
-spec get_supervisor_name(pes:plugin(), key_hash()) -> pes_server_supervisor:name().
get_supervisor_name(Plugin, KeyHash) ->
    SupervisorCount = pes_plugin:get_supervisor_count(Plugin),
    RootSupervisor = pes_plugin:get_root_supervisor_name(Plugin),

    case SupervisorCount of
        1 ->
            RootSupervisor;
        _ ->
            ?SERVER_SUPERVISOR_NAME(RootSupervisor, KeyHash rem SupervisorCount)
    end.


%% @private
-spec get_server_supervisors(pes:plugin()) -> [pes_server_supervisor:name()].
get_server_supervisors(Plugin) ->
    {RootSupervisor, ChildSupervisors} = get_supervision_tree(Plugin),
    get_server_supervisors(RootSupervisor, ChildSupervisors).


%% @private
-spec get_server_supervisors(RootSupervisor :: pes_server_supervisor:name(),
    [ChildSupervisor:: pes_server_supervisor:name()]) -> [pes_server_supervisor:name()].
get_server_supervisors(RootSupervisor, []) ->
    [RootSupervisor];
get_server_supervisors(_RootSupervisor, ChildSupervisors) ->
    ChildSupervisors.


%% @private
-spec get_supervision_tree(pes:plugin()) ->
    {RootSupervisor :: pes_server_supervisor:name(), [ChildSupervisor:: pes_server_supervisor:name()]}.
get_supervision_tree(Plugin) ->
    SupervisorCount = pes_plugin:get_supervisor_count(Plugin),
    RootSupervisor = pes_plugin:get_root_supervisor_name(Plugin),

    case SupervisorCount of
        1 ->
            {RootSupervisor, []};
        _ ->
            ChildSupervisors = lists:map(fun(Num) ->
                ?SERVER_SUPERVISOR_NAME(RootSupervisor, Num)
            end, lists:seq(0, SupervisorCount - 1)),
            {RootSupervisor, ChildSupervisors}
    end.