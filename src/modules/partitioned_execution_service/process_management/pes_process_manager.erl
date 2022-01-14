%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for management of pes_servers and pes_supervisors
%%% tree. pes_process_manager:
%%% - chooses pes_server for each key (see pes.erl),
%%% - chooses supervisor for each pes_server.
%%%
%%% Choice of pes_server and supervisor can be customized using
%%% get_servers_count/1 and get_supervisor_name/1 callbacks.
%%% Each key is mapped to integer value from 1 to get_servers_count()
%%% using hash_key/2 function one pes_server is used for for
%%% each hash. get_supervisor_name/1 determines supervisor for
%%% each pes_server getting hash as an argument.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_process_manager).
-author("Michal Wrzeszcz").


%% Executor Init/Teardown API
-export([init_registry/1, cleanup_registry/1, init_supervisors/1, terminate_supervisors/1]).
%% Server management API
-export([start_server/2, get_server/2, get_server_if_initialized/2, delete_key_to_server_mapping/3, fold_servers/3]).
%%% API for pes_server
-export([register_server/2, report_server_initialized/2, deregister_server/2]).

%% Test API
-export([hash_key/2, get_supervisor_name/2]).


-type key_hash() :: non_neg_integer(). % hashes of keys are used to choose process for key
-type fold_fun() :: fun((pid(), fold_acc()) -> fold_acc()).
-type fold_acc() :: term().
-export_type([key_hash/0]).


% Process states
-define(INITIALIZING, initializing).
-define(INITIALIZED, initialized).


% Record storing key_hash -> pes_server mapping together with pes_server status
-record(entry, {
    key_hash :: key_hash(),
    server :: pid(),
    server_status :: ?INITIALIZING | ?INITIALIZED | '_' % '_' is used to for select on ets
}).


-define(DEFAULT_SERVERS_COUNT, 100).
-define(DEFAULT_SERVER_GROUPS_COUNT, 1).


%%%===================================================================
%%% Executor Init/Teardown API
%%%===================================================================

-spec init_registry(pes:executor()) -> ok.
init_registry(Executor) ->
    lists:foreach(fun(Name) ->
        ets:new(Name, [set, public, named_table, {read_concurrency, true}, {keypos, 2}])
    end, supervisors_namespace(Executor)).


-spec cleanup_registry(pes:executor()) -> ok.
cleanup_registry(Executor) ->
    lists:foreach(fun(Name) ->
        ets:delete(Name)
    end, supervisors_namespace(Executor)).


-spec init_supervisors(pes:executor()) -> ok.
init_supervisors(Executor) ->
    {RootSupervisor, ChildSupervisors} = get_supervision_tree(Executor),
    lists:foreach(fun(Name) ->
        case supervisor:start_child(RootSupervisor, pes_supervisor:child_spec(Name)) of
            {ok, _} -> ok;
            {error, {already_started, _}} -> ok
        end
    end, ChildSupervisors).


-spec terminate_supervisors(pes:executor()) -> ok.
terminate_supervisors(Executor) ->
    {RootSupervisor, ChildSupervisors} = get_supervision_tree(Executor),
    lists:foreach(fun(Name) ->
        ok = supervisor:terminate_child(RootSupervisor, Name),
        ok = supervisor:delete_child(RootSupervisor, Name)
    end, ChildSupervisors).


%%%===================================================================
%%% Server management API
%%% NOTE: this API is used by calling process that uses keys
%%% to identify pes_servers.
%%%===================================================================

-spec start_server(pes:executor(), pes:key()) -> supervisor:startchild_ret().
start_server(Executor, Key) ->
    KeyHash = hash_key(Executor, Key),
    SupName = get_supervisor_name(Executor, KeyHash),
    supervisor:start_child(SupName, [Executor, KeyHash]).


-spec get_server(pes:executor(), pes:key()) -> {ok, pid()} | {error, not_found}.
get_server(Executor, Key) ->
    KeyHash = hash_key(Executor, Key),
    SupName = get_supervisor_name(Executor, KeyHash),
    case ets:lookup(SupName, KeyHash) of
        [] -> {error, not_found};
        [#entry{key_hash = KeyHash, server = Pid}] -> {ok, Pid}
    end.


-spec get_server_if_initialized(pes:executor(), pes:key()) -> {ok, pid()} | {error, not_found}.
get_server_if_initialized(Executor, Key) ->
    KeyHash = hash_key(Executor, Key),
    SupName = get_supervisor_name(Executor, KeyHash),
    case ets:lookup(SupName, KeyHash) of
        [#entry{key_hash = KeyHash, server = Pid, server_status = ?INITIALIZED}] -> {ok, Pid};
        _ -> {error, not_found}
    end.


%%--------------------------------------------------------------------
%% @doc
%% Deletes key only if it is mapped to pid from 3rd argument. This protects against race when
%% other process already changed mapping so there is no need to use it when the key is deleted
%% from the inside of server (such race is impossible in such a case).
%% @end
%%--------------------------------------------------------------------
-spec delete_key_to_server_mapping(pes:executor(), pes:key(), pid()) -> ok.
delete_key_to_server_mapping(Executor, Key, Pid) ->
    KeyHash = hash_key(Executor, Key),
    SupName = get_supervisor_name(Executor, KeyHash),
    ets:select_delete(SupName, ets:fun2ms(
        fun(#entry{key_hash = KH, server = Server}) when KH =:= KeyHash, Server =:= Pid -> true end)),
    ets:select_delete(SupName, [{#entry{key_hash = KeyHash, server = Pid, server_status = '_'}, [], [true]}]),
    ok.


-spec fold_servers(pes:executor(), fold_fun(), fold_acc()) -> fold_acc().
fold_servers(Executor, Fun, InitialAcc) ->
    lists:foldl(fun(Name, Acc) ->
        ets:foldl(fun
            (#entry{server = Pid}, InternalAcc) ->
                Fun(Pid, InternalAcc);
            (_, InternalAcc) ->
                InternalAcc
        end, Acc, Name)
    end, InitialAcc, supervisors_namespace(Executor)).


%%%===================================================================
%%% API for pes_server
%%% NOTE: this API is used by pes_server to report its state and uses
%%% hashes to identify itself.
%%%===================================================================

-spec register_server(pes:executor(), key_hash()) -> ok | {error, already_exists}.
register_server(Executor, KeyHash) ->
    SupName = get_supervisor_name(Executor, KeyHash),
    case ets:insert_new(SupName, #entry{key_hash = KeyHash, server = self(), server_status = ?INITIALIZING}) of
        true -> ok;
        false -> {error, already_exists}
    end.


-spec report_server_initialized(pes:executor(), key_hash()) -> ok.
report_server_initialized(Executor, KeyHash) ->
    SupName = get_supervisor_name(Executor, KeyHash),
    ets:insert(SupName, #entry{key_hash = KeyHash, server = self(), server_status = ?INITIALIZED}),
    ok.


-spec deregister_server(pes:executor(), key_hash()) -> ok.
deregister_server(Executor, KeyHash) ->
    SupName = get_supervisor_name(Executor, KeyHash),
    ets:delete(SupName, KeyHash),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec hash_key(pes:executor(), pes:key()) -> key_hash().
hash_key(Executor, KeyToHash) ->
    HashesSpaceSize = pes_server_utils:call_optional_callback(
        Executor, get_servers_count, [], fun() -> ?DEFAULT_SERVERS_COUNT end, do_not_catch_errors),

    KeyBin = case is_binary(KeyToHash) of
        true -> KeyToHash;
        false -> crypto:hash(md5, term_to_binary(KeyToHash))
    end,

    Id = binary:decode_unsigned(KeyBin),
    Id rem HashesSpaceSize.



%% @private
-spec get_supervisor_name(pes:executor(), key_hash()) -> pes_supervisor:name().
get_supervisor_name(Executor, KeyHash) ->
    SupervisorSpaceSize = pes_server_utils:call_optional_callback(
        Executor, get_server_groups_count, [], fun() -> ?DEFAULT_SERVER_GROUPS_COUNT end, do_not_catch_errors),
    RootSupervisor = Executor:get_root_supervisor(),

    case SupervisorSpaceSize of
        1 ->
            RootSupervisor;
        _ ->
            list_to_atom(atom_to_list(RootSupervisor) ++ integer_to_list(KeyHash rem SupervisorSpaceSize))
    end.


%% @private
-spec supervisors_namespace(pes:executor()) -> [pes_supervisor:name()].
supervisors_namespace(Executor) ->
    case get_supervision_tree(Executor) of
        {RootSupervisor, []} -> [RootSupervisor];
        {_RootSupervisor, ChildSupervisors} -> ChildSupervisors
    end.


%% @private
-spec get_supervision_tree(pes:executor()) ->
    {RootSupervisor :: pes_supervisor:name(), [ChildSupervisor:: pes_supervisor:name()]}.
get_supervision_tree(Executor) ->
    SupervisorSpaceSize = pes_server_utils:call_optional_callback(
        Executor, get_server_groups_count, [], fun() -> ?DEFAULT_SERVER_GROUPS_COUNT end, do_not_catch_errors),
    RootSupervisor = Executor:get_root_supervisor(),

    case SupervisorSpaceSize of
        1 ->
            {RootSupervisor, []};
        _ ->
            ChildSupervisors = lists:map(fun(Num) ->
                list_to_atom(atom_to_list(RootSupervisor) ++ integer_to_list(Num))
            end, lists:seq(0, SupervisorSpaceSize - 1)),
            {RootSupervisor, ChildSupervisors}
    end.