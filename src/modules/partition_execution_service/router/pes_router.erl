%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for management of pes_servers and pes_supervisors
%%% tree. pes_router:
%%% - chooses pes_server for each key (see pes.erl),
%%% - chooses supervisor for each pes_server.
%%%
%%% Choice of pes_server and supervisor can be customized using
%%% hash_key/1 and get_supervisor_name/1 callbacks. hash_key/1 callback
%%% determines pes_server for each key (one pes_server is started for
%%% each hash) while get_supervisor_name/1 determines supervisor for
%%% each pes_server getting hash as an argument.
%%% @end
%%%-------------------------------------------------------------------
-module(pes_router).
-author("Michal Wrzeszcz").


%% Callback Module Init/Teardown API
-export([init_registry/1, cleanup_registry/1, init_supervisors/3, terminate_supervisors/3]).
%% Server management API
-export([start/2, get/2, get_initialized/2, delete_key_if_is_connected_with_pid/3, fold/3]).
%%% API for pes_server
-export([register/2, report_process_initialized/2, deregister/2]).

% Copy of supervisor:sup_ref() type as it is not exported by supervisor module
-type root_sup_ref()  :: Name :: atom()
    | {Name :: atom(), Node :: node()}
    | {'global', Name :: atom()}
    | {'via', Module :: module(), Name :: any()}
    | pid().
-type fold_fun() :: fun((pid(), fold_acc()) -> fold_acc()).
-type fold_acc() :: term().
-export_type([root_sup_ref/0]).

% Process states
-define(INITIALIZING, initializing).
-define(INITIALIZED, initialized).

-define(DEFAULT_HASHES_SPACE_SIZE, 100).


%%%===================================================================
%%% Callback Module Init/Teardown API
%%%===================================================================

-spec init_registry(pes:callback_module()) -> ok.
init_registry(CallbackModule) ->
    lists:foreach(fun(Name) ->
        ets:new(Name, [set, public, named_table, {read_concurrency, true}])
    end, CallbackModule:supervisors_namespace()).


-spec cleanup_registry(pes:callback_module()) -> ok.
cleanup_registry(CallbackModule) ->
    lists:foreach(fun(Name) ->
        ets:delete(Name)
    end, CallbackModule:supervisors_namespace()).


-spec init_supervisors(pes:callback_module(), root_sup_ref(), [pes_supervisor:name()] | all) -> ok.
init_supervisors(CallbackModule, Root, all) ->
    init_supervisors(CallbackModule, Root, CallbackModule:supervisors_namespace() -- [Root]);
init_supervisors(_CallbackModule, Root, SupervisorsToInit) ->
    lists:foreach(fun(Name) ->
        case supervisor:start_child(Root, pes_supervisor:spec(Name)) of
            {ok, _} -> ok;
            {error, {already_started, _}} -> ok
        end
    end, SupervisorsToInit).


-spec terminate_supervisors(pes:callback_module(), root_sup_ref(), [pes_supervisor:name()] | all) -> ok.
terminate_supervisors(CallbackModule, Root, all) ->
    terminate_supervisors(CallbackModule, Root, CallbackModule:supervisors_namespace() -- [Root]);
terminate_supervisors(_CallbackModule, Root, SupervisorsToInit) ->
    lists:foreach(fun(Name) ->
        ok = supervisor:terminate_child(Root, Name),
        ok = supervisor:delete_child(Root, Name)
    end, SupervisorsToInit).


%%%===================================================================
%%% Server management API
%%% NOTE: this API is used by calling process that uses keys
%%% to identify pes_servers.
%%%===================================================================

-spec start(pes:callback_module(), pes:key()) -> supervisor:startchild_ret().
start(CallbackModule, Key) ->
    KeyHash = hash_key(CallbackModule, Key),
    SupName = get_supervisor_name(CallbackModule, KeyHash),
    supervisor:start_child(SupName, [CallbackModule, KeyHash]).


-spec get(pes:callback_module(), pes:key()) -> {ok, pid()} | {error, not_found}.
get(CallbackModule, Key) ->
    KeyHash = hash_key(CallbackModule, Key),
    SupName = get_supervisor_name(CallbackModule, KeyHash),
    case ets:lookup(SupName, KeyHash) of
        [] -> {error, not_found};
        [{KeyHash, Pid, _}] -> {ok, Pid}
    end.


-spec get_initialized(pes:callback_module(), pes:key()) -> {ok, pid()} | {error, not_found}.
get_initialized(CallbackModule, Key) ->
    KeyHash = hash_key(CallbackModule, Key),
    SupName = get_supervisor_name(CallbackModule, KeyHash),
    case ets:lookup(SupName, KeyHash) of
        [{KeyHash, Pid, ?INITIALIZED}] -> {ok, Pid};
        _ -> {error, not_found}
    end.


-spec delete_key_if_is_connected_with_pid(pes:callback_module(), pes:key(), pid()) -> ok.
delete_key_if_is_connected_with_pid(CallbackModule, Key, Pid) ->
    KeyHash = hash_key(CallbackModule, Key),
    SupName = get_supervisor_name(CallbackModule, KeyHash),
    ets:select_delete(SupName, [{{KeyHash, Pid, '_'}, [], [true]}]),
    ok.


-spec fold(pes:callback_module(), fold_fun(), fold_acc()) -> fold_acc().
fold(CallbackModule, Fun, InitialAcc) ->
    lists:foldl(fun(Name, Acc) ->
        ets:foldl(fun
            ({_, Pid, _}, InternalAcc) ->
                Fun(Pid, InternalAcc);
            (_, InternalAcc) ->
                InternalAcc
        end, Acc, Name)
    end, InitialAcc, CallbackModule:supervisors_namespace()).


%%%===================================================================
%%% API for pes_server
%%% NOTE: this API is used by pes_server to report its state and uses
%%% hashes to identify itself.
%%%===================================================================

-spec register(pes:callback_module(), pes:key_hash()) -> ok | {error, already_exists}.
register(CallbackModule, KeyHash) ->
    SupName = get_supervisor_name(CallbackModule, KeyHash),
    case ets:insert_new(SupName, {KeyHash, self(), ?INITIALIZING}) of
        true -> ok;
        false -> {error, already_exists}
    end.


-spec report_process_initialized(pes:callback_module(), pes:key_hash()) -> ok.
report_process_initialized(CallbackModule, KeyHash) ->
    SupName = get_supervisor_name(CallbackModule, KeyHash),
    ets:insert(SupName, {KeyHash, self(), ?INITIALIZED}),
    ok.


-spec deregister(pes:callback_module(), pes:key_hash()) -> ok.
deregister(CallbackModule, KeyHash) ->
    SupName = get_supervisor_name(CallbackModule, KeyHash),
    ets:delete(SupName, KeyHash),
    ok.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
-spec hash_key(pes:callback_module(), pes:key()) -> pes:key_hash().
hash_key(CallbackModule, KeyToHash) ->
    FallbackFun = fun(Key) ->
        KeyBin = case is_binary(Key) of
            true -> Key;
            false -> crypto:hash(md5, term_to_binary(Key))
        end,

        Id = binary:decode_unsigned(KeyBin),
        Id rem ?DEFAULT_HASHES_SPACE_SIZE + 1
    end,
    pes_server_utils:call_optional_callback(CallbackModule, hash_key, [KeyToHash], FallbackFun, do_not_catch_errors).


%% @private
-spec get_supervisor_name(pes:callback_module(), pes:key_hash()) -> pes_supervisor:name().
get_supervisor_name(CallbackModule, KeyHash) ->
    FallbackFun = fun(_KeyHash) ->
        [SupervisorName | _] = CallbackModule:supervisors_namespace(),
        SupervisorName
    end,
    pes_server_utils:call_optional_callback(
        CallbackModule, get_supervisor_name, [KeyHash], FallbackFun, do_not_catch_errors).