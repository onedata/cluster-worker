%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% It is the behaviour of each node_manager plugin.
%%% @end
%%%-------------------------------------------------------------------
-module(node_manager_plugin_behaviour).
-author("Michal Zmuda").

%%--------------------------------------------------------------------
%% @doc
%% This callback is executed when node manager starts. At time
%% of invocation, node_manager is not set init'ed yet. Use to inject
%% custom initialisation.
%% @end
%%--------------------------------------------------------------------
-callback before_init(Args :: term()) -> ok | {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% This callback is executed when cluster has finished to initialize
%% (nagios has reported healthy status).
%% Use to run custom code required for application initialization that might
%% need working services (e.g. database).
%% @end
%%--------------------------------------------------------------------
-callback after_init(Args :: term()) -> ok | {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Extension to handle_call callback from node manager.
%% When none of handle_call matches in node manager,
%% this callback is executed.
%% @end
%%--------------------------------------------------------------------
-callback handle_call_extension(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} |
    {reply, Reply :: term(), NewState :: term(), timeout() | hibernate} |
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Extension to handle_cast callback from node manager.
%% Works like handle_call_extension.
%% @end
%%--------------------------------------------------------------------
-callback handle_cast_extension(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% Extension to handle_info callback from node manager.
%% Works like handle_call_extension.
%% @end
%%--------------------------------------------------------------------
-callback handle_info_extension(Info :: timeout | term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: term()}.

%%--------------------------------------------------------------------
%% @doc
%% This callback is executed when node manager stops.
%% Invoked with same arguments like node manager terminate callback.
%% Returns final result of terminate.
%% @end
%%--------------------------------------------------------------------
-callback on_terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), State :: term()) ->
    term().

%%--------------------------------------------------------------------
%% @doc
%% Callback executed when code_change from node manager is triggered.
%% Invoked before any of node manager actions.
%% @end
%%--------------------------------------------------------------------
-callback on_code_change(OldVsn :: (term() | {down, term()}), State :: term(), Extra :: term()) ->
    {ok, NewState :: term()} | {error, Reason :: term()}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks and returns IP address of node_manager.
%% @end
%%--------------------------------------------------------------------
-callback check_node_ip_address() -> IPV4Addr :: {A :: byte(), B :: byte(), C :: byte(), D :: byte()}.

%%--------------------------------------------------------------------
%% @doc
%% List of listeners to be loaded by node_manager.
%% @end
%%--------------------------------------------------------------------
-callback listeners() -> Listeners :: [atom()].

%%--------------------------------------------------------------------
%% @doc
%% List of modules (accompanied by their configs) to be loaded by node_manager.
%% @end
%%--------------------------------------------------------------------
-callback modules_with_args() -> Models :: [{atom(), [any()]} | {singleton, atom(), [any()]}].

%%--------------------------------------------------------------------
%% @doc
%% List cluster manager nodes to be used by node manager.
%% @end
%%--------------------------------------------------------------------
-callback cm_nodes() -> {ok, Nodes :: [atom()]} | undefined.
%%--------------------------------------------------------------------
%% @doc
%% List db nodes to be used by node manager.
%% @end
%%--------------------------------------------------------------------
-callback db_nodes() -> {ok, Nodes :: [atom()]} | undefined.

%% @doc
%% Returns the name of the application that bases on cluster worker.
%% @end
%%--------------------------------------------------------------------
-callback app_name() -> {ok, Name :: atom()}.

%% @doc
%% Clears memory of node. HighMemUse is true when memory clearing is
%% started because of high memory usage by node. When it is periodic memory
%% cleaning HighMemUse is false.
%% @end
%%--------------------------------------------------------------------
-callback clear_memory(HighMemUse :: boolean()) -> ok.