%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module gives high level DB API which contain veil cluster specific utility methods.
%% All DAO API functions should not be called directly. Call dao:handle(_, {cluster, MethodName, ListOfArgs) instead.
%% See {@link dao:handle/2} for more details.
%% @end
%% ===================================================================
-module(dao_cluster).

-include("veil_modules/dao/dao.hrl").
-include("veil_modules/dao/dao_types.hrl").
-include("veil_modules/dao/couch_db.hrl").
-include("logging.hrl").

%% API - cluster state
-export([save_state/2, save_state/1, get_state/1, get_state/0, clear_state/1, clear_state/0]).

%% API - FUSE session
-export([get_fuse_session/1, get_fuse_session/2, save_fuse_session/1, remove_fuse_session/1, close_fuse_session/1, list_fuse_sessions/1]).
-export([clear_sessions/0]).

%% API - FUSE connections
-export([get_connection_info/1, save_connection_info/1, remove_connection_info/1, close_connection/1, list_connection_info/1]).

-ifdef(TEST).
-compile([export_all]).
-endif.

%% ===================================================================
%% API functions
%% ===================================================================

%% ===================================================================
%% Cluster state management
%% ===================================================================

%% save_state/1
%% ====================================================================
%% @doc Saves cluster state Rec to DB with ID = cluster_state.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec save_state(Rec :: tuple()) ->
    {ok, Id :: string()} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
save_state(Rec) when is_tuple(Rec) ->
    save_state(cluster_state, Rec).

%% save_state/2
%% ====================================================================
%% @doc Saves cluster state Rec to DB with ID = Id.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec save_state(Id :: atom(), Rec :: tuple()) ->
    {ok, Id :: string()} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
save_state(Id, Rec) when is_tuple(Rec), is_atom(Id) ->
    dao:save_record(#veil_document{record = Rec, force_update = true, uuid = Id}).


%% get_state/0
%% ====================================================================
%% @doc Retrieves cluster state with ID = cluster_state from DB.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec get_state() -> {ok, term()} | {error, any()}.
%% ====================================================================
get_state() ->
    get_state(cluster_state).


%% get_state/1
%% ====================================================================
%% @doc Retrieves cluster state with UUID = Id from DB.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec get_state(Id :: atom()) -> {ok, term()} | {error, any()}.
%% ====================================================================
get_state(Id) ->
    case dao:get_record(Id) of
        {ok, State} ->
            {ok, State#veil_document.record};
        Other ->
            {error, Other}
    end.


%% clear_state/0
%% ====================================================================
%% @doc Removes cluster state with Id = cluster_state
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec clear_state() ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
clear_state()->
    clear_state(cluster_state).


%% clear_state/2
%% ====================================================================
%% @doc Removes cluster state with given Id
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec clear_state(Id :: atom()) ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
clear_state(Id) ->
    dao:remove_record(Id).


%% ===================================================================
%% FUSE Session management
%% ===================================================================


%% save_fuse_session/1
%% ====================================================================
%% @doc Saves fuse_session record to DB. If #fuse_session.valid_to field is not valid
%%      (i.e. its value is less then current timestamp) it will be set to default value (specified in application config).
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec save_fuse_session(#fuse_session{} | #veil_document{}) ->
    {ok, Id :: string()} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
save_fuse_session(#fuse_session{} = Env) ->
    save_fuse_session(#veil_document{record = Env});
save_fuse_session(#veil_document{record = #fuse_session{valid_to = OldTime}} = Doc) ->
    SessionExpireTime =
        case application:get_env(veil_cluster_node, fuse_session_expire_time) of
            {ok, ETime} -> ETime;
            _           -> 60*60*3 %% Hardcoded 3h, just in case (e.g. eunit tests dont use env variables)
        end,
    CurrTime = fslogic_utils:time(), %% Get current time
    NewTime =   %% Decide which time shall be used (pre-set or generated just now)
        case OldTime of
            OTime when OTime > CurrTime -> OTime;
            _ -> CurrTime + SessionExpireTime
        end,

    %% Save given document
    NewDoc = Doc#veil_document{record = Doc#veil_document.record#fuse_session{valid_to = NewTime}},
    case dao:save_record(NewDoc) of %% Clear cache, just in case
        {ok, UUID}  -> ets:delete(dao_cache, UUID), {ok, UUID};
        Other       -> Other
    end.


%% get_fuse_session/2
%% ====================================================================
%% @doc Gets fuse_session record with given FuseID form DB.
%%      Second argument shall be either {stale, update_before} (will update cache before getting value)
%%      or {stale, ok} which is default - get value from cache. Default behaviour can be also achieved by ommiting
%%      second argument.
%%      Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec get_fuse_session(FuseId :: uuid(), {stale, update_before | ok}) ->
    {ok, #veil_document{}} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
get_fuse_session(FuseId, {stale, update_before}) ->
    ets:delete(dao_cache, FuseId), %% Delete cached entry
    get_fuse_session(FuseId);
get_fuse_session(FuseId, {stale, ok}) ->
    get_fuse_session(FuseId).
get_fuse_session(FuseId) ->
    case ets:lookup(dao_cache, FuseId) of
        [] -> %% Cached document not found. Fetch it from DB and save in cache
            case dao:get_record(FuseId) of
                {ok, Doc} ->
                    ets:insert(dao_cache, {FuseId, Doc}),
                    {ok, Doc};
                Other -> Other
            end;
        [{_, CachedDoc = #veil_document{record = #fuse_session{}}}] -> %% Return document from cache
            {ok, CachedDoc}
    end.


%% remove_fuse_session/1
%% ====================================================================
%% @doc Removes fuse_session record with given FuseID form DB and cache.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec remove_fuse_session(FuseId :: uuid()) ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
remove_fuse_session(FuseId) ->
    ets:delete(dao_cache, FuseId), %% Delete cached entry
    dao:remove_record(FuseId).


%% close_fuse_session/1
%% ====================================================================
%% @doc Removes fuse_session record with given FuseID form DB and cache.
%%      Also deletes all connections that belongs to this session and tries to close them.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec close_fuse_session(FuseId :: uuid()) ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
close_fuse_session(FuseId) ->
    ok = remove_fuse_session(FuseId), %% Remove session from DB
    {ok, Connections} = list_connection_info({by_session_id, FuseId}), %% List all it's connections
    [close_connection(X) || #veil_document{uuid = X}  <- Connections], %% and close them all
    ok.


%% list_fuse_sessions/1
%% ====================================================================
%% @doc Lists fuse_session records using given select condition.
%%      Current implementeation supports fallowing selects:
%%          {by_valid_to, Time} - select all records whose 'valid_to' field is >= Time
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec list_fuse_sessions({by_valid_to, Time :: non_neg_integer()}) ->
    {ok, [#veil_document{}]} | {error, any()}.
%% ====================================================================
list_fuse_sessions({by_valid_to, Time}) ->
    QueryArgs = #view_query_args{start_key = 0, end_key = Time, include_docs = true},
    case dao:list_records(?EXPIRED_FUSE_SESSIONS_VIEW, QueryArgs) of
        {ok, #view_result{rows = Rows}} ->
            {ok, [Session || #view_row{doc = #veil_document{record = #fuse_session{}} = Session} <- Rows]};
        {error, Reason} ->
            {error, Reason}
    end.


%% ===================================================================
%% FUSE Connection Info management
%% ===================================================================


%% save_connection_info/1
%% ====================================================================
%% @doc Saves connection_info record to DB.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec save_connection_info(#connection_info{} | #veil_document{}) ->
    {ok, Id :: string()} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
save_connection_info(ConnInfo) ->
    dao:save_record(ConnInfo).


%% get_connection_info/1
%% ====================================================================
%% @doc Gets connection_info record with given SessID form DB.
%%      Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec get_connection_info(SessID :: uuid()) ->
    {ok, #veil_document{}} |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
get_connection_info(SessID) ->
    dao:get_record(SessID).

%% remove_connection_info/1
%% ====================================================================
%% @doc Removes connection_info record with given SessID form DB.
%%      Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec remove_connection_info(SessID :: uuid()) ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
remove_connection_info(SessID) ->
    dao:remove_record(SessID).


%% close_connection/1
%% ====================================================================
%% @doc Removes connection_info record with given SessID form DB and tries to close it.
%%      This method should not be used unless connection exists. Otherwise it will fail with exception error.
%%      Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec close_connection(SessID :: uuid()) ->
    ok |
    no_return(). % erlang:error(any()) | throw(any())
%% ====================================================================
close_connection(SessID) ->
    {ok, #veil_document{record = #connection_info{controlling_node = CNode, controlling_pid = CPid}}} = get_connection_info(SessID),
    spawn(CNode, fun() -> CPid ! {self(), shutdown} end),
    ok = remove_connection_info(SessID).


%% list_connection_info/1
%% ====================================================================
%% @doc Lists connection_info records using given select condition.
%%      Current implementeation supports fallowing selects:
%%          {by_session_id, SessID} - select all connections that belongs to session with ID - SessID
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec list_connection_info({by_session_id, SessID :: uuid()}) ->
    {ok, [#veil_document{}]} | {error, any()}.
%% ====================================================================
list_connection_info({by_session_id, SessID}) ->
    QueryArgs = #view_query_args{keys = [dao_helper:name(SessID)], include_docs = true},
    case dao:list_records(?FUSE_CONNECTIONS_VIEW, QueryArgs) of
        {ok, #view_result{rows = Rows}} ->
            {ok, [ConnInfo || #view_row{doc = #veil_document{record = #connection_info{}} = ConnInfo} <- Rows]};
        {error, Reason} ->
            {error, Reason}
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% clear_sessions/0
%% ====================================================================
%% @doc Cleanups old, unused sessions from DB. Each session which is expired is checked.
%%      If there is at least one active connection for the session, its expire time will be extended.
%%      Otherwise it will be deleted.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec clear_sessions() ->
    ok | no_return().
%% ====================================================================
clear_sessions() ->
    SPid = self(),
    CurrentTime = fslogic_utils:time(),
    ?info("FUSE session cleanup started. Time: ~p", [CurrentTime]),

    %% List of worker processes that validates sessions in background
    PidList =
        case list_fuse_sessions({by_valid_to, CurrentTime}) of
            {ok, Sessions} ->
                %% [{#veil_document{record = #fuse_session}, {Pid, MRef}}]
                [{X, spawn_monitor(fun() -> SPid ! {self(), check_session(X)} end)} || X <- Sessions];
            {error, Reason} ->
                ?error("Cannot cleanup old fuse sessions. Expired session list fetch failed: ~p", [Reason]),
                exit(Reason)
        end,

    %% Helper function that fetches and processes check_session result from given worker process
    ProcessSession =
        fun(#veil_document{uuid = SessID}, Pid) ->
            receive
                {Pid, ok} -> %% Sessions seems to be active, extend it's expire time
                    {ok, Doc} = get_fuse_session(SessID, {stale, update_before}), %% Get fresh session document
                    {ok, SessionExpireTime} = application:get_env(veil_cluster_node, fuse_session_expire_time),
                    NewTime = fslogic_utils:time() + SessionExpireTime,
                    NewDoc = Doc#veil_document{record = Doc#veil_document.record#fuse_session{valid_to = NewTime}}, %% Update expire time
                    save_fuse_session(NewDoc), %% Save updated document
                    ok;
                {Pid, {error, Reason1}} -> %% Connection is broken, remove it
                    ?info("FUSE Session ~p is broken (~p). Invalidating...", [SessID, Reason1]),
                    close_fuse_session(SessID),
                    session_closed
            after 60000 ->
                timeout
            end
        end,

    %% Iterate over all check_session results and apply ProcessSession/2 on each
    _Res = [ProcessSession(Doc, Pid) || {Doc, {Pid, _}} <- PidList],

    ?debug("FUSE session cleanup ended. Status: ~p", [_Res]),
    ok.


%% check_session/1
%% ====================================================================
%% @doc Checks FUSE session described by given veil_document{}. If session exists and at least one connection is active
%%      'ok' is returned. {error, any()} otherwise. On critical error exception is thrown.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec check_session(#veil_document{}) ->
    ok | {error, no_active_connections} | {error, any()} | no_return().
%% ====================================================================
check_session(#veil_document{record = #fuse_session{}, uuid = SessID}) ->
    SPid = self(),
    %% Get all connections from this session
    PidList =
        case list_connection_info({by_session_id, SessID}) of
            {ok, Connections} ->    %% Invoke 'check_connection' foreach connection
                %% [{#veil_document{record = #connection_info}, {Pid, MRef}}]
                [{X, spawn_monitor(fun() -> SPid ! {self(), check_connection(X)} end)} || X <- Connections];
            {error, Reason} ->
                ?error("Cannot check connections status for FUSE session ~p. View query failed: ~p", [SessID, Reason]),
                throw(Reason)
        end,

    %% Helper function that fetches check_connection/1 result from worker process
    ProcessConnection =
        fun(#veil_document{uuid = ConnID, record = #connection_info{}}, Pid) ->
            Res =
                receive
                    {Pid, ok} -> %% Connection is alive
                        ok;
                    {Pid, {error, _Reason1}} -> %% There was an error during connection check
                        error
                after 5000 ->
                    timeout
                end,
            case Res of
                ok -> ok;
                _  ->
                    %% When connection is not available for some reason, delete it from DB
                    close_connection(ConnID),
                    error
            end
        end,

    %% Iterate over all 'check_connection' instances
    Result = [ProcessConnection(Doc, Pid) || {Doc, {Pid, _}} <- PidList],
    case lists:member(ok, Result) of
        true    -> %% We have at least one active connection
            ok;
        false   -> %% No active connections. Session shall be invalidated
            ?info("There are no acitve connections for session ~p", [SessID]),
            {error, no_active_connections}
    end.


%% check_connection/1
%% ====================================================================
%% @doc Checks FUSE connection decribed by given veil_document. If connection gives response to
%%      internal ping message within 3sec, 'ok' is returned. {error, any()} otherwise.
%% Should not be used directly, use {@link dao:handle/2} instead.
%% @end
-spec check_connection(#veil_document{}) ->
    ok | {error, invalid_session_id} | {error, timeout}.
%% ====================================================================
check_connection(Connection = #veil_document{record = #connection_info{session_id = SessID, controlling_node = CNode, controlling_pid = CPid}}) ->
    SPid = self(),
    spawn(CNode, fun() -> CPid ! {SPid, get_session_id} end),   %% Send ping to connection controlling process
    receive
        {ok, SessID} ->         %% Connection is alive and has valid session ID. Leave it be.
            ok;
        {ok, Inval} ->          %% Connection has invalid session ID. Close it.
            ?warning("Connection ~p has invalid session ID (~p)", [Connection, Inval]),
            {error, invalid_session_id}
    after 3000 ->
        ?warning("Connection ~p is not avalilable due to timeout.", [Connection]),
        {error, timeout}
    end.