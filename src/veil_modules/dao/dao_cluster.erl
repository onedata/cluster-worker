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

-include_lib("veil_modules/dao/dao.hrl").
-include_lib("veil_modules/dao/couch_db.hrl").

%% API
-export([save_state/2, save_state/1, get_state/1, get_state/0, clear_state/1, clear_state/0]).

-ifdef(TEST).
-compile([export_all]).
-endif.

%% ===================================================================
%% API functions
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
    dao:save_record(#document{record = Rec, force_update = true, uuid = Id}).


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
            {ok, State#document.record};
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
%% Internal functions
%% ===================================================================
    
