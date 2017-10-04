%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc High Level Mnesia database driver.
%%% @end
%%%-------------------------------------------------------------------
-module(memory_store_driver_router).
-author("Rafal Slota").

-include("global_definitions.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/datastore/datastore_engine.hrl").
-include_lib("ctool/include/logging.hrl").
-include("timeouts.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-type ctx() :: datastore_context:driver_ctx().

%% API
-export([call/3]).

%% for apply
-export([get/2, exists/2, fetch_link/3, foreach_link/4]).
%% for rpc
-export([direct_call_internal/5, direct_link_call_internal/5, execute_local/4]).

-define(EXTENDED_ROUTING, [get, exists, fetch_link, foreach_link]).
-define(LINK_DRIVER, memory_store_driver_links).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Routes call to appropriate node/tp process.
%% @end
%%--------------------------------------------------------------------
-spec call(Function :: atom(), ctx(), Args :: [term()]) ->
    term().
call(save, #{links_tree := {true, DocKey}} = Ctx, Args) ->
    execute(Ctx, DocKey, true, {save, Args});
call(Method, Ctx, [#document{key = Key} | _] = Args) ->
    execute(Ctx, Key, false, {Method, Args});
call(Method, #{links_tree := LinkOp} = Ctx, Args) ->
    case lists:member(Method, ?EXTENDED_ROUTING) of
        true ->
            apply(?MODULE, Method, [Ctx | Args]);
        _ ->
            [Key | _] = Args,
            case LinkOp of
                false ->
                    execute(Ctx, Key, false, {Method, Args});
                _ ->
                    execute(Ctx, Key, true, {Method, Args})
            end
    end.

%%%===================================================================
%%% Extended routing functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(ctx(), datastore:ext_key()) ->
    {ok, datastore:document()} | datastore:get_error().
get(#{links_tree := false, model_name := MN,
    persistence := Persistence} = Ctx, Key) ->
    case Persistence of
        false ->
            case direct_call(get, Ctx, Key, [Key]) of
                {error, key_enoent} ->
                    {error, {not_found, MN}};
                {ok, #document{deleted = true}} ->
                    {error, {not_found, MN}};
                Ans ->
                    Ans
            end;
        _ ->
            direct_call(get, Ctx, Key, [Key], {error, key_enoent})
    end;
get(#{links_tree := {true, MainDocKey}, model_name := MN,
    persistence := Persistence} = Ctx, DocKey) ->
    case Persistence of
        false ->
            case direct_link_call(get, Ctx, MainDocKey, [DocKey]) of
                {error, key_enoent} ->
                    {error, {not_found, MN}};
                {ok, #document{deleted = true}} ->
                    {error, {not_found, MN}};
                Ans ->
                    Ans
            end;
        _ ->
            direct_link_call(get, Ctx, MainDocKey, [DocKey],
                {error, key_enoent})
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback exists/2.
%% @end
%%--------------------------------------------------------------------
-spec exists(ctx(), datastore:ext_key()) ->
    {ok, boolean()} | datastore:generic_error().
exists(Ctx, Key) ->
    case get(Ctx, Key) of
        {error, {not_found, _}} -> {ok, false};
        {ok, _} -> {ok, true};
        Error -> Error
    end.


%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback fetch_link/3.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(ctx(), datastore:ext_key(), datastore:link_name()) ->
    {ok, datastore:link_target()} | datastore:link_error().
fetch_link(#{model_name := MN, persistence := Persistence} = Ctx, Key, LinkName) ->
    case Persistence of
        false ->
            direct_link_call(fetch_link, Ctx, Key, [Key, LinkName]);
        _ ->
            direct_link_call(fetch_link, Ctx, Key, [Key, LinkName],
                {error, {not_found_in_memory, MN}})
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link store_driver_behaviour} callback foreach_link/4.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(ctx(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
foreach_link(#{model_name := MN, persistence := Persistence} = Ctx,
    Key, Fun, AccIn) ->
    Ans = case Persistence of
        false ->
            direct_link_call(foreach_link, Ctx, Key, [Key, Fun, AccIn]);
        _ ->
            direct_link_call(foreach_link, Ctx, Key, [Key, Fun, AccIn],
                {error, {not_found_in_memory, MN}})
    end,
    case Ans of
        {throw, Exception} -> throw(Exception);
        _ -> Ans
    end.

%%%===================================================================
%%% Routing functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_call(Op :: atom(), ctx(),
    Key :: datastore:ext_key(), Args :: list()) -> term().
direct_call(Op, Ctx, Key, Args) ->
    Node = get_hashing_node(Ctx, Key),

    rpc:call(Node, datastore_cache, Op, [Ctx | Args]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_call(Op :: atom(), ctx(),
    Key :: datastore:ext_key(), Args :: list(), CheckAns :: term()) -> term().
direct_call(Op, Ctx, Key, Args, CheckAns) ->
    Node = get_hashing_node(Ctx, Key),

    rpc:call(Node, ?MODULE, direct_call_internal, [Op, Ctx, Key, Args, CheckAns]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at local node.
%% @end
%%--------------------------------------------------------------------
-spec direct_call_internal(Op :: atom(), ctx(),
    Key :: datastore:ext_key(), Args :: list(), CheckAns :: term()) -> term().
direct_call_internal(Op, #{model_name := MN} = Ctx, Key, Args, CheckAns) ->
    case apply(datastore_cache, Op, [Ctx | Args]) of
        CheckAns ->
            execute_local(Ctx, Key, false, {Op, Args});
        {ok, #document{deleted = true}} ->
            {error, {not_found, MN}};
        RPCAns ->
            RPCAns
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_link_call(Op :: atom(), ctx(),
    Key :: datastore:ext_key(), Args :: list()) -> term().
direct_link_call(Op, Ctx, Key, Args) ->
    Node = get_hashing_node(Ctx, Key),

    rpc:call(Node, ?LINK_DRIVER, Op,
        [datastore_context:override(get_method, get_direct, Ctx) | Args]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_link_call(Op :: atom(), ctx(),
    Key :: datastore:ext_key(), Args :: list(), CheckAns :: term()) -> term().
direct_link_call(Op, Ctx, Key, Args, CheckAns) ->
    Node = get_hashing_node(Ctx, Key),

    rpc:call(Node, ?MODULE, direct_link_call_internal,
        [Op, Ctx, Key, Args, CheckAns]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation at appropriate node.
%% @end
%%--------------------------------------------------------------------
-spec direct_link_call_internal(Op :: atom(), ctx(),
    Key :: datastore:ext_key(), Args :: list(), CheckAns :: term()) -> term().
direct_link_call_internal(Op, Ctx, Key, Args, CheckAns) ->
    case apply(?LINK_DRIVER, Op,
        [datastore_context:override(get_method, get_direct, Ctx) | Args]) of
        CheckAns ->
            execute_local(Ctx, Key, true, {Op, Args});
        RPCAns2 ->
            RPCAns2
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation in appropriate process.
%% @end
%%--------------------------------------------------------------------
-spec execute(ctx(), Key :: datastore:ext_key(),
    Link :: boolean(), {Op :: atom(), Args :: list()}) -> term().
execute(Ctx, Key, Link, Msg) ->
    Node = get_hashing_node(Ctx, Key),
    rpc:call(Node, ?MODULE, execute_local, [Ctx, Key, Link, Msg]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes operation in appropriate process at local node.
%% @end
%%--------------------------------------------------------------------
-spec execute_local(ctx(), Key :: datastore:ext_key(),
    Link :: boolean(), {Op :: atom(), Args :: list()}) -> term().
execute_local(#{model_name := MN, level := L,
    persistence := Persistence} = Ctx, Key, Link, {Op, _} = Msg) ->
    TpArgs = [Key, Link, Persistence],
    TPKey = {MN, Key, Link, L},

    case caches_controller:throttle(MN) of
        ok ->
            datastore_doc:run_sync(TpArgs, TPKey, {Ctx, Msg});
        Error ->
            Error
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets key for consistent hashing algorithm.
%% @end
%%--------------------------------------------------------------------
-spec get_hashing_node(ctx(), Key :: datastore:ext_key()) -> term().
get_hashing_node(#{model_name := MN, locality := Loc}, Key) ->
    case Loc of
        local ->
            node();
        _ ->
            consistent_hasing:get_node({MN, Key})
    end.