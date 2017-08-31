%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module is responsible for marking document links as deleted.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_links_mask).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").

%% API
-export([init/4, load/1, terminate/1]).
-export([mark_deleted/3, is_deleted/3]).

-record(mask, {
    ctx :: ctx(),
    key :: key(),
    tree_id :: tree_id(),
    head :: undefined | key(),
    tail :: undefined | key(),
    batch :: undefined | batch()
}).

-type ctx() :: datastore_cache:ctx().
-type key() :: datastore:key().
-type tree_id() :: links_tree:id().
-type link_name() :: datastore_links:link_name().
-type link_rev() :: datastore_links:link_rev().
-type mask() :: #mask{}.
-opaque cache() :: gb_sets:set({link_name(), link_rev()}).
-type batch() :: datastore_doc:batch().

-export_type([mask/0, cache/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes links tree mask.
%% @end
%%--------------------------------------------------------------------
-spec init(ctx(), key(), tree_id(), batch()) -> {ok | {error, term()}, mask()}.
init(Ctx, Key, TreeId, Batch) ->
    MaskRootId = datastore_links:get_mask_root_id(Key),
    Head = Tail = datastore_utils:gen_key(),
    Mask = #mask{
        ctx = Ctx,
        key = Key,
        tree_id = TreeId,
        head = Head,
        tail = Tail
    },
    case datastore_doc:fetch(Ctx, MaskRootId, Batch) of
        {{ok, #document{value = #links_mask_root{
            heads = Heads,
            tails = Tails
        }}}, Batch2} ->
            {ok, Mask#mask{
                head = maps:get(TreeId, Heads, Head),
                tail = maps:get(TreeId, Tails, Tail),
                batch = Batch2
            }};
        {{error, not_found}, Batch2} ->
            {ok, Mask#mask{batch = Batch2}};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, Mask#mask{batch = Batch2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Loads links mask from a collection of changed links_mask documents
%% into a cache object.
%% @end
%%--------------------------------------------------------------------
-spec load(mask()) -> {{ok, cache()} | {error, term()}, mask()}.
load(Mask = #mask{head = Head}) ->
    Cache = gb_sets:new(),
    load(Head, Cache, Mask).

%%--------------------------------------------------------------------
%% @doc
%% Returns documents documents batch.
%% @end
%%--------------------------------------------------------------------
-spec terminate(mask()) -> {ok | {error, term()}, batch()}.
terminate(#mask{batch = undefined}) ->
    {ok, undefined};
terminate(#mask{
    ctx = Ctx, key = Key, tree_id = TreeId, batch = Batch,
    head = Head, tail = Tail
}) ->
    Ctx2 = case maps:get(disc_driver_ctx, Ctx, undefined) of
        undefined -> Ctx;
        DriverCtx -> Ctx#{disc_driver_ctx => DriverCtx#{no_seq => true}}
    end,
    Ctx3 = Ctx2#{sync_enabled => false, sync_change => false},
    MaskPtrId = datastore_links:get_mask_root_id(Key),
    Diff = fun(MaskPtr = #links_mask_root{heads = Heads, tails = Tails}) ->
        {ok, MaskPtr#links_mask_root{
            heads = maps:put(TreeId, Head, Heads),
            tails = maps:put(TreeId, Tail, Tails)
        }}
    end,
    Default = #document{
        key = MaskPtrId,
        value = #links_mask_root{
            heads = #{TreeId => Head},
            tails = #{TreeId => Tail}
        }
    },
    case datastore_doc:update(Ctx3, MaskPtrId, Diff, Default, Batch) of
        {{ok, _}, Batch2} -> {ok, Batch2};
        {{error, Reason}, Batch2} -> {{error, Reason}, Batch2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks document link in provided revision as deleted.
%% @end
%%--------------------------------------------------------------------
-spec mark_deleted(link_name(), link_rev(), mask()) ->
    {ok | {error, term()}, mask()}.
mark_deleted(LinkName, LinkRev, Mask = #mask{
    ctx = Ctx, key = Key, tree_id = TreeId, batch = Batch, tail = Tail
}) ->
    Size = application:get_env(cluster_worker, datastore_links_mask_size, 1000),
    Diff = fun(LinksMask = #links_mask{links = Links}) ->
        case length(Links) < Size of
            true ->
                {ok, LinksMask#links_mask{links = [{LinkName, LinkRev} | Links]}};
            false ->
                {ok, LinksMask#links_mask{next = datastore_utils:gen_key()}}
        end
    end,
    Default = #document{
        key = Tail,
        value = #links_mask{
            model = maps:get(model, Ctx),
            key = Key,
            tree_id = TreeId,
            links = [{LinkName, LinkRev}]
        }
    },
    case datastore_doc:update(Ctx, Tail, Diff, Default, Batch) of
        {{ok, #document{value = #links_mask{next = <<>>}}}, Batch2} ->
            {ok, Mask#mask{batch = Batch2}};
        {{ok, #document{value = #links_mask{next = Next}}}, Batch2} ->
            Default2 = Default#document{key = Next},
            case datastore_doc:save(Ctx, Next, Default2, Batch2) of
                {{ok, #document{}}, Batch3} ->
                    {ok, Mask#mask{
                        tail = Next,
                        batch = Batch3
                    }};
                {{error, Reason}, Batch3} ->
                    {{error, Reason}, Mask#mask{batch = Batch3}}
            end;
        {{error, Reason}, Batch2} ->
            {{error, Reason}, Mask#mask{batch = Batch2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Checks whether document link is deleted in provided revision.
%% @end
%%--------------------------------------------------------------------
-spec is_deleted(link_name(), link_rev(), cache()) -> boolean().
is_deleted(LinkName, LinkRev, Cache) ->
    gb_sets:is_member({LinkName, LinkRev}, Cache).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec load(key(), cache(), mask()) -> {{ok, cache()} | {error, term()}, mask()}.
load(Ptr, Cache, Mask = #mask{ctx = Ctx, batch = Batch}) ->
    case datastore_doc:fetch_deleted(Ctx, Ptr, Batch) of
        {{ok, #document{deleted = true, value = #links_mask{
            next = <<>>
        }}}, Batch2} ->
            Head = Tail = datastore_utils:gen_key(),
            {{ok, Cache}, Mask#mask{head = Head, tail = Tail, batch = Batch2}};
        {{ok, #document{deleted = true, value = #links_mask{
            next = Next
        }}}, Batch2} ->
            load(Next, Cache, Mask#mask{head = Next, batch = Batch2});
        {{ok, #document{value = #links_mask{
            links = Links,
            next = <<>>
        }}}, Batch2} ->
            Cache3 = lists:foldl(fun(Link, Cache2) ->
                gb_sets:add(Link, Cache2)
            end, Cache, Links),
            {{ok, Cache3}, Mask#mask{batch = Batch2}};
        {{ok, #document{value = #links_mask{
            links = Links,
            next = Next
        }}}, Batch2} ->
            Cache3 = lists:foldl(fun(Link, Cache2) ->
                gb_sets:add(Link, Cache2)
            end, Cache, Links),
            load(Next, Cache3, Mask#mask{batch = Batch2});
        {{error, not_found}, Batch2} ->
            {{ok, Cache}, Mask#mask{batch = Batch2}};
        {{error, Reason}, Batch2} ->
            {{error, Reason}, Mask#mask{batch = Batch2}}
    end.