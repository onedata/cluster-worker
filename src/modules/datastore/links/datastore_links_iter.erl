%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides navigation functionality for datastore links.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_links_iter).
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_links.hrl").
-include("modules/datastore/datastore_models.hrl").

%% API
-export([init/3, init/4, terminate/1]).
-export([get/2, fold/4, fold/6]).

-record(tree_it, {
    links = [] :: [link()],
    next_node_id = undefined :: undefined | links_node:id()
}).

-record(forest_it, {
    ctx :: ctx(),
    key :: key(),
    acc = [] :: [link()],
    heap = gb_trees:empty() :: gb_trees:tree({link_name(), tree_id()}, tree_it()),
    tree_ids = [] :: [tree_id()],
    masks_cache = #{} :: #{tree_id() => mask_cache()},
    batch :: batch()
}).

-type ctx() :: datastore_cache:ctx().
-type key() :: datastore:key().
-type mask() :: datastore_links_mask:mask().
-type mask_cache() :: datastore_links_mask:cache().
-type batch() :: undefined | datastore_doc_batch:batch().
-type tree_id() :: datastore_links:tree_id().
-type tree_ids() :: datastore_links:tree_ids().
-type forest() :: datastore_links:forest().
-type link() :: datastore_links:link().
-type link_name() :: datastore_links:link_name().
-type tree_it() :: #tree_it{}.
-opaque forest_it() :: #forest_it{}.
-type fold_fun() :: datastore:fold_fun(link()).
-type fold_acc() :: any().
-type fold_opt() :: {node_id, links_node:id()} |
                    {prev_tree_id, tree_id()} |
                    {prev_link_name, link_name()} |
                    {offset, non_neg_integer()} |
                    {size, non_neg_integer()}.
-type fold_opts() :: maps:map([fold_opt()]).

-export_type([forest_it/0, fold_fun/0, fold_acc/0, fold_opts/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv init(Ctx, Key, TreeIds, undefined)
%% @end
%%--------------------------------------------------------------------
-spec init(ctx(), key(), tree_ids()) ->
    {ok, forest_it()} | {{error, term()}, batch()}.
init(Ctx, Key, TreeIds) ->
    init(Ctx, Key, TreeIds, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Creates links forest tree iterator with provided trees or all trees in the
%% links forest. Uses datastore documents batch to cache fetched documents.
%% @end
%%--------------------------------------------------------------------
-spec init(ctx(), key(), tree_ids(), batch()) ->
    {ok | {error, term()}, forest_it()}.
init(Ctx, Key, all, Batch) ->
    case datastore_links:get_links_trees(Ctx, Key, Batch) of
        {{ok, TreeIds}, Batch2} ->
            init(Ctx, Key, TreeIds, Batch2);
        {{error, not_found}, Batch2} ->
            init(Ctx, Key, [], Batch2);
        {{error, Reason}, Batch2} ->
            {{error, Reason}, #forest_it{
                ctx = Ctx,
                key = Key,
                batch = Batch2
            }}
    end;
init(Ctx, Key, TreeIds, Batch) when is_list(TreeIds) ->
    ForestIt = #forest_it{
        ctx = Ctx,
        key = Key,
        tree_ids = TreeIds,
        batch = Batch
    },
    lists:foldl(fun
        (TreeId, {ok, ForestIt2 = #forest_it{}}) ->
            init_tree_mask(Ctx, Key, TreeId, Batch, ForestIt2);
        (_, {{error, Reason}, ForestIt2}) ->
            {{error, Reason}, ForestIt2}
    end, {ok, ForestIt}, TreeIds);
init(Ctx, Key, TreeId, Batch) ->
    init(Ctx, Key, [TreeId], Batch).

%%--------------------------------------------------------------------
%% @doc
%% Returns documents documents batch.
%% @end
%%--------------------------------------------------------------------
-spec terminate(forest_it()) -> batch().
terminate(#forest_it{batch = Batch}) ->
    Batch.

%%--------------------------------------------------------------------
%% @doc
%% Returns document links by name in link tree forest.
%% @end
%%--------------------------------------------------------------------
-spec get(link_name(), forest_it()) ->
    {{ok, [link()]} | {error, term()}, forest_it()}.
get(LinkName, ForestIt = #forest_it{tree_ids = TreeIds}) ->
    Result = lists:foldl(fun
        (TreeId, {ok, ForestIt2}) ->
            get_tree(LinkName, TreeId, ForestIt2);
        (_, {{error, Reason}, ForestIt2}) ->
            {{error, Reason}, ForestIt2}
    end, {ok, ForestIt}, TreeIds),
    case Result of
        {ok, ForestIt3 = #forest_it{acc = []}} ->
            {{error, not_found}, ForestIt3};
        {ok, ForestIt3 = #forest_it{acc = Acc}} ->
            {{ok, Acc}, ForestIt3#forest_it{acc = []}};
        {{error, Reason}, ForestIt3} ->
            {{error, Reason}, ForestIt3#forest_it{acc = []}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Calls `Fun(TreeId, LinkName, LinkTarget, Acc)' for each link in a link tree
%% forest in increasing order of link names.
%% @end
%%--------------------------------------------------------------------
-spec fold(fold_fun(), fold_acc(), forest_it(), fold_opts()) ->
    {{ok, fold_acc()} | {error, term()}, forest_it()}.
fold(Fun, Acc, ForestIt, Opts) ->
    case fold_forest_init(ForestIt, Opts) of
        {ok, ForestIt2} -> fold_forest_step(Fun, Acc, ForestIt2, Opts);
        {{error, Reason}, ForestIt2} -> {{error, Reason}, ForestIt2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates links forest tree iterator and calls {@link fold/4}.
%% @end
%%--------------------------------------------------------------------
-spec fold(ctx(), key(), all | tree_id(), fold_fun(), fold_acc(),
    fold_opts()) -> {ok, fold_acc()} | {error, term()}.
fold(Ctx, Key, TreeId, Fun, Acc, Opts) ->
    case init(Ctx, Key, TreeId, datastore_doc_batch:init()) of
        {ok, ForestIt} ->
            {Result, _} = fold(Fun, Acc, ForestIt, Opts),
            Result;
        {{error, Reason}, _Batch} ->
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes links tree mask.
%% @end
%%--------------------------------------------------------------------
-spec init_tree_mask(ctx(), key(), tree_id(), batch(), forest_it()) ->
    {ok | {error, term()}, forest_it()}.
init_tree_mask(Ctx, Key, TreeId, Batch, ForestIt) ->
    case datastore_links_mask:init(Ctx, Key, TreeId, Batch) of
        {ok, Mask} ->
            init_tree_mask_cache(TreeId, Mask, ForestIt);
        {{error, Reason}, Mask} ->
            {_, Batch} = datastore_links_mask:terminate(Mask),
            {{error, Reason}, ForestIt#forest_it{batch = Batch}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes links tree mask cache.
%% @end
%%--------------------------------------------------------------------
-spec init_tree_mask_cache(tree_id(), mask(), forest_it()) ->
    {ok | {error, term()}, forest_it()}.
init_tree_mask_cache(TreeId, Mask, ForestIt = #forest_it{
    masks_cache = MasksCache
}) ->
    case datastore_links_mask:load(Mask) of
        {{ok, Cache}, Mask2} ->
            case datastore_links_mask:terminate(Mask2) of
                {ok, Batch} ->
                    {ok, ForestIt#forest_it{
                        masks_cache = maps:put(TreeId, Cache, MasksCache),
                        batch = Batch
                    }};
                {{error, Reason}, Batch} ->
                    {{error, Reason}, ForestIt#forest_it{batch = Batch}}
            end;
        {{error, Reason}, Mask2} ->
            {_, Batch} = datastore_links_mask:terminate(Mask2),
            {{error, Reason}, ForestIt#forest_it{batch = Batch}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes link tree and returns document links by name.
%% @end
%%--------------------------------------------------------------------
-spec get_tree(link_name(), tree_id(), forest_it()) ->
    {ok | {error, term()}, forest_it()}.
get_tree(LinkName, TreeId, ForestIt = #forest_it{
    ctx = Ctx, key = Key, acc = Acc, masks_cache = MasksCache, batch = Batch
}) ->
    Cache = maps:get(TreeId, MasksCache),
    {ok, Tree} = datastore_links:init_tree(Ctx, Key, TreeId, Batch),
    case datastore_links_crud:get(LinkName, Tree) of
        {{ok, Link}, Tree2} ->
            ForestIt2 = ForestIt#forest_it{
                batch = datastore_links:terminate_tree(Tree2)
            },
            case is_deleted(Link, Cache) of
                true -> {ok, ForestIt2};
                false -> {ok, ForestIt2#forest_it{acc = [Link | Acc]}}
            end;
        {{error, not_found}, Tree3} ->
            {ok, ForestIt#forest_it{
                batch = datastore_links:terminate_tree(Tree3)
            }};
        {{error, Reason}, Tree3} ->
            {{error, Reason}, ForestIt#forest_it{
                batch = datastore_links:terminate_tree(Tree3)
            }}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes link forest fold state.
%% @end
%%--------------------------------------------------------------------
-spec fold_forest_init(forest(), fold_opts()) ->
    {ok | {error, term()}, forest_it()}.
fold_forest_init(ForestIt = #forest_it{tree_ids = TreeIds}, Opts) ->
    lists:foldl(fun
        (TreeId, {ok, ForestIt2}) ->
            case fold_tree_init(TreeId, ForestIt2, Opts) of
                {{ok, TreeIt}, ForestIt3} ->
                    {ok, add_tree_it(TreeIt, ForestIt3)};
                {{error, Reason}, ForestIt3} ->
                    {{error, Reason}, ForestIt3}
            end;
        (_, {{error, Reason}, ForestIt2}) ->
            {{error, Reason}, ForestIt2}
    end, {ok, ForestIt}, TreeIds).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes link tree fold state.
%% @end
%%--------------------------------------------------------------------
-spec fold_tree_init(tree_id(), forest_it(), fold_opts()) ->
    {{ok, tree_it()} | {error, term()}, forest_it()}.
fold_tree_init(TreeId, ForestIt = #forest_it{
    ctx = Ctx, key = Key, masks_cache = MasksCache, batch = Batch
}, Opts) ->
    Cache = maps:get(TreeId, MasksCache),
    FoldInit = get_fold_tree_init_arg(TreeId, Opts),
    Fun = fun(Name, {Target, Rev}, Acc) ->
        [#link{tree_id = TreeId, name = Name, target = Target, rev = Rev} | Acc]
    end,
    {ok, Tree} = datastore_links:init_tree(Ctx, Key, TreeId, Batch),
    {Result, Tree3} = case bp_tree:fold(FoldInit, Fun, [], Tree) of
        {{ok, {Links, NodeId}}, Tree2} ->
            {{ok, #tree_it{
                links = filter_deleted(lists:reverse(Links), Cache),
                next_node_id = NodeId
            }}, Tree2};
        {{error, not_found}, Tree2} ->
            {{ok, #tree_it{}}, Tree2};
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end,
    {Result, ForestIt#forest_it{batch = datastore_links:terminate_tree(Tree3)}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Selects tree that contains next smallest link name and processes the link.
%% @end
%%--------------------------------------------------------------------
-spec fold_forest_step(fold_fun(), fold_acc(), forest_it(), fold_opts()) ->
    {{ok, fold_acc()} | {error, term()}, forest_it()}.
fold_forest_step(_Fun, Acc, ForestIt, #{size := 0}) ->
    {{ok, Acc}, ForestIt};
fold_forest_step(Fun, Acc, ForestIt, Opts) ->
    case get_next_tree_it(ForestIt) of
        {{ok, Links}, ForestIt2} ->
            case fold_tree_step(Links, ForestIt2) of
                {{ok, Link}, ForestIt3} ->
                    case process_link(Fun, Acc, Link, Opts) of
                        {ok, {Acc2, Opts2}} ->
                            fold_forest_step(Fun, Acc2, ForestIt3, Opts2);
                        {error, Reason} ->
                            {{error, Reason}, ForestIt3}
                    end;
                {{error, Reason}, ForestIt3} ->
                    {{error, Reason}, ForestIt3}
            end;
        {{error, not_found}, ForestIt2} ->
            {{ok, Acc}, ForestIt2}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes smallest link in a tree. If it it the last link in links tree
%% accumulator tries to load next ones.
%% @end
%%--------------------------------------------------------------------
-spec fold_tree_step(tree_it(), forest_it()) ->
    {{ok, link()} | {error, term()}, forest_it()}.
fold_tree_step(#tree_it{links = [Link], next_node_id = undefined}, ForestIt) ->
    {{ok, Link}, ForestIt};
fold_tree_step(#tree_it{
    links = [Link = #link{tree_id = TreeId, name = Name}],
    next_node_id = NodeId
}, ForestIt) ->
    Opts = #{prev_tree_id => TreeId, prev_link_name => Name, node_id => NodeId},
    case fold_tree_init(TreeId, ForestIt, Opts) of
        {{ok, TreeIt}, ForestIt3} ->
            {{ok, Link}, add_tree_it(TreeIt, ForestIt3)};
        {{error, Reason}, ForestIt3} ->
            {{error, Reason}, ForestIt3}
    end;
fold_tree_step(TreeIt = #tree_it{links = [Link | Links]}, ForestIt) ->
    {{ok, Link}, add_tree_it(TreeIt#tree_it{links = Links}, ForestIt)}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Processes a link by calling `Fun(TreeId, LinkName, LinkTarget, Acc)'
%% function and updates options accordingly.
%% @end
%%--------------------------------------------------------------------
-spec process_link(fold_fun(), fold_acc(), link(), fold_opts()) ->
    {ok, {fold_acc(), fold_opts()}} | {error, term()}.
process_link(_, Acc, #link{tree_id = TreeId, name = Name}, Opts = #{
    prev_tree_id := PrevTreeId,
    prev_link_name := PrevName
}) when TreeId =< PrevTreeId andalso Name =< PrevName ->
    {ok, {Acc, Opts}};
process_link(Fun, Acc, Link = #link{tree_id = TreeId, name = Name}, Opts = #{
    offset := Offset
}) when Offset > 0 ->
    Opts2 = Opts#{
        prev_tree_id => TreeId,
        prev_link_name => Name
    },
    case Fun(Link, Acc) of
        {ok, Acc} -> {ok, {Acc, Opts2}};
        {ok, _} -> {ok, {Acc, Opts2#{offset => Offset - 1}}};
        {{stop, Acc2}, _} -> {ok, {Acc2, Opts2#{offset => 0, size => 0}}};
        {{error, Reason}, _} -> {error, Reason}
    end;
process_link(Fun, Acc, Link = #link{tree_id = TreeId, name = Name}, Opts) ->
    Opts2 = Opts#{
        prev_tree_id => TreeId,
        prev_link_name => Name
    },
    case {Fun(Link, Acc), maps:get(size, Opts, undefined)} of
        {{ok, Acc2}, undefined} -> {ok, {Acc2, Opts2}};
        {{ok, Acc2}, Size} -> {ok, {Acc2, Opts2#{size => Size - 1}}};
        {{stop, Acc2}, _} -> {ok, {Acc2, Opts2#{size => 0}}};
        {{error, Reason}, _} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Saves links tree accumulator in forest iterator heap if it is not empty.
%% @end
%%--------------------------------------------------------------------
-spec add_tree_it(tree_it(), forest_it()) -> forest_it().
add_tree_it(#tree_it{links = []}, ForestIt) ->
    ForestIt;
add_tree_it(TreeIt = #tree_it{links = [Link | _]}, ForestIt = #forest_it{
    heap = Heap
}) ->
    #link{tree_id = TreeId, name = Name} = Link,
    ForestIt#forest_it{
        heap = gb_trees:insert({Name, TreeId}, TreeIt, Heap)
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns link tree accumulator which has next smallest link name.
%% @end
%%--------------------------------------------------------------------
-spec get_next_tree_it(forest_it()) ->
    {{ok, tree_it()} | {error, term()}, forest_it()}.
get_next_tree_it(ForestIt = #forest_it{heap = Heap}) ->
    case gb_trees:is_empty(Heap) of
        true ->
            {{error, not_found}, ForestIt};
        false ->
            {_, TreeIt, Heap2} = gb_trees:take_smallest(Heap),
            {{ok, TreeIt}, ForestIt#forest_it{heap = Heap2}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns link tree fold init argument.
%% @end
%%--------------------------------------------------------------------
-spec get_fold_tree_init_arg(tree_id(), fold_opts()) -> bp_tree:fold_init().
get_fold_tree_init_arg(_TreeId, #{node_id := NodeId}) ->
    {node_id, NodeId};
get_fold_tree_init_arg(TreeId, #{
    prev_tree_id := PrevTreeId,
    prev_link_name := Name
}) when TreeId =< PrevTreeId ->
    {prev_key, Name};
get_fold_tree_init_arg(_TreeId, #{prev_link_name := Name}) ->
    {start_key, Name};
get_fold_tree_init_arg(_TreeId, _Opts) ->
    {offset, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks whether link is marked as deleted.
%% @end
%%--------------------------------------------------------------------
-spec is_deleted(link(), mask_cache()) -> boolean().
is_deleted(#link{name = Name, rev = Rev}, Cache) ->
    datastore_links_mask:is_deleted(Name, Rev, Cache).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Skips all links that are marked as deleted.
%% @end
%%--------------------------------------------------------------------
-spec filter_deleted([link()], mask_cache()) -> [link()].
filter_deleted(Links, Cache) ->
    lists:filter(fun(Link = #link{}) -> not is_deleted(Link, Cache) end, Links).