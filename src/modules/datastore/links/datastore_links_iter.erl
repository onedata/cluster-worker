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
-include_lib("ctool/include/logging.hrl").

%% API
-export([init/3, init/4, terminate/1]).
-export([get/2, fold/4, fold/6, fold/7]).

-record(tree_it, {
    links = [] :: [link()],
    next_node_id = undefined :: undefined | links_node:id()
}).

-record(forest_it, {
    ctx :: ctx(),
    key :: key(),
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
-type link() :: datastore_links:link().
-type link_name() :: datastore_links:link_name().
-type tree_it() :: #tree_it{}.
-opaque forest_it() :: #forest_it{}.
-type fold_fun() :: datastore:fold_fun(link()).
-type fold_acc() :: any().
-type fold_opts() :: #{
    prev_link_name => link_name(),
    offset => integer(),
    size => non_neg_integer(),
    token => token(),
    % Datastore internal options
    node_id => links_node:id(),
    prev_tree_id => tree_id(), % Warning - link must exist with this opt!
    node_prev_to_key => link_name() % To walk back with neg offset
}.
-type token() :: #link_token{}.

-export_type([forest_it/0, fold_fun/0, fold_acc/0, fold_opts/0, token/0]).

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
    MasterPid = datastore_cache_writer:get_master_pid(),
    Results = utils:pmap(fun(TreeId) ->
        datastore_cache_writer:save_master_pid(MasterPid),
        get_from_tree(LinkName, TreeId, ForestIt)
    end, TreeIds),
    Result = lists:foldl(fun
        (_, {error, Reason}) -> {error, Reason};
        ({error, not_found}, {ok, Acc}) -> {ok, Acc};
        % Next 2 error can appear for bp_trees when document cannot be found in memory
        % Throw error to allow retry in tp process that can read document from db
        ({error, {fetch_error, not_found}}, _) -> throw(not_found);
        ({error, {{fetch_error, not_found}, _Stacktrace}}, _) -> throw(not_found);
        ({error, Reason}, _) -> {error, Reason};
        ({{error, Reason}, _Stacktrace}, _) -> {error, Reason};
        ({ok, Link}, {ok, Acc}) -> {ok, [Link | Acc]}
    end, {ok, []}, Results),
    case Result of
        {ok, []} -> {{error, not_found}, ForestIt};
        {ok, Acc} -> {{ok, Acc}, ForestIt};
        {error, Reason} -> {{error, Reason}, ForestIt}
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
    case init_forest_fold(ForestIt, Opts) of
        {ok, ForestIt2} -> step_forest_fold(Fun, Acc, ForestIt2, Opts);
        {{error, Reason}, ForestIt2} -> {{error, Reason}, ForestIt2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Calls {@link fold/6} and deletes forest iterator from answer.
%% @end
%%--------------------------------------------------------------------
-spec fold(ctx(), key(), all | tree_id(), fold_fun(), fold_acc(),
    fold_opts()) -> Result | {Result, token()}
    when Result :: {ok, fold_acc()} | {error, term()}.
fold(Ctx, Key, TreeId, Fun, Acc, Opts) ->
    {Result, _ForestIt} = fold(Ctx, Key, TreeId, Fun, Acc, Opts,
        datastore_doc_batch:init()),
    Result.

%%--------------------------------------------------------------------
%% @doc
%% Creates links forest tree iterator and calls {@link fold/4}.
%% @end
%%--------------------------------------------------------------------
-spec fold(ctx(), key(), all | tree_id(), fold_fun(), fold_acc(),
    fold_opts(), datastore_doc_batch:batch()) ->
    {Result | {Result, token()}, forest_it()}
    when Result :: {ok, fold_acc()} | {error, term()}.
fold(Ctx, Key, TreeId, Fun, Acc, #{token := Token} = Opts, InitBatch)
    when Token#link_token.restart_token =/= undefined ->
    ForestIt = Token#link_token.restart_token#forest_it{batch = InitBatch},
    {Ans, ForestIt2} = step_forest_fold(Fun, Acc, ForestIt,
        maps:remove(offset, Opts)),

    case Ans of
        {ok, _} ->
            IsLast = gb_trees:is_empty(ForestIt2#forest_it.heap),
            {{Ans, #link_token{
                restart_token = ForestIt2#forest_it{batch = undefined},
                is_last = IsLast}}, ForestIt2};
        Error ->
            ?warning("Cannot fold links for args ~p by token: ~p",
                [{Ctx, Key, TreeId, Opts}, Error]),
            fold(Ctx, Key, TreeId, Fun, Acc, Opts#{token := #link_token{}}, InitBatch)
    end;
fold(Ctx, Key, TreeId, Fun, Acc, Opts, InitBatch) ->
    case init(Ctx, Key, TreeId, InitBatch) of
        {ok, ForestIt} ->
            {Result, ForestIt2} = Ans = fold(Fun, Acc, ForestIt, Opts),
            case maps:get(token, Opts, undefined) of
                undefined ->
                    Ans;
                _ ->
                    IsLast = gb_trees:is_empty(ForestIt2#forest_it.heap),
                    Token = #link_token{
                        restart_token = ForestIt2#forest_it{batch = undefined},
                        is_last = IsLast},
                    {{Result, Token}, ForestIt2}
            end;
        Other ->
            Other
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
        {ok, Mask, Empty} ->
            init_tree_mask_cache(TreeId, Mask, ForestIt, Empty);
        {{error, Reason}, Mask, _} ->
            {_, Batch} = datastore_links_mask:terminate_read_only_mask(Mask),
            {{error, Reason}, ForestIt#forest_it{batch = Batch}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes links tree mask cache.
%% @end
%%--------------------------------------------------------------------
-spec init_tree_mask_cache(tree_id(), mask(), forest_it(), boolean()) ->
    {ok | {error, term()}, forest_it()}.
init_tree_mask_cache(TreeId, Mask, ForestIt = #forest_it{
    masks_cache = MasksCache
}, Empty) ->
    case datastore_links_mask:load(Mask, Empty) of
        {{ok, Cache}, Mask2} ->
            {ok, Batch} = datastore_links_mask:terminate_read_only_mask(Mask2),
            {ok, ForestIt#forest_it{
                masks_cache = maps:put(TreeId, Cache, MasksCache),
                batch = Batch
            }};
        {{error, Reason}, Mask2} ->
            {_, Batch} = datastore_links_mask:terminate_read_only_mask(Mask2),
            {{error, Reason}, ForestIt#forest_it{batch = Batch}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes link tree and returns document links by name.
%% @end
%%--------------------------------------------------------------------
-spec get_from_tree(link_name(), tree_id(), forest_it()) ->
    {ok, link()} | {error, term()}.
get_from_tree(LinkName, TreeId, #forest_it{
    ctx = Ctx, key = Key, masks_cache = MasksCache, batch = Batch
}) ->
    Cache = maps:get(TreeId, MasksCache),
    case datastore_links:init_tree(Ctx, Key, TreeId, Batch, true) of
        {ok, Tree} ->
            case datastore_links_crud:get(LinkName, Tree) of
                {{ok, Link}, Tree2} ->
                    datastore_links:terminate_tree(Tree2),
                    case is_deleted(Link, Cache) of
                        true -> {error, not_found};
                        false -> {ok, Link}
                    end;
                {{error, Reason}, Tree2} ->
                    datastore_links:terminate_tree(Tree2),
                    {error, Reason}
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes link forest fold state.
%% @end
%%--------------------------------------------------------------------
-spec init_forest_fold(forest_it(), fold_opts()) ->
    {ok | {error, term()}, forest_it()}.
init_forest_fold(ForestIt = #forest_it{tree_ids = TreeIds}, Opts) ->
    Ans = lists:foldl(fun
        (TreeId, {ok, ForestIt2}) ->
            case init_tree_fold(TreeId, ForestIt2, Opts) of
                {{ok, TreeIt}, ForestIt3} ->
                    {ok, add_tree_it(TreeIt, ForestIt3)};
                {{error, Reason}, ForestIt3} ->
                    {{error, Reason}, ForestIt3}
            end;
        (_, {{error, Reason}, ForestIt2}) ->
            {{error, Reason}, ForestIt2}
    end, {ok, ForestIt}, TreeIds),

    add_prev_fold_nodes(Ans, Opts, [], []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds previous nodes to forest_it (used during listing with neg offset).
%% @end
%%--------------------------------------------------------------------
-spec add_prev_fold_nodes({ok | {error, term()}, forest_it()}, fold_opts(), list(), list()) ->
    {ok | {error, term()}, forest_it()}.
add_prev_fold_nodes({ok, #forest_it{heap = Heap, tree_ids = TreeIds} = ForestIt} = Ans,
    #{offset := Offset, prev_link_name := PrevLinkName} = Opts, EmptyTrees, ForceContinue) when Offset < 0 ->
    HeapList = gb_trees:to_list(Heap),
    Keys = lists:foldl(fun({_, #tree_it{links = Links}}, Acc) ->
        Acc ++ lists:map(fun(#link{name = Name}) -> Name end, Links)
    end, [], HeapList),
    Keys2 = lists:sort(Keys),

    Continue = case ForceContinue of
        [] ->
            OffsetAbs = abs(Offset),
            case length(Keys) > OffsetAbs of
                true ->
                    {lists:nth(OffsetAbs, Keys2) >= PrevLinkName, TreeIds -- EmptyTrees};
                _ ->
                    {true, TreeIds -- EmptyTrees}
            end;
        _ ->
            {true, ForceContinue}
    end,

    case Continue of
        {true, FoldIds} when FoldIds =/= [] ->
            Ans2 = lists:foldl(fun
                ({{Name, TreeId} = ItKey, #tree_it{links = Links} = TreeIt},
                    {ok, #forest_it{heap = TmpHeap} = ForestIt2, TmpEmptyTrees}) ->
                    case lists:member(TreeId, FoldIds) of
                        false ->
                            ForestIt3 = ForestIt2#forest_it{
                                heap = gb_trees:insert(ItKey, TreeIt, TmpHeap)
                            },

                            {ok, ForestIt3, TmpEmptyTrees};
                        _ ->
                            case init_tree_fold(TreeId, ForestIt2, #{node_prev_to_key => Name}) of
                                {{ok, #tree_it{links = [#link{name = FirstName} | _] = Links2}}, ForestIt3} ->
                                    ForestIt4 = ForestIt3#forest_it{
                                        heap = gb_trees:insert({FirstName, TreeId},
                                            TreeIt#tree_it{links = Links2 ++ Links}, TmpHeap)
                                    },

                                    {ok, ForestIt4, TmpEmptyTrees};
                                {{error, first_node}, ForestIt3} ->
                                    ForestIt4 = ForestIt3#forest_it{
                                        heap = gb_trees:insert(ItKey, TreeIt, TmpHeap)
                                    },

                                    {ok, ForestIt4, [TreeId | TmpEmptyTrees]};
                                {{error, Reason}, ForestIt3} ->
                                    {{error, Reason}, ForestIt3}
                            end
                    end;
                (_, {{error, Reason}, ForestIt2}) ->
                    {{error, Reason}, ForestIt2}
            end, {ok, ForestIt#forest_it{heap = gb_trees:empty()}, EmptyTrees}, HeapList),

            add_prev_fold_nodes(Ans2, Opts, EmptyTrees, []);
        _ ->
            SmallerKeys = lists:takewhile(fun(Key) -> Key < PrevLinkName end, Keys2),
            FirstIncluded = lists:nth(max(length(SmallerKeys) + Offset + 1, 1), Keys2),

            FoldAns = lists:foldl(fun({{_, ItTree}, #tree_it{links = [First | _] = Links} = TreeIt},
                {#forest_it{heap = TmpHeap} = ForestIt2, ContinueList}) ->
                FilteredLinks = lists:dropwhile(fun(#link{name = Key}) ->
                    Key < FirstIncluded end, Links),
                case FilteredLinks of
                    [#link{name = ItName} = First | _] ->
                        case lists:member(ItTree, EmptyTrees) of
                            true ->
                                TreeIt2 = TreeIt#tree_it{links = FilteredLinks},
                                {ForestIt2#forest_it{
                                    heap = gb_trees:insert({ItName, ItTree}, TreeIt2, TmpHeap)
                                }, ContinueList};
                            _ ->
                                {ForestIt2, [ItTree | ContinueList]}
                        end;
                    [#link{name = ItName} | _] ->
                        TreeIt2 = TreeIt#tree_it{links = FilteredLinks},
                        {ForestIt2#forest_it{
                            heap = gb_trees:insert({ItName, ItTree}, TreeIt2, TmpHeap)
                        }, ContinueList};
                    _ ->
                        {ForestIt2, ContinueList}
                end
            end, {ForestIt#forest_it{heap = gb_trees:empty()}, []}, HeapList),
            case FoldAns of
                {FinalAns, []} ->
                    {ok, FinalAns};
                {_, ContinueIds} ->
                    add_prev_fold_nodes(Ans, Opts, EmptyTrees, ContinueIds)
            end
    end;
add_prev_fold_nodes({ok, ForestIt, NewEmptyTrees}, Opts, _EmptyTrees, ForceContinue) ->
    add_prev_fold_nodes({ok, ForestIt}, Opts, NewEmptyTrees, ForceContinue);
add_prev_fold_nodes(Ans, _, _, _) ->
    Ans.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes link tree fold state.
%% @end
%%--------------------------------------------------------------------
-spec init_tree_fold(tree_id(), forest_it(), fold_opts()) ->
    {{ok, tree_it()} | {error, term()}, forest_it()}.
init_tree_fold(TreeId, ForestIt = #forest_it{
    ctx = Ctx, key = Key, masks_cache = MasksCache, batch = Batch
}, Opts) ->
    Cache = maps:get(TreeId, MasksCache),
    FoldInit = get_fold_tree_init_arg(TreeId, Opts),
    Fun = fun(Name, {Target, Rev}, Acc) ->
        [#link{tree_id = TreeId, name = Name, target = Target, rev = Rev} | Acc]
    end,
    case datastore_links:init_tree(Ctx, Key, TreeId, Batch, true) of
        {ok, Tree} ->
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
            {Result, ForestIt#forest_it{batch = datastore_links:terminate_tree(Tree3)}};
        Error ->
            {Error, ForestIt}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Selects tree that contains next smallest link name and processes the link.
%% @end
%%--------------------------------------------------------------------
-spec step_forest_fold(fold_fun(), fold_acc(), forest_it(), fold_opts()) ->
    {{ok, fold_acc()} | {error, term()}, forest_it()}.
step_forest_fold(_Fun, Acc, ForestIt, #{size := 0}) ->
    {{ok, Acc}, ForestIt};
step_forest_fold(Fun, Acc, ForestIt, Opts) ->
    case get_next_tree_it(ForestIt) of
        {{ok, Links}, ForestIt2} ->
            case step_tree_fold(Links, ForestIt2) of
                {{ok, Link}, ForestIt3} ->
                    case process_link(Fun, Acc, Link, Opts) of
                        {ok, {Acc2, Opts2}} ->
                            step_forest_fold(Fun, Acc2, ForestIt3, Opts2);
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
%% Processes next smallest link in a tree in the alphabetical order.
%% If it it the last link in links tree accumulator tries to load next ones.
%% @end
%%--------------------------------------------------------------------
-spec step_tree_fold(tree_it(), forest_it()) ->
    {{ok, link()} | {error, term()}, forest_it()}.
step_tree_fold(#tree_it{links = [Link], next_node_id = undefined}, ForestIt) ->
    {{ok, Link}, ForestIt};
step_tree_fold(#tree_it{
    links = [Link = #link{tree_id = TreeId, name = Name}],
    next_node_id = NodeId
}, ForestIt) ->
    Opts = #{prev_tree_id => TreeId, prev_link_name => Name, node_id => NodeId},
    case init_tree_fold(TreeId, ForestIt, Opts) of
        {{ok, TreeIt}, ForestIt3} ->
            {{ok, Link}, add_tree_it(TreeIt, ForestIt3)};
        {{error, Reason}, ForestIt3} ->
            {{error, Reason}, ForestIt3}
    end;
step_tree_fold(TreeIt = #tree_it{links = [Link | Links]}, ForestIt) ->
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
get_fold_tree_init_arg(_TreeId, #{prev_link_name := Name, offset := Offset})
    when Offset < 0 ->
    {node_of_key, Name};
get_fold_tree_init_arg(_TreeId, #{prev_link_name := Name}) ->
    {start_key, Name};
get_fold_tree_init_arg(_TreeId, #{node_prev_to_key := Name}) ->
    {node_prev_to_key, Name};
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