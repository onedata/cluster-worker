%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Utility functions for links' management at driver level.
%%% @end
%%%-------------------------------------------------------------------
-module(links_utils).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% Prefix of VHash value (version hash used to tag links) in link name
-define(VHASH_PREFIX, "__VH__").

-type scope() :: undefined | binary().
-type vhash() :: binary() | undefined | {deleted, binary()}.
% mapping to mother scope - function or key in process dict
-type mother_scope() :: fun((datastore:key()) -> scope()) | atom().
% mapping to other scopes - function or key in process dict
-type other_scopes() :: fun((datastore:key()) -> [scope()]) | atom().
-export_type([scope/0, mother_scope/0, other_scopes/0, vhash/0]).

%% API
-export([create_link_in_map/4, save_links_maps/5, delete_links/3, delete_links_from_maps/4,
    fetch_link/4, foreach_link/5, links_doc_key/2, diff/2]).
-export([make_scoped_link_name/4, unpack_link_scope/2, select_scope_related_link/4]).
-export([get_context_to_propagate/1, apply_context/1, get_scopes/2, gen_vhash/0, deduplicate_targets/1]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Generates unique link stamp.
%% @end
%%--------------------------------------------------------------------
-spec gen_vhash() -> vhash().
gen_vhash() ->
    B = integer_to_binary(erlang:system_time(), 16),
    E = http_utils:base64url_encode(crypto:strong_rand_bytes(10)),
    <<?VHASH_PREFIX, B/binary, E/binary>>.


%%--------------------------------------------------------------------
%% @doc
%% Removes duplicated links from given targets list. Requires sorted list.
%% @end
%%--------------------------------------------------------------------
-spec deduplicate_targets([datastore:link_final_target()]) ->
    [datastore:link_final_target()].
deduplicate_targets([T]) ->
    [T];
deduplicate_targets([]) ->
    [];
deduplicate_targets([{S, _, M, K}, {S, _, M, K} = T | R]) ->
    deduplicate_targets([T | R]);
deduplicate_targets([T | R]) ->
    [T | deduplicate_targets(R)].


%%--------------------------------------------------------------------
%% @doc
%% Generates diff between two link records. Returns links that were added or deleted in second records.
%% @end
%%--------------------------------------------------------------------
-spec diff(#links{} | #{}, #links{} | #{}) -> {#{}, #{}}.
diff(#links{link_map = OldMap}, #links{link_map = CurrentMap}) ->
    diff(OldMap, CurrentMap);
diff(OldMap, CurrentMap) ->
    OldList = lists:sort(maps:to_list(OldMap)),
    NewList = lists:sort(maps:to_list(CurrentMap)),

    {Added, Deleted} = diff2(OldList, NewList, [], []),

    {maps:from_list(Added), maps:from_list(Deleted)}.

-spec diff2(OldLinks :: [Link], NewLinks :: [Link], LinkAcc, LinkAcc) ->
    {LinkAcc, LinkAcc} when
    Link :: {datastore:link_name(), datastore:link_final_target()},
    LinkAcc :: [Link].
diff2([], NT, Added, Deleted) ->
%% No old links, assume that everything else is added as new
    {NT ++ Added, Deleted};
diff2(OT, [], Added, Deleted) ->
%% No new links, assume that everything else has been removed
    {Added, OT ++ Deleted};
diff2([{Key, {_, Targets}} | OT], [{Key, {_, Targets}} | NT], Added, Deleted) ->
%% Same link name and targets, nothing has changed
    diff2(OT, NT, Added, Deleted);
diff2([{Key, {_, TargetsO}} | OT], [{Key, {V, TargetsN}} | NT], Added, Deleted) ->
%% Same link name, different targets -> diff targets
    AddedTargets = TargetsN -- TargetsO,
    DeletedTargets = TargetsO -- TargetsN,
    NewAdded = case AddedTargets of
        [] -> Added;
        _ -> [{Key, {V, AddedTargets}} | Added]
    end,
    NewDeleted = case DeletedTargets of
        [] -> Deleted;
        _ -> [{Key, {V, DeletedTargets}} | Deleted]
    end,
    diff2(OT, NT, NewAdded, NewDeleted);
diff2([{KeyO, _} = OldLink | OT], [{KeyN, _} = NewLink | NT], Added, Deleted) when KeyO < KeyN ->
%% Old link name does not exist in new links' map -> assume it has been deleted
    diff2(OT, [NewLink | NT], Added, [OldLink | Deleted]);
diff2([{KeyO, _} = OldLink | OT], [{KeyN, _} = NewLink | NT], Added, Deleted) when KeyO > KeyN ->
%% New link name does not exist in old links' map -> assume it has been created
    diff2([OldLink | OT], NT, [NewLink | Added], Deleted).


%%--------------------------------------------------------------------
%% @doc
%% Creates link. Before creation it checks if it exists in several documents.
%% @end
%%--------------------------------------------------------------------
-spec create_link_in_map(Driver :: atom(), model_behaviour:model_config(), datastore:ext_key(),
    datastore:normalized_link_spec()) -> ok | datastore:create_error().
create_link_in_map(Driver, #model_config{mother_link_scope = Scope1, other_link_scopes = _Scope2} = ModelConfig,
    Key, {LinkName, _} = Link) ->
    LocalScope = get_scopes(?LOCAL_ONLY_LINK_SCOPE, Key),
    RequestedScope = get_scopes(Scope1, Key),
    case fetch_link(Driver, ModelConfig, LinkName, Key, LocalScope) of
        {error, link_not_found} ->
            case create_link_in_map(Driver, ModelConfig, Link, Key, links_doc_key(Key, LocalScope), 1) of
                ok when LocalScope =/= RequestedScope ->
                    create_link_in_map(Driver, ModelConfig, Link, Key, links_doc_key(Key, RequestedScope), 1);
                Other0 ->
                    Other0
            end;
        {ok, _} ->
            {error, already_exists};
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Creates link. Before creation it checks if it exists in several documents.
%% @end
%%--------------------------------------------------------------------
-spec create_link_in_map(Driver :: atom(), model_behaviour:model_config(),
    datastore:normalized_link_spec(), Key :: datastore:ext_key(), LinkKey :: datastore:ext_key(),
    KeyNum :: integer()) -> ok | datastore:create_error().
create_link_in_map(Driver, #model_config{bucket = _Bucket} = ModelConfig, {LinkName, _} = Link, Key, LinkKey, KeyNum) ->
    case Driver:get_link_doc(ModelConfig, LinkKey) of
        {ok, #document{value = #links{link_map = LinkMap, children = Children}} = Doc} ->
            DoAdd =
                fun() ->
                    LinkNum = get_link_child_num(LinkName, KeyNum),
                    NextKey = maps:get(LinkNum, Children, <<"non">>),
                    case NextKey of
                        % TODO use atom instead of binary (performance reasons)
                        <<"non">> ->
                            add_non_existing_to_map(Driver, ModelConfig, Key, LinkKey, Doc, Link, KeyNum);
                        _ ->
                            case create_link_in_map(Driver, ModelConfig, Link, Key, NextKey, KeyNum + 1) of
                                ok ->
                                    add_non_existing_to_map(Driver, ModelConfig, Key, LinkKey, Doc, Link, KeyNum);
                                Other2 ->
                                    Other2
                            end
                    end
                end,
            case maps:get(LinkName, LinkMap, undefined) of
                undefined ->
                    DoAdd();
                {_V, LinkTargets} ->
                    case [{S, VH, K, M} || {S, VH, K, M} <- LinkTargets, not is_tuple(VH)] of
                        [] ->
                            DoAdd();
                        _ ->
                            {error, already_exists}
                    end;
                _ ->
                    {error, already_exists}
            end;
        Other ->
            add_non_existing_to_map(Driver, ModelConfig, Key, LinkKey, Other, Link, KeyNum)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves link maps into several documents. Gets first link document from DB or creates new
%% (if does not exists) to call recursive save_links_maps/5.
%% @end
%%--------------------------------------------------------------------
-spec save_links_maps(Driver :: atom(), model_behaviour:model_config(), datastore:ext_key(),
    [datastore:normalized_link_spec()], OpType :: set | add | add_no_local) -> ok | datastore:generic_error().
save_links_maps(Driver,
    #model_config{bucket = _Bucket, name = ModelName, mother_link_scope = Scope1, other_link_scopes = _Scope2} = ModelConfig,
    Key, LinksList, OpType) ->
    Save = fun(Scope) ->
        LDK = links_doc_key(Key, Scope),
        case Driver:get_link_doc(ModelConfig, LDK) of
            {ok, LinksDoc} ->
                case OpType of
                    set ->
                        save_links_maps(Driver, ModelConfig, Key, LinksDoc, LinksList, 1, norm);
                    _ ->
                        case save_links_maps(Driver, ModelConfig, Key, LinksDoc, LinksList, 1, update) of
                            {ok, [_ | _] = NotAdded} ->
                                save_links_maps(Driver, ModelConfig, Key, LinksDoc, NotAdded, 1, no_old_checking);
                            Other ->
                                Other
                        end
                end;
            {error, {not_found, _}} ->
                LinksDoc = #document{key = LDK, value = #links{doc_key = Key, model = ModelName, origin = get_scopes(Scope, Key)}},
                save_links_maps(Driver, ModelConfig, Key, LinksDoc, LinksList, 1, no_old_checking);
            {error, Reason} ->
                {error, Reason}
        end
    end,

    NormalizedSave = fun(Scope) ->
        case Save(Scope) of
            {ok, []} ->
                ok;
            Err -> Err
        end
    end,


    LocalScope = get_scopes(?LOCAL_ONLY_LINK_SCOPE, Key),
    ReplicateScope = get_scopes(Scope1, Key),

    case OpType of
        add_no_local ->
            NormalizedSave(ReplicateScope);
        _ ->
            case NormalizedSave(LocalScope) of
                ok when LocalScope /= ReplicateScope ->
                    NormalizedSave(ReplicateScope);
                ok ->
                    ok;
                Error2 ->
                    Error2
            end
    end.


%%--------------------------------------------------------------------
%% @doc
%% Saves link maps into several documents.
%% @end
%%--------------------------------------------------------------------
-spec save_links_maps(Driver :: atom(), model_behaviour:model_config(), datastore:ext_key(), datastore:document(),
    [datastore:normalized_link_spec()], KeyNum :: integer(), Mode :: norm | update | no_old_checking) ->
    {ok, [datastore:normalized_link_spec()]} | datastore:generic_error().
save_links_maps(Driver, #model_config{bucket = _Bucket, name = ModelName} = ModelConfig, Key,
    #document{key = LDK, value = #links{link_map = LinkMap, children = Children, origin = Origin} = LinksRecord} = LinksDoc,
    LinksList, KeyNum, Mode) ->
    % update and add links to this document
    {FilledMap, NewLinksList, AddedLinks} =
        case Mode of
            update ->
                {Map, LinksToUpdate} = update_links_map(ModelConfig, LinksList, LinkMap),
                {Map, LinksToUpdate, []};
            _ ->
                fill_links_map(LinksList, LinkMap)
        end,

    case NewLinksList of
        [] ->
            % save changes and delete links from other documents if added here
            case Driver:save_link_doc(ModelConfig, LinksDoc#document{value = LinksRecord#links{link_map = FilledMap}}) of
                {ok, _} ->
                    case Mode of
                        norm ->
                            case del_old_links(Driver, ModelConfig, AddedLinks, LinksDoc, KeyNum) of
                                ok ->
                                    {ok, []};
                                DelErr ->
                                    DelErr
                            end;
                        _ ->
                            {ok, []}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            % Update other documents if needed
            % Find documents to be updated
            SplitedLinks = split_links_list(NewLinksList, KeyNum),
            {NewChildren, ChildrenDocs, ChildAdded} = maps:fold(fun(Num, _SLs, {Acc1, Acc2, Acc3}) ->
                NK = maps:get(Num, Children, <<"non">>),
                case {NK, Mode} of
                    {<<"non">>, update} ->
                        {Acc1, maps:put(Num, <<"non">>, Acc2), Acc3};
                    {<<"non">>, _} ->
                        NewLDK = links_child_doc_key(LDK, Num),
                        NLD = #document{key = NewLDK, value = #links{doc_key = Key, model = ModelName, origin = Origin}},
                        {maps:put(Num, NewLDK, Acc1), maps:put(Num, NLD, Acc2), true};
                    {_, update} ->
                        case Driver:get_link_doc(ModelConfig, NK) of
                            {ok, NLD} ->
                                {Acc1, maps:put(Num, NLD, Acc2), Acc3};
                            {error, {not_found, _}} ->
                                {Acc1, maps:put(Num, <<"non">>, Acc2), Acc3}
                        end;
                    _ ->
                        case Driver:get_link_doc(ModelConfig, NK) of
                            {ok, NLD} ->
                                {Acc1, maps:put(Num, NLD, Acc2), Acc3};
                            {error, {not_found, _}} ->
                                NLD = #document{key = NK, value = #links{doc_key = Key, model = ModelName, origin = Origin}},
                                {maps:put(Num, NK, Acc1), maps:put(Num, NLD, Acc2), Acc3}
                        end
                end
            end, {Children, #{}, false}, SplitedLinks),

            DelOldAns = case Mode of
                norm ->
                    del_old_links(Driver, ModelConfig, AddedLinks, LinksDoc, KeyNum);
                _ ->
                    ok
            end,

            % save modified doc
            NewLinksDoc = LinksDoc#document{value = LinksRecord#links{link_map = FilledMap,
                children = NewChildren}},
            Proceed = case ChildAdded or (length(NewLinksList) /= length(LinksList)) of
                true ->
                    Driver:save_link_doc(ModelConfig, NewLinksDoc);
                false ->
                    {ok, ok}
            end,

            case Proceed of
                {ok, _} ->
                    DelOldAns =
                        case Mode of
                            norm ->
                                del_old_links(Driver, ModelConfig, AddedLinks, LinksDoc, KeyNum);
                            _ ->
                                ok
                        end,
                    case DelOldAns of
                        ok ->
                            % update other docs recursive
                            maps:fold(fun(Num, SLs, FunAns) ->
                                case maps:get(Num, ChildrenDocs) of
                                    <<"non">> ->
                                        case FunAns of
                                            {ok, NotUpdated} ->
                                                {ok, NotUpdated ++ SLs};
                                            {OldError, _} ->
                                                OldError
                                        end;
                                    NDoc ->
                                        case {FunAns,
                                            save_links_maps(Driver, ModelConfig, Key, NDoc, SLs, KeyNum + 1, Mode)} of
                                            {{ok, NotUpdatedAcc}, {ok, NewNotUpdated}} ->
                                                {ok, NotUpdatedAcc ++ NewNotUpdated};
                                            {{ok, _}, NewError} ->
                                                NewError;
                                            {OldError, _} ->
                                                OldError
                                        end
                                end
                            end, {ok, []}, SplitedLinks);
                        Other ->
                            Other
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes all links from all documents connected with key.
%% @end
%%--------------------------------------------------------------------
-spec delete_links(Driver :: atom(), model_behaviour:model_config(), datastore:ext_key()) ->
    ok | datastore:generic_error().
delete_links(Driver, #model_config{mother_link_scope = Scope1, other_link_scopes = _Scope2} = ModelConfig, Key) ->
    Scopes = lists:usort([get_scopes(Scope1, Key), get_scopes(?LOCAL_ONLY_LINK_SCOPE, Key)]),
    lists:foldl(fun(Scope, Acc) ->
        case delete_links_docs(Driver, ModelConfig, links_doc_key(Key, Scope)) of
            ok ->
                Acc;
            Err ->
                Err
        end
    end, ok, Scopes).

%%--------------------------------------------------------------------
%% @doc
%% Deletes links from all documents connected with key. Calls recursive delete_links_from_maps/9 setting
%% arguments to their initial values.
%% @end
%%--------------------------------------------------------------------
-spec delete_links_from_maps(Driver :: atom(), model_behaviour:model_config(), datastore:ext_key(),
    [datastore:link_name()]) -> ok | datastore:generic_error().
delete_links_from_maps(Driver, #model_config{mother_link_scope = Scope1, name = ModelName, other_link_scopes = Scope2} = ModelConfig,
    Key, Links) ->
    LocalScope = get_scopes(?LOCAL_ONLY_LINK_SCOPE, Key),
    ReplicateScope = get_scopes(Scope1, Key),
    case delete_links_from_maps(Driver, ModelConfig, Key, Links, LocalScope) of
        {ok, _, _, DeletedMap} when LocalScope /= ReplicateScope -> %% Deleted links form local only tree
            case delete_links_from_maps(Driver, ModelConfig, Key, Links, get_scopes(Scope1, Key)) of
                {ok, _, Left, LocallyDeletedMap} -> %% Not deleted from requested scope tree
                    LeftRaw = [RawName || {RawName, _, _} <- [unpack_link_scope(ModelName, K1) || K1 <- Left]],
                    ToAdd0 = maps:with(LeftRaw, DeletedMap), %% Select all deleted locally but missing in given scope
                    {Added, _} = diff(LocallyDeletedMap, DeletedMap),

                    ToAdd1 = maps:map( %% Transform deleted links to "marked" as deleted entires
                        fun(_K, {V, Targets}) ->
                            {V, [{Scope0, {deleted, VHash0}, Key0, Model0} || {Scope0, VHash0, Key0, Model0} <- Targets, ReplicateScope =/= Scope0]}
                        end, maps:merge(ToAdd0, maps:without(LeftRaw, Added))),
                    ToAdd2 = maps:filter( %% When link is deleted using explicit VHash, should not be marked as deleted
                        %% since system user is unable to perform such operation
                        %% Also ignore empty links
                        fun
                            (_, {_, []}) -> false;
                            (K0, _) ->
                                {_, _, VH0} = unpack_link_scope(ModelName, K0),
                                VH0 == undefined
                        end, ToAdd1),
                    %% @fixme: find out better way to detect system operations
                    case get_scopes(Scope2, Key) of
                        <<"system_internal">> ->
                            ok;
                        _ ->
                            save_links_maps(Driver, ModelConfig, Key, maps:to_list(ToAdd2), add_no_local)
                    end;
                Ans0 ->
                    Ans0
            end;
        {ok, _, _, _} ->
            ok;
        Ans ->
            Ans
    end.

%%--------------------------------------------------------------------
%% @doc
%% Deletes links from all documents connected with key in chosen scopes.
%% Calls recursive delete_links_from_maps/9 setting arguments to their initial values.
%% @end
%%--------------------------------------------------------------------
-spec delete_links_from_maps(Driver :: atom(), model_behaviour:model_config(), datastore:ext_key(),
    [datastore:link_name()], Scopes :: atom() | [atom()] | links_utils:mother_scope()) ->
    ok | {ok, integer(), [datastore:link_name()]} | datastore:generic_error().
delete_links_from_maps(_Driver, _ModelConfig, _Key, _Links, []) ->
    ok;
delete_links_from_maps(Driver, ModelConfig, Key, Links, Scope) ->
    delete_links_from_maps(Driver, ModelConfig, links_doc_key(Key, Scope), Links, 0, Key, 1, #document{}, 0, #{}).

%%--------------------------------------------------------------------
%% @doc
%% Deletes links from all documents connected with key.
%% @end
%%--------------------------------------------------------------------
-spec delete_links_from_maps(Driver :: atom(), model_behaviour:model_config(), Key :: datastore:ext_key(),
    [datastore:link_name()], FreeSpaces :: integer(), MainDocKey :: datastore:ext_key(), KeyNum :: integer(),
    Parent :: datastore:ext_key() | datastore:document(), ParentNum :: integer(), map()) ->
    {ok, integer(), [datastore:link_name()], map()} | datastore:generic_error().
delete_links_from_maps(_Driver, _ModelConfig, <<"non">>, Links, _FreeSpaces, _MainDocKey, _KeyNum, _Parent, _ParentNum, DeletedMap) ->
    {ok, 0, Links, DeletedMap};
delete_links_from_maps(Driver, ModelConfig = #model_config{name = ModelName}, Key, Links, FreeSpaces, MainDocKey, KeyNum, Parent, ParentNum, DeletedMap) ->
    case Driver:get_link_doc(ModelConfig, Key) of
        {ok, #document{value = #links{children = Children, link_map = LinkMap} = LinksRecord} = LinkDoc} ->
            {NewLinkMap, NewLinks, Deleted, DeletedMap1} = remove_from_links_map(ModelName, Links, LinkMap),
            NewSize = maps:size(NewLinkMap),
            {SaveAns, NewLinkRef} = case Deleted of
                0 ->
                    {{ok, ok}, LinkDoc};
                _ ->
                    NLD = LinkDoc#document{value = LinksRecord#links{link_map = NewLinkMap}},
                    {Driver:save_link_doc(ModelConfig, NLD), NLD#document.key}
            end,

            NewFreeSpaces = FreeSpaces + ?LINKS_MAP_MAX_SIZE - NewSize,
            case {SaveAns, NewLinks} of
                {{ok, _}, []} ->
                    {ok, 0, [], maps:merge(DeletedMap, DeletedMap1)};
                {{ok, _}, _} ->
                    SplitedLinks = split_links_names_list(ModelConfig, NewLinks, KeyNum),
                    maps:fold(fun(Num, SLs, Acc) ->
                        case Acc of
                            {ok, UsedFreeSpaces, LinksLeft, AccDeletedMap} ->
                                NextKey = maps:get(Num, Children, <<"non">>),
                                case delete_links_from_maps(Driver, ModelConfig, NextKey, SLs,
                                    NewFreeSpaces - UsedFreeSpaces, MainDocKey, KeyNum + 1, NewLinkRef, Num, AccDeletedMap) of
                                    {ok, UsedFreeSpaces2, LinksLeft2, DeletedMapChild} ->
                                        {ok, UsedFreeSpaces + UsedFreeSpaces2, LinksLeft ++ LinksLeft2, DeletedMapChild};
                                    Other ->
                                        Other
                                end;
                            _ ->
                                Acc
                        end
                    end, {ok, 0, [], maps:merge(DeletedMap, DeletedMap1)}, SplitedLinks);
                _ ->
                    SaveAns
            end;
        {error, {not_found, _}} ->
            {ok, 0, Links, DeletedMap};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fetches link from set of documents connected with key.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(Driver :: atom(), model_behaviour:model_config(), datastore:link_name(), datastore:ext_key()) ->
    {ok, datastore:link_target()} | datastore:link_error().
fetch_link(Driver, #model_config{} = ModelConfig,
    LinkName, Key) ->
    fetch_link(Driver, ModelConfig, LinkName, Key, get_scopes(?LOCAL_ONLY_LINK_SCOPE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% Fetches link from set of documents connected with key from chosen scopes.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(Driver :: atom(), model_behaviour:model_config(), datastore:ext_key(), datastore:link_name(),
    Scopes :: atom() | [atom()] | links_utils:mother_scope()) ->
    {ok, datastore:link_target()} | datastore:link_error().
fetch_link(_Driver, _ModelConfig, _LinkName, _Key, []) ->
    {error, link_not_found};
fetch_link(Driver, ModelConfig, LinkName, Key, [Scope | Scopes]) ->
    case fetch_link(Driver, ModelConfig, LinkName, Key, Scope) of
        {error, link_not_found} ->
            fetch_link(Driver, ModelConfig, LinkName, Key, Scopes);
        Ans ->
            Ans
    end;
fetch_link(Driver, ModelConfig, LinkName, Key, Scope) ->
    fetch_link_from_docs(Driver, ModelConfig, LinkName, links_doc_key(Key, Scope), 1).

%%--------------------------------------------------------------------
%% @doc
%% Executes function for each link connected with key.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link(Driver :: atom(), model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
foreach_link(Driver, #model_config{} = ModelConfig,
    Key, Fun, AccIn) ->
    foreach_link(Driver, ModelConfig, Key, Fun, {ok, AccIn}, [get_scopes(?LOCAL_ONLY_LINK_SCOPE, Key)]).

-spec foreach_link(Driver :: atom(), model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term(),
    Scopes :: [atom()]) ->
    {ok, Acc :: term()} | datastore:link_error().
foreach_link(_Driver, _ModelConfig, _Key, _Fun, AccIn, []) ->
    AccIn;
foreach_link(Driver, ModelConfig, Key, Fun, {ok, AccIn}, [Scope | Scopes]) ->
    AccOut = foreach_link_in_docs(Driver, ModelConfig, links_doc_key(Key, Scope), Fun, AccIn),
    foreach_link(Driver, ModelConfig, Key, Fun, AccOut, Scopes);
foreach_link(_Driver, _ModelConfig, _Key, _Fun, AccIn, _Scopes) ->
    AccIn.

%%--------------------------------------------------------------------
%% @doc
%% Returns key for document holding links for given document.
%% @end
%%--------------------------------------------------------------------
-spec links_doc_key(Key :: datastore:key(), Scope :: scope()) -> BinKey :: binary().
links_doc_key(Key, Scope) ->
    Base = links_doc_key_from_scope(Key, Scope),
    case byte_size(Base) > 120 of
        true ->
            binary:part(Base, {0, 120});
        _ ->
            Base
    end.


%%--------------------------------------------------------------------
%% @doc
%% Makes full link name based on base link name and its scope. Works only on binary link names and scopes.
%% @end
%%--------------------------------------------------------------------
-spec make_scoped_link_name(datastore:link_name(), scope(), vhash(), non_neg_integer()) ->
    datastore:link_name().
make_scoped_link_name(LinkName, Scope, undefined, Length) when is_binary(LinkName), is_binary(Scope) ->
    ShortScope = binary:part(Scope, 0, Length),
    <<LinkName/binary, ?LINK_NAME_SCOPE_SEPARATOR, ShortScope/binary>>;
make_scoped_link_name(LinkName, Scope, VHash, Length) when is_binary(LinkName), is_binary(Scope) ->
    ShortScope = binary:part(Scope, 0, Length),
    <<LinkName/binary, ?LINK_NAME_SCOPE_SEPARATOR, ShortScope/binary, ?LINK_NAME_SCOPE_SEPARATOR, VHash/binary>>;
make_scoped_link_name(LinkName, Scope, VHash, _Len) ->
    {scoped_link, LinkName, Scope, VHash}.


%%--------------------------------------------------------------------
%% @doc
%% Split given link name to its base name and scope target. Reverts make_scoped_link_name/2.
%% @end
%%--------------------------------------------------------------------
-spec unpack_link_scope(ModelName :: model_behaviour:model_type(), LinkName :: datastore:link_name()) ->
    {datastore:link_name(), scope(), vhash()}.
unpack_link_scope(_ModelName, LinkName) when is_binary(LinkName) ->
    case binary:split(LinkName, <<?LINK_NAME_SCOPE_SEPARATOR>>, [global, trim_all]) of
        [LinkName0] ->
            {LinkName0, undefined, undefined};
        Other ->
            case lists:reverse(Other) of
                [<<?VHASH_PREFIX, _/binary>> = VHash, Scope, OLinkName | _] ->
                    {OLinkName, Scope, VHash};
                [Scope | R] ->
                    {str_utils:join_binary(lists:reverse(R), <<?LINK_NAME_SCOPE_SEPARATOR>>), Scope, undefined}
            end
    end;
unpack_link_scope(_ModelName, {scoped_link, LinkName, Scope, VHash}) ->
    {LinkName, Scope, VHash};
unpack_link_scope(_ModelName, LinkName) ->
    {LinkName, undefined, undefined}.


%%--------------------------------------------------------------------
%% @doc
%% Select link that corresponds to given scope name.
%% @end
%%--------------------------------------------------------------------
-spec select_scope_related_link(LinkName :: datastore:link_name(), RequestedScope :: scope(), VHash :: vhash(),
    [datastore:link_final_target()]) -> datastore:link_final_target() | undefined.
select_scope_related_link(LinkName, RequestedScope, VHash, Targets) ->
    case lists:filter(
        fun
            ({Scope, VH, _, _}) when is_binary(LinkName), is_binary(Scope), is_binary(RequestedScope) ->
                lists:prefix(binary_to_list(RequestedScope), binary_to_list(Scope))
                    andalso (VHash == undefined orelse VHash == VH);
            ({Scope, VH, _, _}) ->
                RequestedScope =:= Scope andalso (VHash == undefined orelse VHash == VH);
            ({Scope, {deleted, VH}, _, _}) when is_binary(LinkName), is_binary(Scope), is_binary(RequestedScope) ->
                lists:prefix(binary_to_list(RequestedScope), binary_to_list(Scope))
                    andalso (VHash == undefined orelse VHash == VH);
            ({Scope, {deleted, VH}, _, _}) ->
                RequestedScope =:= Scope andalso (VHash == undefined orelse VHash == VH)
        end, Targets) of
        [] -> undefined;
        [L] ->
            L;
        _ -> undefined
    end.


%%--------------------------------------------------------------------
%% @doc
%% Gets link context to be applied in another process.
%% @end
%%--------------------------------------------------------------------
-spec get_context_to_propagate(model_behaviour:model_config()) ->
    {{ok | skip, mother_scope() | skip, scope() | skip}, {ok | skip, other_scopes() | skip, [scope()] | skip}}.
get_context_to_propagate(#model_config{mother_link_scope = MS, other_link_scopes = OS}) ->
    A1 = case is_atom(MS) of
        true -> {ok, MS, get(MS)};
        _ -> {skip, skip, skip}
    end,
    A2 = case is_atom(OS) of
        true -> {ok, OS, get(OS)};
        _ -> {skip, skip, skip}
    end,
    {A1, A2}.

%%--------------------------------------------------------------------
%% @doc
%% Sets link context from another process.
%% @end
%%--------------------------------------------------------------------
-spec apply_context({{ok | skip, mother_scope() | skip, scope() | skip},
    {ok | skip, other_scopes() | skip, [scope()] | skip}}) -> ok.
apply_context({MS, OS}) ->
    case MS of
        {ok, MSK, MSV} -> put(MSK, MSV);
        _ -> ok
    end,
    case OS of
        {ok, OSK, OSV} -> put(OSK, OSV);
        _ -> ok
    end,
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets number of child to which link should be mapped.
%% Converts LinkName to binary if needed.
%% @equiv get_link_child_num(LinkName, KeyNum, byte_size(LinkName))
%% @end
%%--------------------------------------------------------------------
-spec get_link_child_num(datastore:link_name(), KeyNum :: integer()) -> integer().
get_link_child_num(LinkName, KeyNum) when is_binary(LinkName) ->
    get_link_child_num(LinkName, KeyNum, byte_size(LinkName));
get_link_child_num(LinkName, KeyNum) ->
    get_link_child_num(term_to_binary(LinkName), KeyNum).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets number of child to which link should be mapped.
%% @end
%%--------------------------------------------------------------------
-spec get_link_child_num(datastore:link_name(), KeyNum :: integer(), ByteSize :: integer()) -> integer().
get_link_child_num(LinkName, KeyNum, ByteSize) when KeyNum > ByteSize ->
    get_link_child_num(binary:at(LinkName, 0));
get_link_child_num(LinkName, KeyNum, ByteSize) ->
    get_link_child_num(binary:at(LinkName, ByteSize - KeyNum)).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets number of child using give byte from link name.
%% @end
%%--------------------------------------------------------------------
-spec get_link_child_num(byte()) -> integer().
get_link_child_num(Byte) ->
    Byte rem ?LINKS_TREE_BASE.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Splits links' list to several lists connected with particular key.
%% @end
%%--------------------------------------------------------------------
-spec split_links_list([datastore:normalized_link_spec()], KeyNum :: integer()) -> map().
split_links_list(LinksList, KeyNum) ->
    lists:foldl(fun({LinkName, _} = Link, Acc) ->
        LinkNum = get_link_child_num(LinkName, KeyNum),
        TmpAns = maps:get(LinkNum, Acc, []),
        maps:put(LinkNum, [Link | TmpAns], Acc)
    end, #{}, LinksList).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Splits links' list to several lists connected with particular key.
%% @end
%%--------------------------------------------------------------------
-spec split_links_names_list(model_behaviour:model_config(), [datastore:link_name()], KeyNum :: integer()) -> map().
split_links_names_list(#model_config{name = ModelName}, LinksList, KeyNum) ->
    lists:foldl(fun(LinkName, Acc) ->
        {RawLinkName, _, _} = unpack_link_scope(ModelName, LinkName),
        LinkNum = get_link_child_num(RawLinkName, KeyNum),
        TmpAns = maps:get(LinkNum, Acc, []),
        maps:put(LinkNum, [LinkName | TmpAns], Acc)
    end, #{}, LinksList).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fills map with links. Replaces link if exists.
%% @end
%%--------------------------------------------------------------------
-spec fill_links_map([datastore:normalized_link_spec()], map()) -> {map(), LinksToAdd :: list(), AddedLinks :: list()}.
fill_links_map(LinksList, LinkMap) ->
    MapSize = maps:size(LinkMap),
    fill_links_map(LinksList, LinkMap, MapSize, [], []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Fills map with links. Replaces link if exists.
%% @end
%%--------------------------------------------------------------------
-spec fill_links_map([datastore:normalized_link_spec()], map(), MapSize :: integer(),
    LinksToAdd :: list(), AddedLinks :: list()) -> {map(), FinalLinksToAdd :: list(), FinalAddedLinks :: list()}.
fill_links_map([], Map, _MapSize, AddedLinks, LinksToAdd) ->
    {Map, LinksToAdd, AddedLinks};
fill_links_map([{LinkName, LinkTarget} = Link | R], Map, MapSize, AddedLinks, LinksToAdd)
    when MapSize >= ?LINKS_MAP_MAX_SIZE ->
    case maps:is_key(LinkName, Map) of
        true ->
            fill_links_map(R, maps:put(LinkName, LinkTarget, Map), MapSize, AddedLinks, LinksToAdd);
        _ ->
            fill_links_map(R, Map, MapSize, AddedLinks, [Link | LinksToAdd])
    end;
fill_links_map([{LinkName, LinkTarget} | R], Map, MapSize, AddedLinks, LinksToAdd) ->
    case maps:is_key(LinkName, Map) of
        true ->
            fill_links_map(R, maps:put(LinkName, LinkTarget, Map), MapSize, AddedLinks, LinksToAdd);
        _ ->
            fill_links_map(R, maps:put(LinkName, LinkTarget, Map), MapSize + 1, [LinkName | AddedLinks], LinksToAdd)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates map with links replacing link if exists.
%% @end
%%--------------------------------------------------------------------
-spec update_links_map(model_behaviour:model_config(), [datastore:normalized_link_spec()], map()) -> {map(), LinksToUpdate :: list()}.
update_links_map(ModelConfig, LinksList, LinkMap) ->
    MapSize = maps:size(LinkMap),
    update_links_map(ModelConfig, LinksList, LinkMap, MapSize, []).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates map with links replacing link if exists.
%% @end
%%--------------------------------------------------------------------
-spec update_links_map(model_behaviour:model_config(), [datastore:normalized_link_spec()], map(), MapSize :: integer(),
    LinksToUpdate :: list()) -> {map(), FinalLinksToUpdate :: list()}.
update_links_map(_ModelConfig, [], Map, _MapSize, LinksToUpdate) ->
    {Map, LinksToUpdate};
update_links_map(ModelConfig, [{LinkName, {_NewVersion, LinkTargets} = LinkTarget} = Link | R], Map, MapSize, LinksToUpdate) ->
    case {maps:is_key(LinkName, Map), ModelConfig#model_config.link_duplication} of
        {true, false} ->
            update_links_map(ModelConfig, R, maps:put(LinkName, LinkTarget, Map), MapSize, LinksToUpdate);
        {true, true} ->
            {_OldVersion, OldLinks} = maps:get(LinkName, Map),
            UpdatedTargets = deduplicate_targets(lists:usort(OldLinks ++ LinkTargets)),
            update_links_map(ModelConfig, R, maps:put(LinkName, {1, UpdatedTargets}, Map), MapSize, LinksToUpdate);
        _ ->
            update_links_map(ModelConfig, R, Map, MapSize, [Link | LinksToUpdate])
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes links updated at higher level of links tree.
%% @end
%%--------------------------------------------------------------------
-spec del_old_links(Driver :: atom(), model_behaviour:model_config(), [datastore:normalized_link_spec()],
    datastore:ext_key() | datastore:document(), KeyNum :: integer()) -> ok | datastore:generic_error().
del_old_links(_Driver, _ModelConfig, [], _Key, _KeyNum) ->
    ok;
del_old_links(_Driver, #model_config{bucket = _Bucket} = _ModelConfig, _Links, <<"non">>, _KeyNum) ->
    ok;
del_old_links(Driver, #model_config{bucket = _Bucket} = ModelConfig, Links,
    #document{value = #links{children = Children}}, KeyNum) ->
    SplitedLinks = split_links_names_list(ModelConfig, Links, KeyNum),
    maps:fold(fun(Num, SLs, Acc) ->
        case Acc of
            ok ->
                NextKey = maps:get(Num, Children, <<"non">>),
                del_old_links(Driver, ModelConfig, SLs, NextKey, KeyNum + 1);
            _ ->
                Acc
        end
    end, ok, SplitedLinks);
del_old_links(Driver, #model_config{bucket = _Bucket} = ModelConfig, Links, Key, KeyNum) ->
    case Driver:get_link_doc(ModelConfig, Key) of
        {ok, #document{value = #links{link_map = LinkMap} = LinksRecord} = LinkDoc} ->
            {NewLinkMap, NewLinks, Deleted} = remove_from_links_map(Links, LinkMap),
            SaveAns = case Deleted of
                0 ->
                    {ok, ok};
                _ ->
                    NLD = LinkDoc#document{value = LinksRecord#links{link_map = NewLinkMap}},
                    Driver:save_link_doc(ModelConfig, NLD)
            end,

            case {SaveAns, NewLinks} of
                {{ok, _}, []} ->
                    ok;
                {{ok, _}, _} ->
                    del_old_links(Driver, ModelConfig, NewLinks, LinkDoc, KeyNum);
                Error ->
                    Error
            end;
        {error, {not_found, _}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes links from links' map.
%% @end
%%--------------------------------------------------------------------
-spec remove_from_links_map([datastore:link_name()], map()) ->
    {map(), NewLinks :: list(), Deleted :: integer()}.
remove_from_links_map(Links, Map) ->
    remove_from_links_map(Links, Map, [], 0).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes links from links' map.
%% @end
%%--------------------------------------------------------------------
-spec remove_from_links_map([datastore:link_name()], map(), TmpNewLinks :: list(), TmpDeleted :: integer()) ->
    {map(), NewLinks :: list(), Deleted :: integer()}.
remove_from_links_map([], Map, NewLinks, Deleted) ->
    {Map, NewLinks, Deleted};
remove_from_links_map([Link | R], Map, NewLinks, Deleted) ->
    case maps:is_key(Link, Map) of
        true ->
            remove_from_links_map(R, maps:remove(Link, Map), NewLinks, Deleted + 1);
        _ ->
            remove_from_links_map(R, Map, [Link | NewLinks], Deleted)
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes all documents that store links connected with key.
%% @end
%%--------------------------------------------------------------------
-spec delete_links_docs(Driver :: atom(), model_behaviour:model_config(), datastore:ext_key()) ->
    ok | datastore:generic_error().
delete_links_docs(_Driver, _ModelConfig, <<"non">>) ->
    ok;
delete_links_docs(Driver, #model_config{} = ModelConfig, Key) ->
    case Driver:get_link_doc(ModelConfig, Key) of
        {error, {not_found, _}} ->
            ok;
        {error, not_found} ->
            ok;
        {error, Reason} ->
            {error, Reason};
        {ok, #document{value = #links{children = Children}} = Doc} ->
            case Driver:delete_link_doc(ModelConfig, Doc) of
                ok ->
                    maps:fold(fun(_Num, ChildKey, FunAns) ->
                        case FunAns of
                            ok ->
                                delete_links_docs(Driver, ModelConfig, ChildKey);
                            OldError ->
                                OldError
                        end
                    end, ok, Children);
                Error ->
                    Error
            end
    end.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes links from links' map.
%% @end
%%--------------------------------------------------------------------
-spec remove_from_links_map(ModelName :: model_behaviour:model_type(), [datastore:link_name()], map()) ->
    {map(), NewLinks :: list(), Deleted :: integer(), map()}.
remove_from_links_map(ModelName, Links, Map) ->
    remove_from_links_map(ModelName, Links, Map, [], 0, #{}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Removes links from links' map.
%% @end
%%--------------------------------------------------------------------
-spec remove_from_links_map(ModelName :: model_behaviour:model_type(), [datastore:link_name()], map(),
    TmpNewLinks :: list(), TmpDeleted :: integer(), map()) ->
    {map(), NewLinks :: list(), Deleted :: integer(), map()}.
remove_from_links_map(_ModelName, [], Map, NewLinks, Deleted, DeletedMap) ->
    {Map, NewLinks, Deleted, DeletedMap};
remove_from_links_map(ModelName, [Link | R], Map, NewLinks, Deleted, DeletedMap) ->
    {RawLinkName, RequestedScope, VHash} = unpack_link_scope(ModelName, Link),
    case maps:is_key(RawLinkName, Map) of
        true ->
            {V, Targets} = maps:get(RawLinkName, Map),
            case length(Targets) of
                L when L > 0 ->
                    RelatedTarget = select_scope_related_link(RawLinkName, RequestedScope, VHash, Targets),
                    case {RequestedScope, RelatedTarget} of
                        {undefined, _} -> %% Non-scoped link - delete all
                            remove_from_links_map(ModelName, R, maps:remove(RawLinkName, Map), NewLinks, Deleted + length(Targets),
                                maps:put(RawLinkName, maps:get(RawLinkName, Map), DeletedMap));
                        {_, undefined} -> %% Unable to find scope, link is ambiguous - unable to delete
                            remove_from_links_map(ModelName, R, Map, NewLinks, Deleted, DeletedMap);
                        {_, _} ->
                            NewTargets = Targets -- [RelatedTarget],
                            case NewTargets of
                                [] ->
                                    remove_from_links_map(ModelName, R, maps:remove(RawLinkName, Map), NewLinks, Deleted + length(Targets),
                                        maps:put(RawLinkName, {V, [RelatedTarget]}, DeletedMap));
                                _ ->
                                    remove_from_links_map(ModelName, R, maps:put(RawLinkName, {V, NewTargets}, Map), NewLinks,
                                        Deleted + length(Targets -- NewTargets), maps:put(RawLinkName, {V, [RelatedTarget]}, DeletedMap))
                            end
                    end;
                _ ->
                    remove_from_links_map(ModelName, R, maps:remove(RawLinkName, Map), NewLinks, Deleted + 1,
                        maps:put(RawLinkName, maps:get(RawLinkName, Map), DeletedMap))
            end;
        _ ->
            remove_from_links_map(ModelName, R, Map, [Link | NewLinks], Deleted, DeletedMap)
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fetches link from set of documents.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link_from_docs(Driver :: atom(), model_behaviour:model_config(), datastore:link_name(),
    datastore:ext_key(), KeyNum :: integer()) -> {ok, datastore:link_target()} | datastore:link_error().
fetch_link_from_docs(Driver, #model_config{bucket = _Bucket} = ModelConfig, LinkName, LinkKey, KeyNum) ->
    case Driver:get_link_doc(ModelConfig, LinkKey) of
        {ok, #document{value = #links{link_map = LinkMap, children = Children}}} ->
            case maps:get(LinkName, LinkMap, undefined) of
                undefined ->
                    LinkNum = get_link_child_num(LinkName, KeyNum),
                    NextKey = maps:get(LinkNum, Children, <<"non">>),
                    case NextKey of
                        <<"non">> ->
                            {error, link_not_found};
                        _ ->
                            fetch_link_from_docs(Driver, ModelConfig, LinkName, NextKey, KeyNum + 1)
                    end;
                {V, LinkTargets} ->
                    case [{S, VH, K, M} || {S, VH, K, M} <- LinkTargets, not is_tuple(VH)] of
                        [] ->
                            {error, link_not_found};
                        NewTargets ->
                            {ok, {V, NewTargets}}
                    end
            end;
        {error, {not_found, _}} ->
            {error, link_not_found};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Executes function at each link.
%% @end
%%--------------------------------------------------------------------
-spec foreach_link_in_docs(Driver :: atom(), model_behaviour:model_config(), Key :: datastore:ext_key(),
    fun((datastore:link_name(), datastore:link_target(), Acc :: term()) -> Acc :: term()), AccIn :: term()) ->
    {ok, Acc :: term()} | datastore:link_error().
foreach_link_in_docs(_Driver, _ModelConfig, <<"non">>, _Fun, AccIn) ->
    {ok, AccIn};
foreach_link_in_docs(Driver, #model_config{bucket = _Bucket} = ModelConfig, LinkKey, Fun, AccIn) ->
    case Driver:get_link_doc(ModelConfig, LinkKey) of
        {ok, #document{value = #links{link_map = LinkMap, children = Children}}} ->
            WrapperFun = fun
                (K0, {Ver0, Targets0}, Acc0) ->
                    case [{S, VH, K, M} || {S, VH, K, M} <- Targets0, not is_tuple(VH)] of
                        [] ->
                            Acc0;
                        NewTargets0 ->
                            Fun(K0, {Ver0, NewTargets0}, Acc0)
                    end
            end,
            NewAccIn = maps:fold(WrapperFun, AccIn, LinkMap),
            maps:fold(fun(_Num, ChildKey, FunAns) ->
                case FunAns of
                    {ok, TmpAcc} ->
                        foreach_link_in_docs(Driver, ModelConfig, ChildKey, Fun, TmpAcc);
                    OldError ->
                        OldError
                end
            end, {ok, NewAccIn}, Children);
        {error, {not_found, _}} ->
            {ok, AccIn};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Create base for links document key (base must be cut if too long).
%% @end
%%--------------------------------------------------------------------
-spec links_doc_key_from_scope(Key :: datastore:key(), Scope :: scope()) -> BinKey :: binary().
links_doc_key_from_scope(Key, ?LOCAL_ONLY_LINK_SCOPE = Scope) when is_binary(Key) ->
    <<?NOSYNC_KEY_OVERRIDE_PREFIX/binary, Key/binary, Scope/binary>>;
links_doc_key_from_scope(Key, Scope) when is_binary(Key), is_binary(Scope) ->
    <<Key/binary, Scope/binary>>;
links_doc_key_from_scope(Key, Scope) ->
    base64:encode(term_to_binary({Key, Scope})).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns key for document holding links for given document.
%% @end
%%--------------------------------------------------------------------
-spec links_child_doc_key(PrevKey :: datastore:key(), Num :: integer()) -> BinKey :: binary().
links_child_doc_key(Key, Num) ->
    BinNum = list_to_binary("_" ++ integer_to_list(Num)),
    <<Key/binary, BinNum/binary>>.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Adds link to map. Assumes that link does not exist.
%% @end
%%--------------------------------------------------------------------
-spec add_non_existing_to_map(Driver :: atom(), model_behaviour:model_config(),
    Key :: datastore:ext_key(), LinkKey :: datastore:ext_key(),
    GetAns :: datastore:document() | datastore:generic_error(), datastore:normalized_link_spec(),
    KeyNum :: integer()) -> ok | datastore:generic_error().
add_non_existing_to_map(Driver, #model_config{bucket = _Bucket, name = ModelName, mother_link_scope = MScopeFun} = ModelConfig, Key, LDK, GetAns, Link, 1) ->
    TmpAns = case GetAns of
        #document{} = Doc ->
            save_links_maps(Driver, ModelConfig, Key, Doc, [Link], 1, no_old_checking);
        {error, {not_found, _}} ->
            LinksDoc = #document{key = LDK, value = #links{origin = get_scopes(MScopeFun, Key), doc_key = Key, model = ModelName}},
            save_links_maps(Driver, ModelConfig, Key, LinksDoc, [Link], 1, no_old_checking);
        {error, Reason} ->
            {error, Reason}
    end,
    case TmpAns of
        {ok, _} ->
            ok;
        _ ->
            TmpAns
    end;
add_non_existing_to_map(_Driver, _ModelConfig, _Key, _LDK, _GetAns, _Link, _KeyNum) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get scopes.
%% @end
%%--------------------------------------------------------------------
-spec get_scopes(ScopesGetter :: mother_scope() | other_scopes() | scope(), datastore:key()) -> scope() | [scope()].
get_scopes(ScopesGetter, _) when is_atom(ScopesGetter) ->
    get(ScopesGetter);
get_scopes(ScopesGetter, Key) when is_function(ScopesGetter) ->
    ScopesGetter(Key);
get_scopes(Scope, _Key) ->
    Scope.
