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

-type mother_scope_fun() :: fun(() -> atom()).
-type other_scopes_fun() :: fun(() -> [atom()]).
-export_type([mother_scope_fun/0, other_scopes_fun/0]).

%% API
-export([create_link_in_map/4, save_links_maps/4, delete_links/3, delete_links_from_maps/4,
    fetch_link/4, foreach_link/5, links_doc_key/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates link. Before creation it checks if it exists in several documents.
%% @end
%%--------------------------------------------------------------------
-spec create_link_in_map(Driver :: atom(), model_behaviour:model_config(), datastore:ext_key(),
    datastore:normalized_link_spec()) -> ok | datastore:create_error().
create_link_in_map(Driver, #model_config{mother_link_scope = ScopeFun1, other_link_scopes = ScopeFun2} = ModelConfig,
    Key, {LinkName, _} = Link) ->
    case fetch_link(Driver, ModelConfig, LinkName, Key, ScopeFun2()) of
        {error, link_not_found} ->
            create_link_in_map(Driver, ModelConfig, Link, Key, links_doc_key(Key, ScopeFun1), 1);
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
    case Driver:get_link_doc_inside_trans(ModelConfig, LinkKey) of
        {ok, #document{value = #links{link_map = LinkMap, children = Children}} = Doc} ->
            case maps:get(LinkName, LinkMap, undefined) of
                undefined ->
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
    [datastore:normalized_link_spec()]) -> ok | datastore:generic_error().
save_links_maps(Driver,
    #model_config{bucket = _Bucket, name = ModelName, mother_link_scope = ScopeFun1, other_link_scopes = ScopeFun2} = ModelConfig,
    Key, LinksList) ->
    LDK = links_doc_key(Key, ScopeFun1),
    SaveAns = case Driver:get_link_doc_inside_trans(ModelConfig, LDK) of
        {ok, LinksDoc} ->
            save_links_maps(Driver, ModelConfig, Key, LinksDoc, LinksList, 1, true);
        {error, {not_found, _}} ->
            LinksDoc = #document{key = LDK, value = #links{doc_key = Key, model = ModelName}},
            save_links_maps(Driver, ModelConfig, Key, LinksDoc, LinksList, 1, true);
        {error, Reason} ->
            {error, Reason}
    end,
    case SaveAns of
        {ok, []} ->
            ok;
        {ok, LinksToDel} ->
            delete_links_from_maps(Driver, ModelConfig, Key, LinksToDel, ScopeFun2());
        _ ->
            SaveAns
    end.

%%--------------------------------------------------------------------
%% @doc
%% Saves link maps into several documents.
%% @end
%%--------------------------------------------------------------------
-spec save_links_maps(Driver :: atom(), model_behaviour:model_config(), datastore:ext_key(), datastore:document(),
    [datastore:normalized_link_spec()], KeyNum :: integer(), CheckForOld :: boolean()) ->
    {ok, [datastore:link_name()]} | datastore:generic_error().
save_links_maps(Driver, #model_config{bucket = _Bucket, name = ModelName} = ModelConfig, Key,
    #document{key = LDK, value = #links{link_map = LinkMap, children = Children} = LinksRecord} = LinksDoc,
    LinksList, KeyNum, CheckForOld) ->
    {FilledMap, NewLinksList, AddedLinks} = fill_links_map(LinksList, LinkMap),
    case NewLinksList of
        [] ->
            case Driver:save_link_doc(ModelConfig, LinksDoc#document{value = LinksRecord#links{link_map = FilledMap}}) of
                {ok, _} ->
                    case CheckForOld of
                        true ->
                            del_old_links(Driver, ModelConfig, AddedLinks, LinksDoc, KeyNum);
                        _ ->
                            {ok, AddedLinks}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            SplitedLinks = split_links_list(NewLinksList, KeyNum),
            {NewChildren, ChildrenDocs} = maps:fold(fun(Num, _SLs, {Acc1, Acc2}) ->
                NK = maps:get(Num, Children, <<"non">>),
                case NK of
                    <<"non">> ->
                        NewLDK = links_child_doc_key(LDK, Num),
                        NLD = #document{key = NewLDK, value = #links{doc_key = Key, model = ModelName}},
                        {maps:put(Num, NewLDK, Acc1), maps:put(Num, NLD, Acc2)};
                    NewLDK2 ->
                        case Driver:get_link_doc_inside_trans(ModelConfig, NK) of
                            {ok, NLD} ->
                                {Acc1, maps:put(Num, NLD, Acc2)};
                            {error, {not_found, _}} ->
                                NLD = #document{key = NewLDK2, value = #links{doc_key = Key, model = ModelName}},
                                {maps:put(Num, NewLDK2, Acc1), maps:put(Num, NLD, Acc2)}
                        end
                end
            end, {Children, #{}}, SplitedLinks),
            NewLinksDoc = LinksDoc#document{value = LinksRecord#links{link_map = FilledMap,
                children = NewChildren}},
            case Driver:save_link_doc(ModelConfig, NewLinksDoc) of
                {ok, _} ->
                    DelOldAns = case CheckForOld of
                        true ->
                            del_old_links(Driver, ModelConfig, AddedLinks, LinksDoc, KeyNum);
                        _ ->
                            {ok, []}
                    end,
                    case DelOldAns of
                        {ok, AnsExt} ->
                            FoldAns = maps:fold(fun(Num, SLs, FunAns) ->
                                NDoc = maps:get(Num, ChildrenDocs),
                                case {FunAns,
                                    save_links_maps(Driver, ModelConfig, Key, NDoc, SLs, KeyNum + 1, CheckForOld)} of
                                    {{ok, AddedAcc}, {ok, NewlyAdded}} ->
                                        {ok, AddedAcc ++ NewlyAdded};
                                    {{ok, _}, NewError} ->
                                        NewError;
                                    {OldError, _} ->
                                        OldError
                                end
                            end, {ok, []}, SplitedLinks),
                            case FoldAns of
                                {ok, FoldlAddedLinks} ->
                                    {ok, FoldlAddedLinks ++ AnsExt};
                                _ ->
                                    FoldAns
                            end;
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
delete_links(Driver, #model_config{mother_link_scope = ScopeFun1, other_link_scopes = ScopeFun2} = ModelConfig, Key) ->
    Scopes = [ScopeFun1() | ScopeFun2()],
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
delete_links_from_maps(Driver, #model_config{mother_link_scope = ScopeFun1, other_link_scopes = ScopeFun2} = ModelConfig,
    Key, Links) ->
    % Try mother scope first for performance reasons
    case delete_links_from_maps(Driver, ModelConfig, Key, Links, ScopeFun1) of
        {ok, _, []} ->
            ok;
        {ok, _, LinksLeft} ->
            delete_links_from_maps(Driver, ModelConfig, Key, LinksLeft, ScopeFun2());
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
    [datastore:link_name()], Scopes :: atom() | [atom()] | links_utils:mother_scope_fun()) ->
    ok | {ok, integer(), [datastore:link_name()]} | datastore:generic_error().
delete_links_from_maps(_Driver, _ModelConfig, _Key, _Links, []) ->
    ok;
delete_links_from_maps(Driver, ModelConfig, Key, Links, [Scope | Scopes]) ->
    case delete_links_from_maps(Driver, ModelConfig, Key, Links, Scope) of
        {ok, _, []} ->
            ok;
        {ok, _, LinksLeft} ->
            delete_links_from_maps(Driver, ModelConfig, Key, LinksLeft, Scopes);
        Ans ->
            Ans
    end;
delete_links_from_maps(Driver, ModelConfig, Key, Links, Scope) ->
    delete_links_from_maps(Driver, ModelConfig, links_doc_key(Key, Scope), Links, 0, Key, 1,  #document{}, 0).

%%--------------------------------------------------------------------
%% @doc
%% Deletes links from all documents connected with key.
%% @end
%%--------------------------------------------------------------------
-spec delete_links_from_maps(Driver :: atom(), model_behaviour:model_config(), Key :: datastore:ext_key(),
    [datastore:link_name()], FreeSpaces :: integer(), MainDocKey :: datastore:ext_key(), KeyNum :: integer(),
    Parent :: datastore:ext_key() | datastore:document(), ParentNum :: integer()) ->
    {ok, integer(), [datastore:link_name()]} | datastore:generic_error().
delete_links_from_maps(_Driver, _ModelConfig, <<"non">>, Links, _FreeSpaces, _MainDocKey, _KeyNum, _Parent, _ParentNum) ->
    {ok, 0, Links};
delete_links_from_maps(Driver, ModelConfig, Key, Links, FreeSpaces, MainDocKey, KeyNum, Parent, ParentNum) ->
    case Driver:get_link_doc_inside_trans(ModelConfig, Key) of
        {ok, #document{value = #links{children = Children, link_map = LinkMap} = LinksRecord} = LinkDoc} ->
            {NewLinkMap, NewLinks, Deleted} = remove_from_links_map(Links, LinkMap),
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
                    case NewFreeSpaces >= ?LINKS_MAP_MAX_SIZE of
                        true ->
                            case rebuild_links_tree(Driver, ModelConfig, MainDocKey, NewLinkRef, Parent, ParentNum, Children) of
                                {ok, NumAns} ->
                                    {ok, NumAns, []};
                                OtherAns ->
                                    OtherAns
                            end;
                        _ ->
                            {ok, 0, []}
                    end;
                {{ok, _}, _} ->
                    SplitedLinks = split_links_names_list(NewLinks, KeyNum),
                    maps:fold(fun(Num, SLs, Acc) ->
                        case Acc of
                            {ok, UsedFreeSpaces, LinksLeft} ->
                                NextKey = maps:get(Num, Children, <<"non">>),
                                case delete_links_from_maps(Driver, ModelConfig, NextKey, SLs,
                                    NewFreeSpaces - UsedFreeSpaces, MainDocKey, KeyNum + 1, NewLinkRef, Num) of
                                    {ok, UsedFreeSpaces2, LinksLeft2} ->
                                        {ok, UsedFreeSpaces + UsedFreeSpaces2, LinksLeft ++ LinksLeft2};
                                    Other ->
                                        Other
                                end;
                            _ ->
                                Acc
                        end
                    end, {ok, 0, []}, SplitedLinks);
                Error ->
                    Error
            end;
        {error, {not_found, _}} ->
            {ok, 0, Links};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fetches link from set of documents connected with key.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(Driver :: atom(), model_behaviour:model_config(), datastore:ext_key(), datastore:link_name()) ->
    {ok, datastore:link_target()} | datastore:link_error().
fetch_link(Driver, #model_config{mother_link_scope = ScopeFun1, other_link_scopes = ScopeFun2} = ModelConfig,
    LinkName, Key) ->
    % Try mother scope first for performance reasons
    case fetch_link(Driver, ModelConfig, LinkName, Key, ScopeFun1) of
        {error, link_not_found} ->
            fetch_link(Driver, ModelConfig, LinkName, Key, ScopeFun2());
        Ans ->
            Ans
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fetches link from set of documents connected with key from chosen scopes.
%% @end
%%--------------------------------------------------------------------
-spec fetch_link(Driver :: atom(), model_behaviour:model_config(), datastore:ext_key(), datastore:link_name(),
    Scopes :: atom() | [atom()] | links_utils:mother_scope_fun()) ->
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
foreach_link(Driver, #model_config{mother_link_scope = ScopeFun1, other_link_scopes = ScopeFun2} = ModelConfig,
    Key, Fun, AccIn) ->
    Scopes = [ScopeFun1() | ScopeFun2()],
    foreach_link(Driver, ModelConfig, Key, Fun, {ok, AccIn}, Scopes).

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
-spec links_doc_key(Key :: datastore:key(), Scope :: atom() | mother_scope_fun()) -> BinKey :: binary().
links_doc_key(Key, Scope) ->
    Base = links_doc_key_from_scope(Key, Scope),
    case byte_size(Base) > 120 of
        true ->
            binary:part(Base, {0,120});
        _ ->
            Base
    end.

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
-spec split_links_names_list([datastore:link_name()], KeyNum :: integer()) -> map().
split_links_names_list(LinksList, KeyNum) ->
    lists:foldl(fun(LinkName, Acc) ->
        LinkNum = get_link_child_num(LinkName, KeyNum),
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
%% Deletes links updated at higher level of links tree.
%% @end
%%--------------------------------------------------------------------
-spec del_old_links(Driver :: atom(), model_behaviour:model_config(), [datastore:normalized_link_spec()],
    datastore:ext_key() | datastore:document(), KeyNum :: integer()) ->
    {ok, [datastore:normalized_link_spec()]} | datastore:generic_error().
del_old_links(_Driver, _ModelConfig, [], _Key, _KeyNum) ->
    {ok, []};
del_old_links(_Driver, #model_config{bucket = _Bucket} = _ModelConfig, Links, <<"non">>, _KeyNum) ->
    {ok, Links};
del_old_links(Driver, #model_config{bucket = _Bucket} = ModelConfig, Links,
    #document{value = #links{children = Children}}, KeyNum) ->
    SplitedLinks = split_links_names_list(Links, KeyNum),
    maps:fold(fun(Num, SLs, Acc) ->
        NextKey = maps:get(Num, Children, <<"non">>),
        case {Acc, del_old_links(Driver, ModelConfig, SLs, NextKey, KeyNum + 1)} of
            {{ok, NotDelAcc}, {ok, NotDel2}} ->
                {ok, NotDelAcc ++ NotDel2};
            {{ok, _}, NewError} ->
                NewError;
            {OldError, _} ->
                OldError
        end
    end, {ok, []}, SplitedLinks);
del_old_links(Driver, #model_config{bucket = _Bucket} = ModelConfig, Links, Key, KeyNum) ->
    case Driver:get_link_doc_inside_trans(ModelConfig, Key) of
        {ok, #document{value = #links{link_map = LinkMap} = LinksRecord} = LinkDoc} ->
            {NewLinkMap, NewLinks, Deleted} = remove_from_links_map(Links, LinkMap),
            SaveAns= case Deleted of
                         0 ->
                             {ok, ok};
                         _ ->
                             NLD = LinkDoc#document{value = LinksRecord#links{link_map = NewLinkMap}},
                             Driver:save_link_doc(ModelConfig, NLD)
                     end,

            case {SaveAns, NewLinks} of
                {{ok, _}, []} ->
                    {ok, []};
                {{ok, _}, _} ->
                    del_old_links(Driver, ModelConfig, NewLinks, LinkDoc, KeyNum);
                Error ->
                    Error
            end;
        {error, {not_found, _}} ->
            {ok, Links};
        {error, Reason} ->
            {error, Reason}
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
    case Driver:get_link_doc_inside_trans(ModelConfig, Key) of
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
%% Deletes links from all documents connected with key.
%% @end
%%--------------------------------------------------------------------
-spec rebuild_links_tree(Driver :: atom(), model_behaviour:model_config(), MainDocKey :: datastore:ext_key(),
    LinkDoc :: datastore:ext_key() | datastore:document(), Parent :: datastore:ext_key() | datastore:document(),
    ParentNum :: integer(), Children :: map()) ->
    {ok, integer()} | datastore:generic_error().
rebuild_links_tree(Driver, ModelConfig, MainDocKey, LinkDoc, Parent, ParentNum, Children) ->
    case maps:size(Children) of
        0 ->
            delete_leaf(Driver, ModelConfig, MainDocKey, LinkDoc, Parent, ParentNum);
        _ ->
            [{FirstChildNum, FirstChild} | _] = maps:to_list(Children),
            case Driver:get_link_doc_inside_trans(ModelConfig, FirstChild) of
                {ok, #document{value = #links{children = NewChildren}} = NewLinkDoc} ->
                    rebuild_links_tree(Driver, ModelConfig, MainDocKey, NewLinkDoc, LinkDoc, FirstChildNum, NewChildren);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Deletes leaf from documents' tree that stores links connected with key.
%% @end
%%--------------------------------------------------------------------
-spec delete_leaf(Driver :: atom(), model_behaviour:model_config(), MainDocKey :: datastore:ext_key(),
    LinkDoc :: datastore:ext_key() | datastore:document(), Parent :: datastore:ext_key() | datastore:document(),
    ParentNum :: integer()) ->
    {ok, integer()} | datastore:generic_error().
delete_leaf(Driver, #model_config{} = ModelConfig, MainDocKey,
    #document{value = #links{link_map = LinkMap}} = LinkDoc,
    #document{key = ParentKey} = Parent, ParentNum) ->
    case Driver:delete_link_doc(ModelConfig, LinkDoc) of
        ok ->
            case ParentKey of
                undefined ->
                    {ok, ok};
                _ ->
                    #document{value = #links{children = ParentChildren} = ParentLinks} = Parent,
                    NewParent = Parent#document{
                        value = ParentLinks#links{children = maps:remove(ParentNum, ParentChildren)}},
                    case Driver:save_link_doc(ModelConfig, NewParent) of
                        {ok, _} ->
                            case save_links_maps(Driver, ModelConfig, MainDocKey, maps:to_list(LinkMap)) of
                                ok ->
                                    {ok, maps:size(LinkMap)};
                                Other ->
                                    Other
                            end;
                        SaveParentError ->
                            SaveParentError
                    end
            end;
        DelError ->
            DelError
    end;
delete_leaf(Driver, ModelConfig, MainDocKey, #document{} = LinkDoc, Parent, ParentNum) ->
    case Driver:get_link_doc_inside_trans(ModelConfig, Parent) of
        {error, Reason} ->
            {error, Reason};
        {ok, Doc} ->
            delete_leaf(Driver, ModelConfig, MainDocKey, LinkDoc, Doc, ParentNum)
    end;
delete_leaf(Driver, ModelConfig, MainDocKey, LinkDoc, Parent, ParentNum) ->
    case Driver:get_link_doc_inside_trans(ModelConfig, LinkDoc) of
        {error, Reason} ->
            {error, Reason};
        {ok, Doc} ->
            delete_leaf(Driver, ModelConfig, MainDocKey, Doc, Parent, ParentNum)
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
                LinkTarget ->
                    {ok, LinkTarget}
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
            NewAccIn = maps:fold(Fun, AccIn, LinkMap),
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
-spec links_doc_key_from_scope(Key :: datastore:key(), Scope :: atom() | mother_scope_fun()) -> BinKey :: binary().
links_doc_key_from_scope(Key, ScopeFun) when is_function(ScopeFun) ->
    links_doc_key_from_scope(Key, ScopeFun());
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
    BinNum = base64:encode(list_to_binary("_" ++ integer_to_list(Num))),
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
add_non_existing_to_map(Driver, #model_config{bucket = _Bucket, name = ModelName} = ModelConfig, Key, LDK, GetAns, Link, 1) ->
    TmpAns = case GetAns of
        #document{} = Doc ->
            save_links_maps(Driver, ModelConfig, Key, Doc, [Link], 1, false);
        {error, {not_found, _}} ->
            LinksDoc = #document{key = LDK, value = #links{doc_key = Key, model = ModelName}},
            save_links_maps(Driver, ModelConfig, Key, LinksDoc, [Link], 1, false);
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