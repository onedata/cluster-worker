%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% TODO
%%% @end
%%%-------------------------------------------------------------------
-module(links_utils).
-author("Michal Wrzeszcz").

-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([save_links_maps/4, delete_links/3, delete_links_from_maps/4, fetch_link/4, foreach_link/5]).

get_link_child_num(LinkName, KeyNum) when is_binary(LinkName) ->
    get_link_child_num(LinkName, KeyNum, byte_size(LinkName));
get_link_child_num(LinkName, KeyNum) ->
    get_link_child_num(term_to_binary(LinkName), KeyNum).

get_link_child_num(LinkName, KeyNum, ByteSize) when KeyNum > ByteSize ->
    get_link_child_num(binary:at(LinkName, 0));
get_link_child_num(LinkName, KeyNum, ByteSize) ->
    get_link_child_num(binary:at(LinkName, ByteSize - KeyNum)).

get_link_child_num(Byte) ->
    Byte rem ?LINKS_TREE_BASE.

split_links_list(LinksList, KeyNum) ->
    lists:foldl(fun({LinkName, _} = Link, Acc) ->
        LinkNum = get_link_child_num(LinkName, KeyNum),
        TmpAns = maps:get(LinkNum, Acc, []),
        maps:put(LinkNum, [Link | TmpAns], Acc)
    end, #{}, LinksList).

split_links_names_list(LinksList, KeyNum) ->
    lists:foldl(fun(LinkName, Acc) ->
        LinkNum = get_link_child_num(LinkName, KeyNum),
        TmpAns = maps:get(LinkNum, Acc, []),
        maps:put(LinkNum, [LinkName | TmpAns], Acc)
    end, #{}, LinksList).

save_links_maps(Driver, #model_config{bucket = _Bucket, name = ModelName} = ModelConfig, Key, LinksList) ->
    LDK = links_doc_key(Key),
    case Driver:get_link_doc_inside_trans(ModelConfig, LDK) of
        {ok, LinksDoc} ->
            save_links_maps(Driver, ModelConfig, Key, LinksDoc, LinksList, 1);
        {error, {not_found, _}} ->
            LinksDoc = #document{key = LDK, value = #links{doc_key = Key, model = ModelName}},
            save_links_maps(Driver, ModelConfig, Key, LinksDoc, LinksList, 1);
        {error, Reason} ->
            {error, Reason}
    end.

save_links_maps(Driver, #model_config{bucket = _Bucket, name = ModelName} = ModelConfig, Key,
    #document{key = LDK, value = #links{link_map = LinkMap, children = Children} = LinksRecord} = LinksDoc,
    LinksList, KeyNum) ->
    {FilledMap, NewLinksList, AddedLinks} = fill_links_map(LinksList, LinkMap),
    case NewLinksList of
        [] ->
            case Driver:save_link_doc(ModelConfig, LinksDoc#document{value = LinksRecord#links{link_map = FilledMap}}) of
                {ok, _} ->
                    del_old_links(Driver, ModelConfig, AddedLinks, LinksDoc, KeyNum);
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
                    case del_old_links(Driver, ModelConfig, AddedLinks, LinksDoc, KeyNum) of
                        ok ->
                            maps:fold(fun(Num, SLs, FunAns) ->
                                NDoc = maps:get(Num, ChildrenDocs),
                                case FunAns of
                                    ok ->
                                        save_links_maps(Driver, ModelConfig, Key, NDoc, SLs, KeyNum + 1);
                                    OldError ->
                                        OldError
                                end
                            end, ok, SplitedLinks);
                        Other ->
                            Other
                    end;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

fill_links_map(LinksList, LinkMap) ->
    MapSize = maps:size(LinkMap),
    fill_links_map(LinksList, LinkMap, MapSize, [], []).

fill_links_map([], Map, _MapSize, AddedLinks, LinksToAdd) ->
    {Map, LinksToAdd, AddedLinks};
fill_links_map([{LinkName, LinkTarget} = Link | R], Map, MapSize, AddedLinks, LinksToAdd) when MapSize >= ?LINKS_MAP_MAX_SIZE ->
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

del_old_links(_Driver, _ModelConfig, [], _Key, _KeyNum) ->
    ok;
del_old_links(_Driver, #model_config{bucket = _Bucket} = _ModelConfig, _Links, <<"non">>, _KeyNum) ->
    ok;
del_old_links(Driver, #model_config{bucket = _Bucket} = ModelConfig, Links,
    #document{value = #links{children = Children}}, KeyNum) ->
    SplitedLinks = split_links_names_list(Links, KeyNum),
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




delete_links(Driver, ModelConfig, Key) ->
    delete_links_docs(Driver, ModelConfig, links_doc_key(Key)).

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

delete_links_from_maps(Driver, ModelConfig, Key, Links) ->
    case delete_links_from_maps(Driver, ModelConfig, links_doc_key(Key), Links, 0, Key, 1,  #document{}, 0) of
        {ok, _} ->
            ok;
        Other ->
            Other
    end.

delete_links_from_maps(_Driver, _ModelConfig, <<"non">>, _Links, _FreeSpaces, _MainDocKey, _KeyNum, _Parent, _ParentNum) ->
    {ok, 0};
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
                            rebuild_links_tree(Driver, ModelConfig, MainDocKey, NewLinkRef, Parent, ParentNum, Children);
                        _ ->
                            {ok, 0}
                    end;
                {{ok, _}, _} ->
                    SplitedLinks = split_links_names_list(NewLinks, KeyNum),
                    maps:fold(fun(Num, SLs, Acc) ->
                        case Acc of
                            {ok, UsedFreeSpaces} ->
                                NextKey = maps:get(Num, Children, <<"non">>),
                                case delete_links_from_maps(Driver, ModelConfig, NextKey, SLs,
                                    NewFreeSpaces - UsedFreeSpaces, MainDocKey, KeyNum + 1, NewLinkRef, Num) of
                                    {ok, UsedFreeSpaces2} ->
                                        {ok, UsedFreeSpaces + UsedFreeSpaces2};
                                    Other ->
                                        Other
                                end;
                            _ ->
                                Acc
                        end
                    end, {ok, 0}, SplitedLinks);
                Error ->
                    Error
            end;
        {error, {not_found, _}} ->
            {ok, 0};
        {error, Reason} ->
            {error, Reason}
    end.

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

remove_from_links_map(Links, Map) ->
    remove_from_links_map(Links, Map, [], 0).

remove_from_links_map([], Map, NewLinks, Deleted) ->
    {Map, NewLinks, Deleted};
remove_from_links_map([Link | R], Map, NewLinks, Deleted) ->
    case maps:is_key(Link, Map) of
        true ->
            remove_from_links_map(R, maps:remove(Link, Map), NewLinks, Deleted + 1);
        _ ->
            remove_from_links_map(R, Map, [Link | NewLinks], Deleted)
    end.




fetch_link(Driver, ModelConfig, LinkName, Key) ->
    fetch_link_from_docs(Driver, ModelConfig, LinkName, links_doc_key(Key), 1).

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




foreach_link(Driver, ModelConfig, Key, Fun, AccIn) ->
    foreach_link_in_docs(Driver, ModelConfig, links_doc_key(Key), Fun, AccIn).

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
%% Returns key for document holding links for given document.
%% @end
%%--------------------------------------------------------------------
-spec links_doc_key(Key :: datastore:key()) -> BinKey :: binary().
links_doc_key(Key) ->
    base64:encode(term_to_binary({"links", Key})).

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