%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides CouchBase views management functions.
%%% They should be used only by CouchBase pool workers.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_view).
-author("Krzysztof Trzepla").

-include("global_definitions.hrl").

%% API
-export([save_design_doc/3, get_design_doc/2, delete_design_doc/2]).
-export([query/4]).

-define(TIMEOUT, application:get_env(?CLUSTER_WORKER_APP_NAME,
    couchbase_view_timeout, 120000)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves design document in a database.
%% @end
%%--------------------------------------------------------------------
-spec save_design_doc(cberl:connection(), couchbase_driver:design(),
    datastore_json:ejson()) -> ok | {error, term()}.
save_design_doc(Connection, DesignName, EJson) ->
    Path = <<"_design/", DesignName/binary>>,
    Body = json_utils:encode(EJson),
    ContentType = <<"application/json">>,
    Ans = parse_design_doc_response(save,
        cberl:http(Connection, view, put, Path, ContentType, Body, ?TIMEOUT)
    ),

    case Ans of
        ok ->
            Views = get_views(EJson),
            test_views(Connection, DesignName, Views, 5, 0);
        _ ->
            Ans
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves design document from a database.
%% @end
%%--------------------------------------------------------------------
-spec get_design_doc(cberl:connection(), couchbase_driver:design()) ->
    {ok, datastore_json:ejson()} | {error, term()}.
get_design_doc(Connection, DesignName) ->
    Path = <<"_design/", DesignName/binary>>,
    parse_design_doc_response(get,
        cberl:http(Connection, view, get, Path, <<>>, <<>>, ?TIMEOUT)
    ).

%%--------------------------------------------------------------------
%% @doc
%% Removes design document from a database.
%% @end
%%--------------------------------------------------------------------
-spec delete_design_doc(cberl:connection(), couchbase_driver:design()) ->
    ok | {error, term()}.
delete_design_doc(Connection, DesignName) ->
    Path = <<"_design/", DesignName/binary>>,
    parse_design_doc_response(delete,
        cberl:http(Connection, view, delete, Path, <<>>, <<>>, ?TIMEOUT)
    ).

%%--------------------------------------------------------------------
%% @doc
%% Returns view response from a database.
%% @end
%%--------------------------------------------------------------------
-spec query(cberl:connection(), couchbase_driver:design(),
    couchbase_driver:view(), [couchbase_driver:view_opt()]) ->
    {ok, json_utils:json_map()} | {error, term()}.
query(Connection, DesignName, ViewName, Opts) ->
    Type = case proplists:get_value(spatial, Opts, false) of
        true -> <<"_spatial">>;
        false -> <<"_view">>
    end,
    Opts2 = proplists:delete(spatial, Opts),
    Path = <<"_design/", DesignName/binary, "/", Type/binary, "/",
        ViewName/binary, (get_query_params(Opts2))/binary>>,
    ContentType = <<"application/json">>,
    case cberl:http(Connection, view, get, Path, ContentType, <<>>, ?TIMEOUT) of
        {ok, _Code, Response} ->
            case json_utils:decode(Response) of
                #{<<"error">> := Error, <<"reason">> := Reason} ->
                    {error, {Error, Reason}};
                DecodedResult ->
                    {ok, DecodedResult}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Parses add/delete design document response.
%% @end
%%--------------------------------------------------------------------
-spec parse_design_doc_response(Method :: atom(),
    {ok, cberl:http_status(), cberl:http_body()} | {error, term()}) ->
    ok | {ok, datastore_json:ejson()} | {error, term()}.
parse_design_doc_response(get, {ok, Code, Response})
    when 200 =< Code andalso Code < 300 ->
    {ok, jiffy:decode(Response)};
parse_design_doc_response(_Method, {ok, Code, _Response})
    when 200 =< Code andalso Code < 300 ->
    ok;
parse_design_doc_response(_Method, {ok, _Code, Response}) ->
    case jiffy:decode(Response) of
        {[{<<"error">>, Error}, {<<"reason">>, Reason}]} ->
            {error, {Error, Reason}};
        _ ->
            {error, {unknown_error, Response}}
    end;
parse_design_doc_response(_Method, {error, Reason}) ->
    {error, Reason}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts view arguments to query parameters.
%% @end
%%--------------------------------------------------------------------
-spec get_query_params([couchbase_driver:view_opt()]) -> binary().
get_query_params([]) ->
    <<>>;
get_query_params([Opt | Opts]) ->
    lists:foldl(fun(Opt2, Prefix) ->
        <<Prefix/binary, "&", (get_query_param(Opt2))/binary>>
    end, <<"?", (get_query_param(Opt))/binary>>, Opts).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Converts view argument to query parameter.
%% @end
%%--------------------------------------------------------------------
-spec get_query_param(couchbase_driver:view_opt()) -> binary().
get_query_param({descending, true}) -> <<"descending=true">>;
get_query_param({descending, false}) -> <<"descending=false">>;
get_query_param({endkey, Key}) -> <<"endkey=", (json_utils:encode(Key))/binary>>;
get_query_param({endkey_docid, Id}) -> <<"endkey_docid=", Id/binary>>;
get_query_param({full_set, true}) -> <<"full_set=true">>;
get_query_param({full_set, false}) -> <<"full_set=false">>;
get_query_param({inclusive_end, true}) -> <<"inclusive_end=true">>;
get_query_param({inclusive_end, false}) -> <<"inclusive_end=false">>;
get_query_param({key, Key}) -> <<"key=", (json_utils:encode(Key))/binary>>;
get_query_param({keys, Keys}) -> <<"keys=", (json_utils:encode(Keys))/binary>>;
get_query_param({limit, L}) -> <<"limit=", (integer_to_binary(L))/binary>>;
get_query_param({on_error, continue}) -> <<"on_error=continue">>;
get_query_param({on_error, stop}) -> <<"on_error=stop">>;
get_query_param({reduce, true}) -> <<"reduce=true">>;
get_query_param({reduce, false}) -> <<"reduce=false">>;
get_query_param({skip, Skip}) -> <<"skip=", (integer_to_binary(Skip))/binary>>;
get_query_param({stale, ok}) -> <<"stale=ok">>;
get_query_param({stale, false}) -> <<"stale=false">>;
get_query_param({stale, update_after}) -> <<"stale=update_after">>;
get_query_param({startkey, Key}) -> <<"startkey=", (json_utils:encode(Key))/binary>>;
get_query_param({startkey_docid, Id}) -> <<"startkey_docid=", Id/binary>>;
get_query_param({bbox, BBox}) -> <<"bbox=", BBox/binary>>;
get_query_param({start_range, Range}) ->
    <<"start_range=", (json_utils:encode(Range))/binary>>;
get_query_param({end_range, Range}) ->
    <<"end_range=", (json_utils:encode(Range))/binary>>;
get_query_param({group, true}) -> <<"group=true">>;
get_query_param({group, false}) -> <<"group=false">>;
get_query_param({group_level, Level}) ->
    <<"group_level=", (integer_to_binary(Level))/binary>>.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Get views list from ejson.
%% @end
%%--------------------------------------------------------------------
-spec get_views(term()) -> list().
get_views([Element | Tail]) ->
    get_views(Element) ++ get_views(Tail);
get_views({Element}) ->
    get_views(Element);
get_views({<<"views">>, {Views}}) ->
    lists:map(fun({Name, _}) -> {Name, false} end, Views);
get_views({<<"spatial">>, {Views}}) ->
    lists:map(fun({Name, _}) -> {Name, true} end, Views);
get_views(_) ->
    [].

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Tests if all views can be queried.
%% @end
%%--------------------------------------------------------------------
-spec test_views(cberl:connection(), couchbase_driver:design(),
    list(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
test_views(_Connection, _DesignName, [], _Repeats, _SleepTime) ->
    ok;
test_views(_Connection, _DesignName, _Views, 0, _SleepTime) ->
    {error, cannot_query_views};
test_views(Connection, DesignName, Views, Repeats, SleepTime) ->
    timer:sleep(SleepTime),
    Views2 = lists:foldl(fun({Name, Spatial} = View, Acc) ->
        case query(Connection, DesignName, Name, [
            {stale, update_after}, {key, <<"key">>}, {spatial, Spatial}]) of
            {error, {_, <<"view_undefined">>}} -> [View | Acc];
            _ -> Acc
        end
    end, [], Views),
    test_views(Connection, DesignName, Views2, Repeats - 1, SleepTime * 2 + 100).