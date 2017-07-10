%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an interface for CouchBase changes management
%%% and streaming.
%%% @end
%%%-------------------------------------------------------------------
-module(couchbase_changes).
-author("Krzysztof Trzepla").

%% API
-export([enable/1, start/3, stop/2]).
-export([stream/3, stream/4, cancel_stream/1]).
-export([design/0, view/0]).
-export([get_seq_key/1, get_seq_safe_key/1, get_change_key/2]).

-type callback() :: fun(({ok, [datastore:document()] | end_of_stream}
                    | {error, since(), Reason :: term()}) -> any()).
-type seq() :: non_neg_integer().
-type since() :: seq().
-type until() :: seq() | infinity.
-type change() :: proplists:proplist().
-type option() :: {since, non_neg_integer()} |
                  {until, non_neg_integer() | infinity} |
                  {except_mutator, datastore:mutator()}.

-export_type([callback/0, seq/0, since/0, until/0, change/0, option/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Enables CouchBase documents changes generation.
%% @end
%%--------------------------------------------------------------------
-spec enable([couchbase_config:bucket()]) -> ok.
enable(Buckets) ->
    EJson = {[{<<"views">>,
        {[{view(),
            {[{<<"map">>,
                <<"function (doc, meta) {\r\n"
                "  if(doc._scope == undefined || doc._rev == undefined ||\r\n"
                "     doc._seq == undefined || doc._mutator == undefined)\r\n"
                "    return;\r\n"
                "  emit([doc._scope, doc._seq], {\r\n"
                "    \"_rev\": doc._rev[0], \r\n"
                "    \"_mutator\": doc._mutator[0]\r\n"
                "  })\r\n"
                "}\r\n">>
            }]}
        }]}
    }]},
    lists:foreach(fun(Bucket) ->
        Ctx = #{bucket => Bucket},
        case couchbase_driver:get_design_doc(Ctx, design()) of
            {ok, EJson} -> ok;
            _ -> ok = couchbase_driver:save_design_doc(Ctx, design(), EJson)
        end
    end, Buckets).

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase changes worker.
%% @end
%%--------------------------------------------------------------------
-spec start(couchbase_config:bucket(), datastore:scope(), pid()) ->
    {ok, pid()} | {error, Reason :: term()}.
start(Bucket, Scope, GC_Pid) ->
    couchbase_changes_sup:start_worker(Bucket, Scope, GC_Pid).

%%--------------------------------------------------------------------
%% @doc
%% Stops CouchBase changes worker.
%% @end
%%--------------------------------------------------------------------
-spec stop(couchbase_config:bucket(), datastore:scope()) ->
    ok | {error, Reason :: term()}.
stop(Bucket, Scope) ->
    couchbase_changes_sup:stop_worker(Bucket, Scope).

%%--------------------------------------------------------------------
%% @equiv stream(Bucket, Scope, Callback, [])
%% @end
%%--------------------------------------------------------------------
-spec stream(couchbase_config:bucket(), datastore:scope(), callback()) ->
    {ok, pid()} | {error, Reason :: term()}.
stream(Bucket, Scope, Callback) ->
    stream(Bucket, Scope, Callback, []).

%%--------------------------------------------------------------------
%% @doc
%% Starts CouchBase changes stream.
%% Following options are available:
%% - {since, non_neg_integer()}
%% - {until, non_neg_integer() | infinity} (exclusive)
%% - {except_mutator, datastore:mutator()}
%% @end
%%--------------------------------------------------------------------
-spec stream(couchbase_config:bucket(), datastore:scope(), callback(),
    [option()]) -> {ok, pid()} | {error, Reason :: term()}.
stream(Bucket, Scope, Callback, Opts) ->
    couchbase_changes_stream_sup:start_worker(Bucket, Scope, Callback, Opts).

%%--------------------------------------------------------------------
%% @doc
%% Stops CouchBase changes stream.
%% @end
%%--------------------------------------------------------------------
-spec cancel_stream(pid()) -> ok | {error, Reason :: term()}.
cancel_stream(Pid) ->
    couchbase_changes_stream_sup:stop_worker(Pid).

%%--------------------------------------------------------------------
%% @doc
%% Returns name of changes design document.
%% @end
%%--------------------------------------------------------------------
-spec design() -> couchbase_driver:design().
design() ->
    <<"onedata">>.

%%--------------------------------------------------------------------
%% @doc
%% Returns name of changes view name.
%% @end
%%--------------------------------------------------------------------
-spec view() -> couchbase_driver:view().
view() ->
    <<"changes">>.

%%--------------------------------------------------------------------
%% @doc
%% Returns key of document holding sequence number counter associated with 
%% provided scope.
%% @end
%%--------------------------------------------------------------------
-spec get_seq_key(datastore:scope()) -> datastore:key().
get_seq_key(Scope) ->
    <<"seq:", Scope/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Returns key of document holding safe sequence number counter associated with 
%% provided scope.
%% @end
%%--------------------------------------------------------------------
-spec get_seq_safe_key(datastore:scope()) -> datastore:key().
get_seq_safe_key(Scope) ->
    <<"seq_safe:", Scope/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Returns key of document holding reference to a document associated with 
%% provided scope and sequence number.
%% @end
%%--------------------------------------------------------------------
-spec get_change_key(datastore:scope(), seq()) -> datastore:key().
get_change_key(Scope, Seq) ->
    <<(get_seq_key(Scope))/binary, ":", (integer_to_binary(Seq))/binary>>.
