%%%-------------------------------------------------------------------
%%% @author Michal Stanisz, Lukasz Opiola
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common datastore model definition of infinite log with JSON entries.
%%% Operates on indices in binary format, rather than integer as it is done
%%% originally in the infinite_log.
%%% @end
%%%-------------------------------------------------------------------
-module(json_infinite_log_model).
-author("Michal Stanisz").
-author("Lukasz Opiola").

-include("modules/datastore/infinite_log.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([create/1, create/2]).
-export([destroy/1]).
-export([append/2]).
-export([default_start_index/1]).
-export([list_and_postprocess/3]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: datastore_infinite_log:key().
-type entry_index() :: binary().
-type entry_content() :: json_utils:json_map().
-type entry() :: {entry_index(), {infinite_log:timestamp_millis(), entry_content()}}.
% log entry in a format suitable for external APIs (ready to be encoded to json)
% #{
%     <<"isLast">> => boolean(),
%     <<"logEntries">> => [
%         #{
%             <<"index">> => entry_index(),
%             <<"timestamp">> => infinite_log:timestamp(),
%             <<"content">> => json_utils:json_map()
%         }
%     ]
% }
-type browse_result() :: json_utils:json_map().

% Redefinition of infinite log browser listing opts to allow binary indices
% and listing exclusively from an index.
% @formatter:off
-type listing_opts() :: #{
    direction => infinite_log_browser:direction(),
    start_from => undefined |
                  {index, entry_index()} |  % behaves as in infinite_log
                  {index_exclusive, entry_index()} |  % starts from a subsequent entry
                  {timestamp, infinite_log:timestamp_millis()},
    offset => infinite_log_browser:offset(),
    limit => infinite_log_browser:limit()
}.
% @formatter:on

% Mapping function that will be applied to each listed infinite log
% entry before the result is returned.
-type listing_postprocessor(MappedEntry) :: fun((entry()) -> MappedEntry).

-export_type([id/0, entry_index/0, entry/0, browse_result/0, listing_opts/0, listing_postprocessor/1]).

-define(MAX_LOG_LIST_LIMIT, 1000).

-define(CTX, #{model => ?MODULE}).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(infinite_log:log_opts()) -> {ok, id()} | {error, term()}.
create(Opts) ->
    Id = datastore_key:new(),
    case create(Id, Opts) of
        ok ->
            {ok, Id};
        {error, _} = Error ->
            Error
    end.


-spec create(id(), infinite_log:log_opts()) -> ok | {error, term()}.
create(Id, Opts) ->
    datastore_infinite_log:create(?CTX, Id, Opts).


-spec destroy(id()) -> ok | {error, term()}.
destroy(Id) ->
    datastore_infinite_log:destroy(?CTX, Id).


-spec append(id(), entry_content()) -> ok | {error, term()}.
append(Id, EntryValue) ->
    datastore_infinite_log:append(?CTX, Id, json_utils:encode(EntryValue)).


-spec default_start_index(exclusive | inclusive) -> entry_index().
default_start_index(exclusive) -> <<"-1">>;
default_start_index(inclusive) -> <<"0">>.


-spec list_and_postprocess(id(), listing_opts(), listing_postprocessor(MappedEntry)) ->
    {ok, {infinite_log_browser:progress_marker(), [MappedEntry]}} | {error, term()}.
list_and_postprocess(Id, Opts, ListingPostprocessor) ->
    case datastore_infinite_log:list(?CTX, Id, prepare_listing_opts(Opts)) of
        {ok, {ProgressMarker, EntrySeries}} ->
            {ok, {ProgressMarker, postprocess_entries(EntrySeries, ListingPostprocessor)}};
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec prepare_listing_opts(listing_opts()) -> infinite_log_browser:listing_opts().
prepare_listing_opts(Opts) when not is_map_key(start_from, Opts) ->
    Opts;
prepare_listing_opts(Opts) ->
    maps:update_with(start_from, fun
        ({index, IndexBinary}) -> {index, binary_to_integer(IndexBinary)};
        ({index_exclusive, IndexBinary}) -> {index, binary_to_integer(IndexBinary) + 1};
        (Other) -> Other
    end, Opts).


%% @private
-spec postprocess_entries(
    [{infinite_log:entry_index(), infinite_log:entry()}],
    listing_postprocessor(MappedEntry)
) ->
    [MappedEntry].
postprocess_entries(EntrySeries, ListingPostprocessor) ->
    lists:map(fun({Index, {Timestamp, EncodedContent}}) ->
        IndexBin = integer_to_binary(Index),
        EntryContent = json_utils:decode(EncodedContent),
        ListingPostprocessor({IndexBin, {Timestamp, EntryContent}})
    end, EntrySeries).
