%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common backend for audit log implementations with well defined API.
%%% All audit logs in the system have mandatory thresholds set for
%%% size pruning, age pruning and expiry. This means that each audit log
%%% is expected to be deleted at some point. For that reason, all append
%%% operations internally recreate the audit log as needed. Consequently,
%%% it is not mandatory to create the audit log before the first append
%%% is to be done.
%%% @end
%%%-------------------------------------------------------------------
-module(audit_log).
-author("Bartosz Walkowicz").

-include("audit_log.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% CRUD API
-export([ensure_created/2, delete/1]).
-export([normalize_severity/1]).
-export([append/3, browse/2]).
%% Iterator API
-export([new_iterator/0, next_batch/3]).


-type id() :: json_infinite_log_model:id().

-type entry_source() :: binary().    %% ?SYSTEM_AUDIT_LOG_ENTRY_SOURCE | ?USER_AUDIT_LOG_ENTRY_SOURCE
-type entry_severity() :: binary().  %% see ?AUDIT_LOG_SEVERITY_LEVELS

%% entry() typespec (impossible to define in pure erlang due to binary keys):
%% #{
%%     <<"index">> := audit_log_browse_opts:index(),
%%     <<"timestamp">> := audit_log_browse_opts:timestamp_millis(),
%%     <<"source">> => entry_source()  // [default: system]
%%     <<"severity">> := entry_severity(),
%%     <<"content">> := json_utils:json_map()
%% }
-type entry() :: json_utils:json_map().

-type append_request() :: #audit_log_append_request{}.

%% browse_result() typespec (impossible to define in pure erlang due to binary keys):
%% #{
%%     <<"isLast">> := boolean(),
%%     <<"logEntries">> := [entry()]
%% }
-type browse_result() :: json_utils:json_map().

-opaque iterator() :: audit_log_browse_opts:index().

-export_type([id/0]).
-export_type([entry_source/0, entry_severity/0, entry/0]).
-export_type([append_request/0, browse_result/0]).
-export_type([iterator/0]).

-type threshold_key() :: size_pruning_threshold | age_pruning_threshold | expiry_threshold.

% maximum thresholds that can be specified for an audit log;
% if larger values are provided, they are lowered to these boundaries
-define(MAX_SIZE_PRUNING_THRESHOLD, cluster_worker:get_env(audit_log_max_size_pruning_threshold, 500000)).
-define(MAX_AGE_PRUNING_THRESHOLD, cluster_worker:get_env(audit_log_max_age_pruning_threshold_seconds, 5184000)). % 60 days
-define(MAX_EXPIRY_THRESHOLD, cluster_worker:get_env(audit_log_max_expiry_threshold_seconds, 7776000)). % 90 days
% default thresholds used if not provided
-define(DEFAULT_SIZE_PRUNING_THRESHOLD, cluster_worker:get_env(audit_log_default_size_pruning_threshold, 5000)).
-define(DEFAULT_AGE_PRUNING_THRESHOLD, cluster_worker:get_env(audit_log_default_age_pruning_threshold_seconds, 1209600)). % 14 days
-define(DEFAULT_EXPIRY_THRESHOLD, cluster_worker:get_env(audit_log_default_expiry_threshold_seconds, 2592000)). % 30 days


%%%===================================================================
%%% CRUD API
%%%===================================================================


-spec ensure_created(id(), infinite_log:log_opts()) -> ok | {error, term()}.
ensure_created(Id, Opts) ->
    case json_infinite_log_model:create(Id, sanitize_opts(Opts)) of
        ok -> ok;
        {error, already_exists} -> ok;
        {error, _} = Error -> Error
    end.


-spec delete(id()) -> ok | {error, term()}.
delete(Id) ->
    json_infinite_log_model:destroy(Id).


-spec normalize_severity(binary()) -> entry_severity().
normalize_severity(ProvidedSeverity) ->
    case lists:member(ProvidedSeverity, ?AUDIT_LOG_SEVERITY_LEVELS) of
        true -> ProvidedSeverity;
        false -> ?INFO_AUDIT_LOG_SEVERITY
    end.


-spec append(id(), infinite_log:log_opts(), append_request()) -> ok | {error, term()}.
append(Id, RecreateOpts, #audit_log_append_request{
    severity = Severity,
    source = Source,
    content = Content
} = AppendRequest) ->
    EntryValue = maps_utils:put_if_defined(#{
        <<"severity">> => Severity,
        <<"content">> => Content
    }, <<"source">>, Source, ?SYSTEM_AUDIT_LOG_ENTRY_SOURCE),
    case json_infinite_log_model:append(Id, EntryValue) of
        ok ->
            ok;
        {error, not_found} ->
            ensure_created(Id, RecreateOpts),
            append(Id, RecreateOpts, AppendRequest);
        {error, _} = Error ->
            Error
    end.


-spec browse(id(), audit_log_browse_opts:opts()) ->
    {ok, browse_result()} | errors:error().
browse(Id, BrowseOpts) ->
    case json_infinite_log_model:list_and_postprocess(Id, BrowseOpts, fun listing_postprocessor/1) of
        {ok, {ProgressMarker, EntrySeries}} ->
            {ok, #{
                <<"logEntries">> => EntrySeries,
                <<"isLast">> => ProgressMarker =:= done
            }};
        {error, not_found} ->
            ?ERROR_NOT_FOUND;
        {error, _} = Error ->
            ?report_internal_server_error("returned error: ~p", [Error])
    end.


%%%===================================================================
%%% Iterator API
%%%===================================================================


-spec new_iterator() -> iterator().
new_iterator() ->
    json_infinite_log_model:default_start_index(exclusive).


-spec next_batch(pos_integer(), id(), iterator()) ->
    {ok, [entry()], iterator()} | stop | {error, term()}.
next_batch(BatchSize, Id, LastListedIndex) ->
    Opts = #{start_from => {index_exclusive, LastListedIndex}, limit => BatchSize},
    case json_infinite_log_model:list_and_postprocess(Id, Opts, fun listing_postprocessor/1) of
        {ok, {ProgressMarker, Entries}} ->
            case {Entries, ProgressMarker} of
                {[], done} -> stop;
                _ -> {ok, Entries, maps:get(<<"index">>, lists:last(Entries))}
            end;
        {error, not_found} ->
            ?ERROR_NOT_FOUND;
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec listing_postprocessor(json_infinite_log_model:entry()) -> entry().
listing_postprocessor({IndexBin, {Timestamp, Entry}}) ->
    Entry#{
        <<"index">> => IndexBin,
        <<"timestamp">> => Timestamp
    }.


%% @private
-spec sanitize_opts(infinite_log:log_opts()) -> infinite_log:log_opts().
sanitize_opts(Opts) ->
    Opts#{
        size_pruning_threshold => acquire_sanitized_threshold(size_pruning_threshold, Opts),
        age_pruning_threshold => acquire_sanitized_threshold(age_pruning_threshold, Opts),
        expiry_threshold => acquire_sanitized_threshold(expiry_threshold, Opts)
    }.


%% @private
-spec acquire_sanitized_threshold(threshold_key(), infinite_log:log_opts()) -> non_neg_integer().
acquire_sanitized_threshold(Key, Opts) ->
    RequestedThreshold = maps:get(Key, Opts, default_threshold(Key)),
    MaxThreshold = max_threshold(Key),
    case RequestedThreshold > MaxThreshold of
        true ->
            ?warning(
                "Requested an audit log with ~s of ~B, which is larger than allowed maximum (~B), "
                "using the max value instead",
                [Key, RequestedThreshold, MaxThreshold]
            ),
            MaxThreshold;
        false ->
            RequestedThreshold
    end.


%% @private
-spec default_threshold(threshold_key()) -> non_neg_integer().
default_threshold(size_pruning_threshold) -> ?DEFAULT_SIZE_PRUNING_THRESHOLD;
default_threshold(age_pruning_threshold) -> ?DEFAULT_AGE_PRUNING_THRESHOLD;
default_threshold(expiry_threshold) -> ?DEFAULT_EXPIRY_THRESHOLD.


%% @private
-spec max_threshold(threshold_key()) -> non_neg_integer().
max_threshold(size_pruning_threshold) -> ?MAX_SIZE_PRUNING_THRESHOLD;
max_threshold(age_pruning_threshold) -> ?MAX_AGE_PRUNING_THRESHOLD;
max_threshold(expiry_threshold) -> ?MAX_EXPIRY_THRESHOLD.
