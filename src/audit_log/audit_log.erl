%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2022 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common backend for audit log implementations with well defined API,
%%% using infinite_log behind the scenes.
%%%
%%% All audit logs in the system have mandatory thresholds set for
%%% size pruning, age pruning and expiry. This means that each audit log
%%% is expected to be deleted at some point. For that reason, all append
%%% operations internally (re)create the audit log as needed. Consequently,
%%% audit logs are not created unless the first append is done.
%%% @end
%%%-------------------------------------------------------------------
-module(audit_log).
-author("Bartosz Walkowicz").

-include("audit_log.hrl").
-include_lib("ctool/include/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% CRUD API
-export([normalize_severity/1]).
-export([severity_to_int/1, severity_from_int/1]).
-export([should_log/2]).
-export([append/3, browse/2]).
-export([delete/1]).
%% Iterator API
-export([new_iterator/0, next_batch/3]).


-type id() :: json_infinite_log_model:id().

-type entry_source() :: binary().    %% ?SYSTEM_AUDIT_LOG_ENTRY_SOURCE | ?USER_AUDIT_LOG_ENTRY_SOURCE
-type entry_severity() :: binary().  %% see ?AUDIT_LOG_SEVERITY_LEVELS
-type entry_severity_int() :: 0..7.  %% see ?AUDIT_LOG_SEVERITY_LEVELS

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
-export_type([entry_source/0, entry_severity/0, entry_severity_int/0, entry/0]).
-export_type([append_request/0, browse_result/0]).
-export_type([iterator/0]).

%% @see infinite_log.erl for detailed information on pruning/expiry thresholds
-type threshold_key() :: size_pruning_threshold | age_pruning_threshold | expiry_threshold.

% boundaries for thresholds that can be specified for an audit log;
% if lower/greater values are provided, they are adjusted to these boundaries
-define(MIN_SIZE_PRUNING_THRESHOLD_SEC, cluster_worker:get_env(audit_log_min_size_pruning_threshold, 1)).
-define(MIN_AGE_PRUNING_THRESHOLD_SEC, cluster_worker:get_env(audit_log_min_age_pruning_threshold_seconds, 1)).
-define(MIN_EXPIRY_THRESHOLD_SEC, cluster_worker:get_env(audit_log_min_expiry_threshold_seconds, 86_400)).  % a day
-define(MAX_SIZE_PRUNING_THRESHOLD_SEC, cluster_worker:get_env(audit_log_max_size_pruning_threshold, 500_000)).
-define(MAX_AGE_PRUNING_THRESHOLD_SEC, cluster_worker:get_env(audit_log_max_age_pruning_threshold_seconds, 5_184_000)). % 60 days
-define(MAX_EXPIRY_THRESHOLD_SEC, cluster_worker:get_env(audit_log_max_expiry_threshold_seconds, 7_776_000)). % 90 days
% default thresholds used if not provided
-define(DEFAULT_SIZE_PRUNING_THRESHOLD_SEC, cluster_worker:get_env(audit_log_default_size_pruning_threshold, 5_000)).
-define(DEFAULT_AGE_PRUNING_THRESHOLD_SEC, cluster_worker:get_env(audit_log_default_age_pruning_threshold_seconds, 1_209_600)). % 14 days
-define(DEFAULT_EXPIRY_THRESHOLD_SEC, cluster_worker:get_env(audit_log_default_expiry_threshold_seconds, 2_592_000)). % 30 days


%%%===================================================================
%%% CRUD API
%%%===================================================================


-spec normalize_severity(binary()) -> entry_severity().
normalize_severity(ProvidedSeverity) ->
    case lists:member(ProvidedSeverity, ?AUDIT_LOG_SEVERITY_LEVELS) of
        true -> ProvidedSeverity;
        false -> ?INFO_AUDIT_LOG_SEVERITY
    end.


-spec severity_to_int(entry_severity()) -> entry_severity_int().
severity_to_int(?DEBUG_AUDIT_LOG_SEVERITY) -> ?DEBUG_AUDIT_LOG_SEVERITY_INT;
severity_to_int(?INFO_AUDIT_LOG_SEVERITY) -> ?INFO_AUDIT_LOG_SEVERITY_INT;
severity_to_int(?NOTICE_AUDIT_LOG_SEVERITY) -> ?NOTICE_AUDIT_LOG_SEVERITY_INT;
severity_to_int(?WARNING_AUDIT_LOG_SEVERITY) -> ?WARNING_AUDIT_LOG_SEVERITY_INT;
severity_to_int(?ERROR_AUDIT_LOG_SEVERITY) -> ?ERROR_AUDIT_LOG_SEVERITY_INT;
severity_to_int(?CRITICAL_AUDIT_LOG_SEVERITY) -> ?CRITICAL_AUDIT_LOG_SEVERITY_INT;
severity_to_int(?ALERT_AUDIT_LOG_SEVERITY) -> ?ALERT_AUDIT_LOG_SEVERITY_INT;
severity_to_int(?EMERGENCY_AUDIT_LOG_SEVERITY) -> ?EMERGENCY_AUDIT_LOG_SEVERITY_INT.


-spec severity_from_int(entry_severity_int()) -> entry_severity().
severity_from_int(?DEBUG_AUDIT_LOG_SEVERITY_INT) -> ?DEBUG_AUDIT_LOG_SEVERITY;
severity_from_int(?INFO_AUDIT_LOG_SEVERITY_INT) -> ?INFO_AUDIT_LOG_SEVERITY;
severity_from_int(?NOTICE_AUDIT_LOG_SEVERITY_INT) -> ?NOTICE_AUDIT_LOG_SEVERITY;
severity_from_int(?WARNING_AUDIT_LOG_SEVERITY_INT) -> ?WARNING_AUDIT_LOG_SEVERITY;
severity_from_int(?ERROR_AUDIT_LOG_SEVERITY_INT) -> ?ERROR_AUDIT_LOG_SEVERITY;
severity_from_int(?CRITICAL_AUDIT_LOG_SEVERITY_INT) -> ?CRITICAL_AUDIT_LOG_SEVERITY;
severity_from_int(?ALERT_AUDIT_LOG_SEVERITY_INT) -> ?ALERT_AUDIT_LOG_SEVERITY;
severity_from_int(?EMERGENCY_AUDIT_LOG_SEVERITY_INT) -> ?EMERGENCY_AUDIT_LOG_SEVERITY.


-spec should_log(entry_severity_int(), entry_severity_int()) -> boolean().
should_log(LogLevel, LogSeverityInt) ->
    LogSeverityInt =< LogLevel.


-spec append(id(), infinite_log:log_opts(), append_request()) -> ok | {error, term()}.
append(Id, AcquireLogOpts, #audit_log_append_request{
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
            ensure_created(Id, AcquireLogOpts),
            append(Id, AcquireLogOpts, AppendRequest);
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
            ?report_internal_server_error("returned error: ~tp", [Error])
    end.


-spec delete(id()) -> ok | {error, term()}.
delete(Id) ->
    json_infinite_log_model:destroy(Id).


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
-spec ensure_created(id(), infinite_log:log_opts()) -> ok | {error, term()}.
ensure_created(Id, Opts) ->
    case json_infinite_log_model:create(Id, resolve_opts(Opts)) of
        ok -> ok;
        {error, already_exists} -> ok;
        {error, _} = Error -> Error
    end.


%% @private
-spec listing_postprocessor(json_infinite_log_model:entry()) -> entry().
listing_postprocessor({IndexBin, {Timestamp, Entry}}) ->
    Entry#{
        <<"index">> => IndexBin,
        <<"timestamp">> => Timestamp
    }.


%% @private
-spec resolve_opts(infinite_log:log_opts()) -> infinite_log:log_opts().
resolve_opts(Opts) ->
    Opts#{
        size_pruning_threshold => resolve_threshold_opt(size_pruning_threshold, Opts),
        age_pruning_threshold => resolve_threshold_opt(age_pruning_threshold, Opts),
        expiry_threshold => resolve_threshold_opt(expiry_threshold, Opts)
    }.


%% @private
-spec resolve_threshold_opt(threshold_key(), infinite_log:log_opts()) -> non_neg_integer().
resolve_threshold_opt(Key, Opts) ->
    RequestedThreshold = maps:get(Key, Opts, default_threshold(Key)),
    MinThreshold = min_threshold(Key),
    MaxThreshold = max_threshold(Key),
    if
        RequestedThreshold > MaxThreshold ->
            ?warning(
                "Requested an audit log with ~ts of ~B, which is greater than allowed maximum (~B), "
                "using the max value instead",
                [Key, RequestedThreshold, MaxThreshold]
            ),
            MaxThreshold;
        RequestedThreshold < MinThreshold ->
            ?warning(
                "Requested an audit log with ~ts of ~B, which is lower than allowed minimum (~B), "
                "using the min value instead",
                [Key, RequestedThreshold, MinThreshold]
            ),
            MinThreshold;
        true ->
            RequestedThreshold
    end.


%% @private
-spec min_threshold(threshold_key()) -> non_neg_integer().
min_threshold(size_pruning_threshold) -> ?MIN_SIZE_PRUNING_THRESHOLD_SEC;
min_threshold(age_pruning_threshold) -> ?MIN_AGE_PRUNING_THRESHOLD_SEC;
min_threshold(expiry_threshold) -> ?MIN_EXPIRY_THRESHOLD_SEC.


%% @private
-spec max_threshold(threshold_key()) -> non_neg_integer().
max_threshold(size_pruning_threshold) -> ?MAX_SIZE_PRUNING_THRESHOLD_SEC;
max_threshold(age_pruning_threshold) -> ?MAX_AGE_PRUNING_THRESHOLD_SEC;
max_threshold(expiry_threshold) -> ?MAX_EXPIRY_THRESHOLD_SEC.


%% @private
-spec default_threshold(threshold_key()) -> non_neg_integer().
default_threshold(size_pruning_threshold) -> ?DEFAULT_SIZE_PRUNING_THRESHOLD_SEC;
default_threshold(age_pruning_threshold) -> ?DEFAULT_AGE_PRUNING_THRESHOLD_SEC;
default_threshold(expiry_threshold) -> ?DEFAULT_EXPIRY_THRESHOLD_SEC.
