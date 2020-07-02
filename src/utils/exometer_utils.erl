%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides utils functions connected with exometer.
%%%-------------------------------------------------------------------
-module(exometer_utils).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").

%% API
-export([init_counters/1, update_counter/1, update_counter/2, init_reports/1,
  init_reports/2, init_exometer_counters/0, init_exometer_reporters/0,
  init_exometer_reporters/1, init_reporter/1, get_value/2, reset/1,
  extend_counter_name/1]).

-define(EXOMETER_REPORTERS, [
  {exometer_report_lager, ?MODULE},
  {exometer_report_graphite, ?MODULE}
]).

-define(EXOMETER_MODULES,
  [couchbase_batch, couchbase_pool, node_manager,  datastore_worker, 
       datastore_router, datastore_cache]).

-define(GRAPHITE_REPORTER_OPTS, [
  graphite_api_key,
  graphite_prefix,
  graphite_host,
  graphite_port,
  graphite_connect_timeout
]).

-define(EXOMETER_DEFAULT_LOGGING_INTERVAL, 60000).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Initializes exometer counters if they are not at excluded list.
%% @end
%%--------------------------------------------------------------------
-spec init_counters([{Param :: list(), Type :: atom(),
    Options:: proplists:proplist()} | {Param :: list(), Type :: atom()}]) -> ok.
init_counters([]) ->
  ok;
init_counters([{Param, Type, Options} | Tail]) ->
  init_counter(Param, Type, Options),
  init_counters(Tail);
init_counters([{Param, Type} | Tail]) ->
  init_counter(Param, Type),
  init_counters(Tail).

%%--------------------------------------------------------------------
%% @doc
%% Updates exometer counter if it is not at excluded list.
%% @end
%%--------------------------------------------------------------------
-spec update_counter(Param :: list()) -> ok.
update_counter(Param) ->
  case is_counter_excluded(Param) of
    true ->
      ok;
    _ ->
      exometer:update(extend_counter_name(Param), 1)
  end,
  ok.

%%--------------------------------------------------------------------
%% @doc
%% Updates exometer counter if it is not at excluded list.
%% @end
%%--------------------------------------------------------------------
-spec update_counter(Param :: list(), Value :: number()) -> ok.
update_counter(Param, Value) ->
  case is_counter_excluded(Param) of
    true ->
      ok;
    _ ->
      exometer:update(extend_counter_name(Param), Value)
  end,
  ok.

%%--------------------------------------------------------------------
%% @doc
%% Gets exometer counter if it is not at excluded list.
%% @end
%%--------------------------------------------------------------------
-spec get_value(Param :: list(), Types :: [atom()]) ->
  {ok, any()} | {error, not_found}.
get_value(Param, Types) ->
  case is_counter_excluded(Param) of
    true ->
      {ok, []};
    _ ->
      exometer:get_value(extend_counter_name(Param), Types)
  end.

%%--------------------------------------------------------------------
%% @doc
%% Resets exometer counter if it is not at excluded list.
%% @end
%%--------------------------------------------------------------------
-spec reset(Param :: list()) -> list().
reset(Param) ->
  case is_counter_excluded(Param) of
    true ->
      [];
    _ ->
      exometer:reset(extend_counter_name(Param))
  end.

%%--------------------------------------------------------------------
%% @doc
%% Subscribe for reports for parameters if parameters are not at excluded list.
%% @end
%%--------------------------------------------------------------------
-spec init_reports([{Param :: list(), Report :: [atom()]}]) -> ok.
init_reports(Reports) ->
  init_reports(Reports, [exometer_report_lager, exometer_report_graphite]).

%%--------------------------------------------------------------------
%% @doc
%% Subscribe for reports for parameters if parameters are not at excluded list.
%% @end
%%--------------------------------------------------------------------
-spec init_reports([{Param :: list(), Report :: [atom()]}],
    Reporters :: [atom()]) -> ok.
init_reports([], _Reporters) ->
  ok;
init_reports([{Param, Report} | Tail], Reporters) ->
  init_report(Param, Report, Reporters),
  init_reports(Tail).

%%--------------------------------------------------------------------
%% @doc
%% Checks if counters. Can be used after change of counters list to add/stop
%% counters.
%% @end
%%--------------------------------------------------------------------
-spec init_exometer_counters() -> ok.
init_exometer_counters() ->
  Modules = plugins:apply(node_manager_plugin, modules_with_exometer, []),

  lists:foreach(fun(Module) ->
    Module:init_counters()
  end, ?EXOMETER_MODULES ++ Modules),
  init_exometer_reports().

%%--------------------------------------------------------------------
%% @doc
%% Checks if reporter is alive. If not, starts it and subscribe for reports.
%% @end
%%--------------------------------------------------------------------
-spec init_exometer_reporters() -> ok.
init_exometer_reporters() ->
  init_exometer_reporters(true).

%%--------------------------------------------------------------------
%% @doc
%% Checks if reporter is alive. If not, starts it and subscribe for reports.
%% @end
%%--------------------------------------------------------------------
-spec init_exometer_reporters(InitReports :: boolean()) -> ok.
init_exometer_reporters(InitReports) ->
  GraphiteOn = application:get_env(?CLUSTER_WORKER_APP_NAME,
    integrate_with_graphite, false),
  ExpectedReporters0 = case GraphiteOn of
    true -> ?EXOMETER_REPORTERS;
    false -> ?EXOMETER_REPORTERS -- [{exometer_report_graphite, ?MODULE}]
  end,

  LagerOn = application:get_env(?CLUSTER_WORKER_APP_NAME,
    exometer_lager_reporter, false),
  ExpectedReporters00 = case LagerOn of
    true -> ExpectedReporters0;
    false -> ExpectedReporters0 -- [{exometer_report_lager, ?MODULE}]
  end,

  ExpectedReporters = ExpectedReporters00 ++
    plugins:apply(node_manager_plugin, exometer_reporters, []),
  ReportersNames = lists:map(fun({Name, _Mod}) -> Name end, ExpectedReporters),
  Find = lists:filtermap(fun({Reporter, Pid}) ->
    case {lists:member(Reporter, ReportersNames), erlang:is_process_alive(Pid)} of
      {true, true} -> {true, Reporter};
      _ -> false
    end
  end, exometer_report:list_reporters()),

  ToStart = ReportersNames -- Find,
  lists:foreach(fun(Reporter) ->
    Module = proplists:get_value(Reporter, ExpectedReporters),
    exometer_report:remove_reporter(Reporter),
    Module:init_reporter(Reporter)
  end, ToStart),

  case {ToStart, InitReports} of
    {[], _} ->
      ok;
    {_, true} ->
      init_exometer_reports();
    _ ->
      ok
  end.

%%--------------------------------------------------------------------
%% @doc
%% Checks if reporter is alive. If not, starts it and subscribe for reports.
%% @end
%%--------------------------------------------------------------------
-spec init_exometer_reports() -> ok.
init_exometer_reports() ->
  Modules = plugins:apply(node_manager_plugin, modules_with_exometer, []),

  lists:foreach(fun(Module) ->
    Module:init_report()
  end, ?EXOMETER_MODULES ++ Modules).

%%--------------------------------------------------------------------
%% @doc
%% Initialize exometer reporter.
%% @end
%%--------------------------------------------------------------------
-spec init_reporter(atom()) -> ok.
init_reporter(exometer_report_lager) ->
  Level = application:get_env(?CLUSTER_WORKER_APP_NAME,
    exometer_logging_level, debug),
  ok = exometer_report:add_reporter(exometer_report_lager, [
    {type_map,[{'_',integer}]},
    {level, Level}
  ]);
init_reporter(exometer_report_graphite) ->
  Opts = get_graphite_reporter_options(),
  ok = exometer_report:add_reporter(exometer_report_graphite, Opts).

%%--------------------------------------------------------------------
%% @doc
%% Extends counter name with prefix.
%% @end
%%--------------------------------------------------------------------
-spec extend_counter_name(Param :: list()) -> list().
extend_counter_name(Name) ->
  Prefixes = application:get_env(?CLUSTER_WORKER_APP_NAME,
    counter_name_prefixes, []),
  Prefixes ++ Name.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes exometer counter if it is not at excluded list.
%% @end
%%--------------------------------------------------------------------
-spec init_counter(Param :: list(), Type :: atom(),
    Options :: proplists:proplist()) -> ok.
init_counter(Param, Type, Options) ->
  case is_counter_excluded(Param) of
    true ->
      exometer:delete(extend_counter_name(Param));
    _ ->
      catch exometer:new(extend_counter_name(Param), Type, Options)
  end,
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes exometer counter if it is not at excluded list.
%% @end
%%--------------------------------------------------------------------
-spec init_counter(Param :: list(), Type :: atom()) -> ok.
init_counter(Param, Type) ->
  case is_counter_excluded(Param) of
    true ->
      exometer:delete(extend_counter_name(Param));
    _ ->
      catch exometer:new(extend_counter_name(Param), Type)
  end,
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Subscribe for reports for parameter if parameter is not at excluded list.
%% @end
%%--------------------------------------------------------------------
-spec init_report(Param :: list(), Report :: [atom()],
    Reporters :: [atom()]) -> ok.
init_report(Param, Report, Reporters) ->
  Name = extend_counter_name(Param),
  case is_counter_excluded(Param) of
    true ->
      LagerOn = application:get_env(?CLUSTER_WORKER_APP_NAME,
        exometer_lager_reporter, false),
      case LagerOn andalso lists:member(exometer_report_lager, Reporters) of
        true ->
          exometer_report:unsubscribe(exometer_report_lager, Name, Report),
          ok;
        false ->
          ok
      end;
    _ ->
      LagerOn = application:get_env(?CLUSTER_WORKER_APP_NAME,
        exometer_lager_reporter, false),
      case LagerOn andalso lists:member(exometer_report_lager, Reporters) of
        true ->
          exometer_report:subscribe(exometer_report_lager, Name,
            Report, application:get_env(?CLUSTER_WORKER_APP_NAME,
              exometer_logging_interval, ?EXOMETER_DEFAULT_LOGGING_INTERVAL));
        false ->
          ok
      end,

      GraphiteOn = application:get_env(?CLUSTER_WORKER_APP_NAME,
        integrate_with_graphite, false),
      case GraphiteOn andalso lists:member(exometer_report_graphite, Reporters) of
        true ->
          exometer_report:subscribe(exometer_report_graphite, Name,
            Report, application:get_env(?CLUSTER_WORKER_APP_NAME,
              exometer_logging_interval, ?EXOMETER_DEFAULT_LOGGING_INTERVAL));
        false ->
          ok
      end
  end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Reads graphite_reporter options from app.config.
%% @end
%%-------------------------------------------------------------------
-spec get_graphite_reporter_options() -> proplists:proplist().
get_graphite_reporter_options() ->
  lists:foldl(fun(Opt, AccIn) ->
    case application:get_env(?CLUSTER_WORKER_APP_NAME, Opt) of
      {ok, Value} ->
        [{strip_graphite_prefix(Opt), Value} | AccIn];
      undefined -> AccIn
    end
  end, [], ?GRAPHITE_REPORTER_OPTS).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Strips "graphite_" prefix from option name.
%% @end
%%-------------------------------------------------------------------
-spec strip_graphite_prefix(atom()) -> atom().
strip_graphite_prefix(Option) ->
  <<"graphite_", OptionBin/binary>> = atom_to_binary(Option, latin1),
  binary_to_atom(OptionBin, latin1).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Checks if counter is at excluded list.
%% @end
%%--------------------------------------------------------------------
-spec is_counter_excluded(Param :: list()) -> boolean().
is_counter_excluded([thread, _Thread, mod, Module | _]) ->
  Excluded = application:get_env(?CLUSTER_WORKER_APP_NAME,
    excluded_exometer_modules, [datastore_router]),
  Excluded =:= all orelse lists:member(Module, Excluded);
is_counter_excluded(Param) ->
  Excluded = application:get_env(?CLUSTER_WORKER_APP_NAME,
    excluded_exometer_modules, [datastore_router]),
  Excluded =:= all orelse lists:member(Param, Excluded).
