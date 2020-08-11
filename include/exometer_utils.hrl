%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2013 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-ifndef(EXOMETER_UTILS_HRL).
-define(EXOMETER_UTILS_HRL, 1).

%%%===================================================================
%%% Macros that allows disabling exometer
%%%===================================================================

%-define(skip_exometer, 1).
% Skip (replace with ok) exometer counters that are updated during datastore calls
% Counters used by datastore backend (async saves to couch) are not skipped
-define(skip_datastore_internals, 1).

-ifdef(skip_exometer).

-define(init_counters(_Counters), ok).
-define(init_reports(_Reports), ok).
-define(init_reports(_Reports, _Reporters), ok).
-define(update_counter(_Param), ok).
-define(update_counter(_Param, _Value), ok).
-define(get_value(_Param, _Type), ok).
-define(reset(_Param), ok).
-define(init_exometer_reporters, ok).
-define(init_exometer_reporters(_InitReports), ok).
-define(update_datastore_internal_counter(_Param), ok).
-define(update_datastore_internal_counter(_Param, _Value), ok).

-endif.

-ifndef(skip_exometer).

-define(init_counters(Counters), exometer_utils:init_counters(Counters)).
-define(init_reports(Counters), exometer_utils:init_reports(Counters)).
-define(init_reports(Reports, Reporters),
  exometer_utils:init_reports(Reports, Reporters)).
-define(update_counter(Param), exometer_utils:update_counter(Param)).
-define(update_counter(Param, Value),
  exometer_utils:update_counter(Param, Value)).
-define(get_value(Param, Type), exometer_utils:get_value(Param, Type)).
-define(reset(Param), exometer_utils:reset(Param)).
-define(init_exometer_reporters, exometer_utils:init_exometer_reporters()).
-define(init_exometer_reporters(InitReports),
  exometer_utils:init_exometer_reporters(InitReports)).

-ifdef(skip_datastore_internals).
-define(update_datastore_internal_counter(_Param), ok).
-define(update_datastore_internal_counter(_Param, _Value), ok).
-endif.
-ifndef(skip_datastore_internals).
-define(update_datastore_internal_counter(Param), exometer_utils:update_counter(Param)).
-define(update_datastore_internal_counter(Param, Value),
  exometer_utils:update_counter(Param, Value)).
-endif.

-endif.

%%%===================================================================
%%% Macros for creation exometer counters' names
%%%===================================================================

-define(exometer_name(Module, Name), 
  ?exometer_name(Module, none, Name)).

-define(exometer_name(Module, Submodule, Name),
  ?exometer_name(0, Module, Submodule, Name)).

-define(exometer_name(Thread, Module, Submodule, Name),
  [thread, Thread, mod, Module, submod, Submodule, Name]).

-endif.
