%% ==================================================================
%% @author Michal Sitko
%% @copyright (C) 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ==================================================================
%% @doc:
%% This module contains rule definitions, most of them are supposed
%% to be registered on cluster startup by register_default_rules
%% function.
%% @end
%% ==================================================================

-module(rule_definitions).
-author("Michal Sitko").

-include("logging.hrl").
-include("registered_names.hrl").
-include("veil_modules/dao/dao_helper.hrl").
-include("veil_modules/dao/dao.hrl").
-include("veil_modules/cluster_rengine/cluster_rengine.hrl").

-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").

%% API
-export([register_quota_exceeded_handler/0, register_rm_event_handler/0, register_for_write_events/1, register_for_truncate_events/0]).

-define(ProtocolVersion, 1).
-define(VIEW_UPDATE_DELAY, 5000).

%% ====================================================================
%% Rule definitions
%% ====================================================================
register_quota_exceeded_handler() ->
  EventHandler = fun(Event) ->
    UserDn = proplists:get_value("user_dn", Event),
    change_write_enabled(UserDn, false)
  end,
  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

  %% no client configuration needed - register event handler
  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {"quota_exceeded_event", EventItem}}}).

register_rm_event_handler() ->
  EventHandler = fun(Event) ->
    ?info("---- bazinga - inside rm_event_handler"),

    CheckQuotaExceeded = fun(UserDn) ->
      case user_logic:quota_exceeded({dn, UserDn}, ?ProtocolVersion) of
        false ->
          change_write_enabled(UserDn, true),
          false;
        _ ->
          true
      end
    end,

    %% this function returns boolean true only if check quota is needed and quota is exceeded
    CheckQuotaIfNeeded = fun(UserDn) ->
      case user_logic:get_user({dn, UserDn}) of
        {ok, UserDoc} ->
          case user_logic:get_quota(UserDoc) of
            {ok, #quota{exceeded = true}} ->
              %% calling CheckQuota causes view reloading so we call it only when needed (quota has been already exceeded)
              CheckQuotaExceeded(UserDn);
            _ -> false
          end;
        Error ->
          ?warning("cannot get user with dn: ~p, Error: ~p", [UserDn, Error]),
          false
      end
    end,

    UserDn = proplists:get_value("user_dn", Event),
    Exceeded = CheckQuotaIfNeeded(UserDn),

    %% if quota is exceeded check quota one more time after 5s - in meanwhile db view might get reloaded
    case Exceeded of
      true ->
        spawn(fun() ->
          receive
            _ -> ok
          after ?VIEW_UPDATE_DELAY ->
            CheckQuotaExceeded(UserDn)
          end
        end);
      _ ->
        ok
    end
  end,
  EventItem = #event_handler_item{processing_method = standard, handler_fun = EventHandler},

  %% client configuration
  EventFilter = #eventfilterconfig{field_name = "type", desired_value = "rm_event"},
  EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},

  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {"rm_event", EventItem, EventFilterConfig}}}).

%% Registers handler which will be called every Bytes will be written.
register_for_write_events(Bytes) ->
  ?info("-- bazinga - register_for_write_events with freq: ~p", [Bytes]),
  EventHandler = fun(Event) ->
    UserDn = proplists:get_value("user_dn", Event),
    case user_logic:quota_exceeded({dn, UserDn}, ?ProtocolVersion) of
      true ->
        ?info("--- bazinga - quota exceeded for user ~p", [UserDn]),
        cluster_rengine:send_event(?ProtocolVersion, [{"type", "quota_exceeded_event"}, {"user_dn", UserDn}]);
      _ ->
        ok
    end
  end,

  EventHandlerMapFun = get_standard_map_fun(),
  EventHandlerDispMapFun = get_standard_disp_map_fun(),
  EventItem = #event_handler_item{processing_method = tree, handler_fun = EventHandler, map_fun = EventHandlerMapFun, disp_map_fun = EventHandlerDispMapFun, config = #event_stream_config{config = #aggregator_config{field_name = "user_dn", fun_field_name = "bytes", threshold = Bytes}}},

  %% client configuration
  EventFilter = #eventfilterconfig{field_name = "type", desired_value = "write_event"},
  EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},
  EventAggregator = #eventaggregatorconfig{field_name = "type", threshold = Bytes, sum_field_name = "bytes"},
  EventAggregatorConfig = #eventstreamconfig{aggregator_config = EventAggregator, wrapped_config = EventFilterConfig},

  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {add_event_handler, {"write_event", EventItem, EventAggregatorConfig}}}).

register_for_truncate_events() ->
  EventFilter = #eventfilterconfig{field_name = "type", desired_value = "truncate_event"},
  EventFilterConfig = #eventstreamconfig{filter_config = EventFilter},
  EventTransformer = #eventtransformerconfig{field_names_to_replace = ["type"], values_to_replace = ["truncate_event"], new_values = ["rm_event"]},
  EventTransformerConfig = #eventstreamconfig{transformer_config = EventTransformer},

  gen_server:call({?Dispatcher_Name, node()}, {rule_manager, ?ProtocolVersion, self(), {register_producer_config, EventTransformerConfig}}).


%% ====================================================================
%% Helper functions.
%% ====================================================================

change_write_enabled(UserDn, true) ->
  ?info("----- bazinga - change_write_enabled true"),
  worker_host:send_to_user({dn, UserDn}, #atom{value = "write_enabled"}, "communication_protocol", ?ProtocolVersion),
  gen_server:cast({global, ?CCM}, {update_user_write_enabled, UserDn, true});
change_write_enabled(UserDn, false) ->
  ?info("----- bazinga - change_write_enabled false"),
  worker_host:send_to_user({dn, UserDn}, #atom{value = "write_disabled"}, "communication_protocol", ?ProtocolVersion),
  gen_server:cast({global, ?CCM}, {update_user_write_enabled, UserDn, false}).


get_standard_map_fun() ->
  fun(WriteEv) ->
    UserDnString = proplists:get_value("user_dn", WriteEv),
    case UserDnString of
      undefined -> ok;
      _ -> string:len(UserDnString)
    end
  end.

get_standard_disp_map_fun() ->
  fun(WriteEv) ->
    UserDnString = proplists:get_value("user_dn", WriteEv),
    case UserDnString of
      undefined -> ok;
      _ ->
        UserIdInt = string:len(UserDnString),
        UserIdInt div 10
    end
  end.