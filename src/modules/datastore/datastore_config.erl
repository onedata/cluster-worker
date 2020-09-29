%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides datastore configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_config).
-author("Krzysztof Trzepla").

%% API
-export([init/0, get_models/0, get_throttled_models/0, get_timestamp/0]).

-type model() :: datastore_model:model().

-define(DEFAULT_MODELS, [
    links_forest,
    links_mask,
    links_mask_root,
    links_node,
    lock,
    node_management,
    cluster_generation,
    task_pool,
    gs_session,
    gs_subscriber,
    gs_subscription,
    traverse_task,
    traverse_tasks_scheduler,
    view_traverse_job,
    node_internal_services,
    % Model used for performance testing (mocked models cannot be used as they affect performance)
    performance_test_record
]).

-define(PLUGIN, datastore_config_plugin).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Ensures that datastore_config_plugin is loaded, if existent.
%% @end
%%--------------------------------------------------------------------
-spec init() -> ok.
init() ->
    code:ensure_loaded(?PLUGIN),
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of default and custom models.
%% @end
%%--------------------------------------------------------------------
-spec get_models() -> [model()].
get_models() ->
    ?DEFAULT_MODELS ++ apply_plugin(get_models, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of throttled models.
%% @end
%%--------------------------------------------------------------------
-spec get_throttled_models() -> [model()].
get_throttled_models() ->
    apply_plugin(get_throttled_models, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Returns timestamp to be used to tag document.
%% @end
%%--------------------------------------------------------------------
-spec get_timestamp() -> datastore_doc:timestamp().
get_timestamp() ->
    case erlang:function_exported(?PLUGIN, get_timestamp, 0) of
        true -> erlang:apply(?PLUGIN, get_timestamp, []);
        false -> time_utils:timestamp_seconds()
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls plugin function if provided or returns default value.
%% @end
%%--------------------------------------------------------------------
-spec apply_plugin(atom(), list(), term()) -> term().
apply_plugin(Callback, Args, Default) ->
    Arity = length(Args),
    case erlang:function_exported(?PLUGIN, Callback, Arity) of
        true -> erlang:apply(?PLUGIN, Callback, Args);
        false -> Default
    end.
