%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Helper functions that allow multiplication of datastore components.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_multiplier).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").

%% API
-export([extend_name/2, get_names/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Extends the name with namespace extension calculated using key.
%% @end
%%--------------------------------------------------------------------
-spec extend_name(datastore:key(), atom() | datastore:ctx()) ->
  atom() | datastore:ctx().
extend_name(Key, Name) when is_atom(Name) ->
  list_to_atom(atom_to_list(Name) ++ get_num(Key));
extend_name(Key, #{table := Table} = Ctx) ->
  NewName = list_to_atom(atom_to_list(Table) ++ get_num(Key)),
  datastore_context:override(table, NewName, Ctx);
extend_name(Key, #{memory_driver_ctx := #{table := Table}} = Ctx) ->
  NewName = list_to_atom(atom_to_list(Table) ++ get_num(Key)),
  override_table(NewName, Ctx);
extend_name(_Key, Name) ->
  Name.

%%--------------------------------------------------------------------
%% @doc
%% Returns all namespaces connected with particular name.
%% @end
%%--------------------------------------------------------------------
-spec get_names(atom() | datastore:ctx()) ->
  [atom() | datastore:ctx()].
get_names(Name) when is_atom(Name) ->
  lists:map(fun(Num) ->
    list_to_atom(atom_to_list(Name) ++ Num)
  end, get_name_extensions());
get_names(#{table := Table} = Ctx) ->
  lists:map(fun(Num) ->
    NewName = list_to_atom(atom_to_list(Table) ++ Num),
    datastore_context:override(table, NewName, Ctx)
  end, get_name_extensions());
get_names(#{memory_driver_ctx := #{table := Table}} = Ctx) ->
  lists:map(fun(Num) ->
    NewName = list_to_atom(atom_to_list(Table) ++ Num),
    override_table(NewName, Ctx)
  end, get_name_extensions()).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns all namespaces' suffixes.
%% @end
%%--------------------------------------------------------------------
-spec get_name_extensions() ->
  [string()].
get_name_extensions() ->
  Num = application:get_env(?CLUSTER_WORKER_APP_NAME,
    tp_subtrees_number, 10),
  lists:map(fun(Int) ->
    integer_to_list(Int)
  end, lists:seq(1, Num)).

%%--------------------------------------------------------------------
%% @doc
%% Returns namespace's suffix for a key.
%% @end
%%--------------------------------------------------------------------
-spec get_num(datastore:key()) ->
  [non_neg_integer()].
get_num(Key) when is_binary(Key) ->
  MaxNum = application:get_env(?CLUSTER_WORKER_APP_NAME,
    tp_subtrees_number, 10),
  ID = binary:decode_unsigned(Key),
  integer_to_list(ID rem MaxNum + 1);
get_num(Key) ->
  get_num(crypto:hash(md5, term_to_binary(Key))).

%%--------------------------------------------------------------------
%% @doc
%% Overrides memory driver table name in context.
%% @end
%%--------------------------------------------------------------------
-spec override_table(atom(), datastore:ctx()) ->
  datastore:ctx().
override_table(Name, Ctx) ->
  MemCtx = maps:get(memory_driver_ctx, Ctx),
  datastore_context:override(memory_driver_ctx,
    datastore_context:override(table, Name, MemCtx), Ctx).
