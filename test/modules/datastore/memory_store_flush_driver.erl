%%%--------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Helper for unit tests for memory_store_driver module.
%%% @end
%%%--------------------------------------------------------------------
-module(memory_store_flush_driver).

-ifdef(TEST).

-include("global_definitions.hrl").
-include("modules/datastore/memory_store_driver.hrl").
-include("modules/datastore/datastore_models_def.hrl").
-include("modules/datastore/datastore_common_internal.hrl").
-include("modules/tp/tp.hrl").
-include_lib("eunit/include/eunit.hrl").

% Driver mock functions
-export([save/2, get/2, delete/3, get_link_doc/2, save_doc_asynch/2, asynch_response/1,
    delete_doc_asynch/2, delete_doc/2, select_bucket/2, get_last/2, save_revision/3,
    save_revision_asynch/3]).


%%%===================================================================
%%% Driver mock functions
%%%===================================================================


save(_ModelConfig, #document{value = "error"}) ->
    {error, error};
save(_ModelConfig, Document) ->
    {ok, Document#document.key}.

get(_ModelConfig, _Key) ->
    get(get_flush_response).

delete(_ModelConfig, _Key, _Pred) ->
    ok.

get_link_doc(_ModelConfig, _Key) ->
    get(get_flush_response).

save_doc_asynch(_ModelConfig, Document) ->
    Document.

asynch_response(#document{value = error}) ->
    {error, error};
asynch_response(#document{} = Document) ->
    {ok, Document#document.key};
asynch_response(_) ->
    ok.

delete_doc_asynch(_ModelConfig, _Key) ->
    get(get_flush_response).

delete_doc(_ModelConfig, _Key) ->
    ok.

select_bucket(_, _) ->
    b.

get_last(_, _) ->
    get(get_last_response).

save_revision(_, _, #document{key = Key}) ->
    {ok, Key}.

save_revision_asynch(_, _, #document{key = Key}) ->
    {ok, Key}.

-endif.