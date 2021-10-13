%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This header defines common macros used in datastore tests.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_TEST_UTILS_HRL).
-define(DATASTORE_TEST_UTILS_HRL, 1).

-include("modules/datastore/datastore_links.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("performance_test_utils.hrl").

-define(TEST_MODELS, [
    ets_only_model,
    mnesia_only_model,
    ets_cached_model,
    mnesia_cached_model,
    disc_only_model
]).

-define(TEST_CACHED_MODELS, [
    ets_cached_model,
    mnesia_cached_model
]).

-define(TEST_PERSISTENT_MODELS, [
    ets_cached_model,
    mnesia_cached_model,
    disc_only_model
]).

-define(MODEL_VALUE(Model), ?MODEL_VALUE(Model, 1)).
-define(MODEL_VALUE(Model, N), ?MODEL_VALUE(Model, N, 'field')).
-define(MODEL_VALUE(Model, N, Field3), ?MODEL_VALUE(Model, N, integer_to_binary(N), Field3)).
-define(MODEL_VALUE(Model, Field1, Field2, Field3), {Model, Field1, Field2, Field3}).

-define(MEM_DRV, ets_driver).
-define(MEM_DRV(Model), datastore_test_utils:get_memory_driver(Model)).
-define(MEM_CTX(Model), #{table => ?TABLE(Model)}).
-define(TABLE(Model), list_to_atom(atom_to_list(Model) ++ "_table")).
-define(TABLE1(Model), list_to_atom(atom_to_list(Model) ++ "_table1")).

-define(DISC_DRV, couchbase_driver).
-define(DISC_DRV(Model), datastore_test_utils:get_disc_driver(Model)).
-define(DISC_CTX, #{bucket => ?BUCKET}).
-define(BUCKET, <<"onedata">>).

-define(REMOTE_DRV, undefined).

-define(CASE, atom_to_binary(?FUNCTION_NAME, utf8)).
-define(TERM(Name, N), <<Name, "-", (?CASE)/binary, "-",
    (integer_to_binary(N))/binary>>).

-define(KEY, ?KEY(1)).
-define(KEY(N), ?TERM("key", N)).
-define(UNIQUE_KEY(Model, Key),
    datastore_model:get_unique_key(Model, Key)).
-define(RND_KEY, datastore_key:new()).
-define(REV, ?REV(1)).
-define(REV(N), <<(integer_to_binary(N))/binary, "-",
    (str_utils:rand_hex(16))/binary>>).
-define(MUTATOR, ?MUTATOR(1)).
-define(MUTATOR(N), ?TERM("mutator", N)).
-define(SCOPE, ?SCOPE(?CASE)).
-define(SCOPE(Case), <<"scope-", (Case)/binary>>).


-define(DESIGN, <<"design-", (?CASE)/binary>>).
-define(VIEW, <<"view-", (?CASE)/binary>>).
-define(VIEW_SPATIAL, <<"spatial-", (?CASE)/binary>>).

-define(LINK_TREE_ID, ?LINK_TREE_ID(1)).
-define(LINK_TREE_ID(N), ?TERM("tree", N)).
-define(LINK_NAME, ?LINK_NAME(1)).
-define(LINK_NAME(N), ?TERM("link", N)).
-define(LINK_TARGET, ?LINK_TARGET(1)).
-define(LINK_TARGET(N), ?TERM("target", N)).

-define(BASE_DOC(Key, Value), ?BASE_DOC(Key, Value, ?SCOPE)).
-define(BASE_DOC(Key, Value, Scope), ?BASE_DOC(Key, Value, Scope, [])).
-define(BASE_DOC(Key, Value, Scope, Mutators), #document{
    key = Key,
    value = Value,
    revs = [?REV],
    scope = Scope,
    mutators = Mutators
}).

-define(OPS_NUM(Value), ?PERF_PARAM(ops_num, Value, "",
    "Number of operations.")).
-define(THR_NUM(Value), ?PERF_PARAM(threads_num, Value, "",
    "Number of threads.")).

-define(assertAllMatch(Expected, List), lists:foreach(fun(Elem) ->
    ?assertMatch(Expected, Elem)
end, List)).

-endif.
