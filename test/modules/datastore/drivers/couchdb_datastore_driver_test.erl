%%%--------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for couchdb_datastore_driver module.
%%% @end
%%%--------------------------------------------------------------------
-module(couchdb_datastore_driver_test).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

normalize_seq_test() ->
    ?assertEqual(42342, couchdb_datastore_driver:normalize_seq(42342)),
    ?assertEqual(423424234, couchdb_datastore_driver:normalize_seq(423424234)),
    ?assertEqual(5464564, couchdb_datastore_driver:normalize_seq(<<"5464564">>)),
    ?assertEqual(931809318, couchdb_datastore_driver:normalize_seq(<<"931809318::423564564564">>)),
    ?assertEqual(423564564564, couchdb_datastore_driver:normalize_seq(<<"423564564564::931809318">>)),

    ok.

-endif.
