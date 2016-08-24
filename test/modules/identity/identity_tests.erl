%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(identity_tests).
-author("Michal Zmuda").

-ifdef(TEST).
-include("modules/datastore/datastore_models_def.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").

-define(DB_KEY_FROM_IDENTITY_MODULE, <<"identity_cert">>).

-define(DB_ENV, db_data).
-define(CACHE_ENV, db_data).
-define(REPO_ENV, repo_data).

-define(SAMPLE_CERT_FILE, "../certs/onedataServerWeb.pem").

%%%===================================================================
%%% Tests description
%%%===================================================================

identity_test_() ->
    {foreach,
        fun setup/0,
        fun teardown/1,
        [
            {"verification succeeds on properly published cert", fun verification_succeeds_on_published_cert/0},
            {"verification fails on not published cert", fun verification_fails_on_not_published_cert/0},
            {"verification fails on cert with different public key", fun verification_fails_on_public_key_mismatch/0},

            {"verification succeeds on properly published cert but not found in cache", fun verification_succeeds_on_published_cert_not_found_in_cache/0},

            {"certs and db state created when no cert data in both db and fs", fun cert_file_and_doc_created_on_no_cert_supplied/0},
            {"certs on fs created when cert data in db", fun cert_file_created_on_doc_supplied/0},
            {"certs in db created when cert data on fs", fun cert_doc_created_on_cert_supplied_by_fs/0}
        ]
    }.

%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

setup() ->
    meck:new(plugins),
    meck:expect(plugins, apply, fun
    %% cache
        (identity_cache, put, [ID, PublicKey]) -> save_to_env(ID, PublicKey, ?CACHE_ENV);
        (identity_cache, get, [ID]) -> get_from_env(ID, ?CACHE_ENV);
        (identity_cache, invalidate, [ID]) -> delete_from_env(ID, ?CACHE_ENV);
        %% repo
        (identity_repository, get, [ID]) -> get_from_env(ID, ?REPO_ENV);
        (identity_repository, publish, [ID, PublicKey]) -> save_to_env(ID, PublicKey, ?REPO_ENV)
    end),
    %% db
    meck:new(synced_cert),
    meck:expect(synced_cert, create, fun(Doc = #document{key = Key}) -> save_to_env(Key, Doc, ?DB_ENV), {ok, Key} end),
    meck:expect(synced_cert, get, fun(Key) -> get_from_env(Key, ?DB_ENV) end),
    %% critical_section
    meck:new(critical_section),
    meck:expect(critical_section, run, fun(_, Fun) -> Fun() end),
    ok.


teardown(_) ->
    meck:unload(critical_section),
    meck:unload(synced_cert),
    ok = application:unset_env(app, ?DB_ENV),
    meck:unload(plugins),
    ok = application:unset_env(app, ?REPO_ENV),
    ok = application:unset_env(app, ?CACHE_ENV),
    ok.

%%%===================================================================
%%% Tests functions
%%%===================================================================

cert_file_and_doc_created_on_no_cert_supplied() ->
    %% given
    TmpDir = utils:mkdtemp(),
    CertFile = TmpDir ++ "/cert",
    KeyFile = TmpDir ++ "/key",

    %% when
    identity_utils:ensure_synced_cert_present(KeyFile, CertFile, "onedata.example.com"),

    %% then
    Cert = identity_utils:read_cert(CertFile),
    ?assertMatch(<<"onedata.example.com">>, identity_utils:get_id(Cert)),

    {ok, CertBin} = file:read_file(CertFile),
    {ok, KeyBin} = file:read_file(KeyFile),
    DbContent = synced_cert:get(?DB_KEY_FROM_IDENTITY_MODULE),
    ?assertMatch({ok, #document{key = _, value = #synced_cert{
        cert_file_content = CertBin,
        key_file_content = KeyBin
    }}}, DbContent).

cert_file_created_on_doc_supplied() ->
    %% given
    TmpDir = utils:mkdtemp(),
    CertFile = TmpDir ++ "/cert",
    KeyFile = TmpDir ++ "/key",
    identity_utils:ensure_synced_cert_present(KeyFile, CertFile, "onedata.example.com"),

    %% when
    ok = file:delete(CertFile),
    ok = file:delete(KeyFile),
    identity_utils:ensure_synced_cert_present(KeyFile, CertFile, "onedata.example.com"),

    %% then
    {ok, CertBin} = file:read_file(CertFile),
    {ok, KeyBin} = file:read_file(KeyFile),
    DbContent = synced_cert:get(?DB_KEY_FROM_IDENTITY_MODULE),
    ?assertMatch({ok, #document{key = _, value = #synced_cert{
        cert_file_content = CertBin,
        key_file_content = KeyBin
    }}}, DbContent).


cert_doc_created_on_cert_supplied_by_fs() ->
    %% given
    TmpDir = utils:mkdtemp(),
    CertFile = TmpDir ++ "/cert",
    KeyFile = TmpDir ++ "/key",
    identity_utils:ensure_synced_cert_present(KeyFile, CertFile, "onedata.example.com"),

    %% when
    ok = application:unset_env(app, ?DB_ENV),
    identity_utils:ensure_synced_cert_present(KeyFile, CertFile, "onedata.example.com"),

    %% then
    {ok, CertBin} = file:read_file(CertFile),
    {ok, KeyBin} = file:read_file(KeyFile),
    DbContent = synced_cert:get(?DB_KEY_FROM_IDENTITY_MODULE),
    ?assertMatch({ok, #document{key = _, value = #synced_cert{
        cert_file_content = CertBin,
        key_file_content = KeyBin
    }}}, DbContent).

verification_succeeds_on_published_cert() ->
    %% given
    Cert = identity_utils:read_cert(?SAMPLE_CERT_FILE),

    %% when
    identity:publish(Cert),
    Res = identity:verify(Cert),

    %% then
    ?assertEqual(ok, Res).

verification_fails_on_not_published_cert() ->
    %% given
    Cert = identity_utils:read_cert(?SAMPLE_CERT_FILE),

    %% when
    Res = identity:verify(Cert),

    %% then
    ?assertMatch({error, {key_not_available, _}}, Res).


verification_fails_on_public_key_mismatch() ->
    %% given
    Cert = identity_utils:read_cert(?SAMPLE_CERT_FILE),
    #'OTPCertificate'{tbsCertificate = TBS = #'OTPTBSCertificate'{
        subjectPublicKeyInfo = Subject = #'OTPSubjectPublicKeyInfo'{}}} = Cert,
    ChangedCert = Cert#'OTPCertificate'{tbsCertificate = TBS#'OTPTBSCertificate'{
        subjectPublicKeyInfo = Subject#'OTPSubjectPublicKeyInfo'{
            subjectPublicKey = {<<"changed">>}}}},

    %% when
    identity:publish(Cert),
    Res = identity:verify(ChangedCert),

    %% then
    ?assertEqual({error, key_does_not_match}, Res).



verification_succeeds_on_published_cert_not_found_in_cache() ->
    %% given
    Cert = identity_utils:read_cert(?SAMPLE_CERT_FILE),
    identity:publish(Cert),

    %% when
    ok = application:unset_env(app, ?DB_ENV),
    Res = identity:verify(Cert),

    %% then
    ?assertEqual(ok, Res).

%%%===================================================================
%%% Internal functions
%%%===================================================================

save_to_env(ID, Data, Env) ->
    Saved = application:get_env(app, Env, #{}),
    application:set_env(app, Env, Saved#{ID => Data}),
    ok.

delete_from_env(ID, Env) ->
    Saved = application:get_env(app, Env, #{}),
    application:set_env(app, Env, maps:remove(ID, Saved)),
    ok.

get_from_env(ID, Env) ->
    case application:get_env(app, Env, #{}) of
        #{ID := Value} -> {ok, Value};
        _ -> {error, {not_found, Env}}
    end.

-endif.