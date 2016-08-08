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

-define(DB_ENV, db_data).
-define(DB_KEY_FROM_IDENTITY_MODULE, <<"identity_cert">>).

-define(REPO_ENV, repo_data).
-define(REPO_REASON, repo_error).

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
            {"certs and db state created when no cert data in both db and fs", fun cert_file_and_doc_created_on_no_cert_supplied/0},
            {"certs on fs created when cert data in db", fun cert_file_created_on_doc_supplied/0},
            {"certs in db created when cert data on fs", fun cert__doc_created_on_cert_supplied_by_fs/0}
        ]
    }.

%%%===================================================================
%%% Setup/teardown functions
%%%===================================================================

setup() ->
    meck:new(plugins),
    meck:expect(plugins, apply, fun
        (identity_repository, get, [ID]) ->
            case application:get_env(app, ?REPO_ENV, #{}) of
                #{ID := Value} -> {ok, Value};
                _ -> {error, ?REPO_REASON}
            end;
        (identity_repository, publish, [ID, PublicKey]) ->
            Saved = application:get_env(app, ?REPO_ENV, #{}),
            application:set_env(app, ?REPO_ENV, Saved#{ID => PublicKey}),
            {ok, ID}
    end),
    meck:new(synced_cert),
    meck:expect(synced_cert, create, fun(Doc = #document{key = Key}) ->
        Saved = application:get_env(app, ?DB_ENV, #{}),
        application:set_env(app, ?DB_ENV, Saved#{Key => Doc}),
        {ok, Key}
    end),
    meck:expect(synced_cert, get, fun(Key) ->
        case application:get_env(app, ?DB_ENV, #{}) of
            #{Key := Doc} -> {ok, Doc};
            _ -> {error, {not_found, synced_cert}}
        end
    end),
    ok.

teardown(_) ->
    meck:unload(synced_cert),
    ok = application:unset_env(app, ?DB_ENV),
    meck:unload(plugins),
    ok = application:unset_env(app, ?REPO_ENV),
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
    identity:ensure_identity_cert_created(KeyFile, CertFile, "onedata.example.com"),

    %% then
    Cert = identity:read_cert(CertFile),
    ?assertMatch(<<"onedata.example.com">>, identity:get_id(Cert)),

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
    identity:ensure_identity_cert_created(KeyFile, CertFile, "onedata.example.com"),

    %% when
    ok = file:delete(CertFile),
    ok = file:delete(KeyFile),
    identity:ensure_identity_cert_created(KeyFile, CertFile, "onedata.example.com"),

    %% then
    {ok, CertBin} = file:read_file(CertFile),
    {ok, KeyBin} = file:read_file(KeyFile),
    DbContent = synced_cert:get(?DB_KEY_FROM_IDENTITY_MODULE),
    ?assertMatch({ok, #document{key = _, value = #synced_cert{
        cert_file_content = CertBin,
        key_file_content = KeyBin
    }}}, DbContent).


cert__doc_created_on_cert_supplied_by_fs() ->
    %% given
    TmpDir = utils:mkdtemp(),
    CertFile = TmpDir ++ "/cert",
    KeyFile = TmpDir ++ "/key",
    identity:ensure_identity_cert_created(KeyFile, CertFile, "onedata.example.com"),

    %% when
    ok = application:unset_env(app, ?DB_ENV),
    identity:ensure_identity_cert_created(KeyFile, CertFile, "onedata.example.com"),

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
    Cert = identity:read_cert(?SAMPLE_CERT_FILE),

    %% when
    identity:publish_to_dht(Cert),
    Res = identity:verify_with_dht(Cert),

    %% then
    ?assertEqual(ok, Res).

verification_fails_on_not_published_cert() ->
    %% given
    Cert = identity:read_cert(?SAMPLE_CERT_FILE),

    %% when
    Res = identity:verify_with_dht(Cert),

    %% then
    ?assertEqual({error, ?REPO_REASON}, Res).


verification_fails_on_public_key_mismatch() ->
    %% given
    Cert = identity:read_cert(?SAMPLE_CERT_FILE),
    #'OTPCertificate'{tbsCertificate = TBS = #'OTPTBSCertificate'{
        subjectPublicKeyInfo = Subject = #'OTPSubjectPublicKeyInfo'{}}} = Cert,
    ChangedCert = Cert#'OTPCertificate'{tbsCertificate = TBS#'OTPTBSCertificate'{
        subjectPublicKeyInfo = Subject#'OTPSubjectPublicKeyInfo'{
            subjectPublicKey = {<<"changed">>}}}},

    %% when
    identity:publish_to_dht(Cert),
    Res = identity:verify_with_dht(ChangedCert),

    %% then
    ?assertEqual({error, key_does_not_match}, Res).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-endif.