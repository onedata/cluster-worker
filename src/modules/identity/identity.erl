%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module publishes own certificate info to repository
%%% and verifies other certs with repository.
%%% @end
%%%-------------------------------------------------------------------
-module(identity).
-author("Michal Zmuda").

-include("modules/datastore/datastore_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("public_key/include/public_key.hrl").

-type(id() :: binary()).
-type(public_key() :: term()).
-type(certificate() :: #'OTPCertificate'{}).

-define(CERT_DB_KEY, <<"identity_cert">>).

-export([publish/1, verify/1, ssl_verify_fun_impl/3]).
-export([get_public_key/1, get_id/1, ensure_identity_cert_created/3, read_cert/1]).
-export_type([id/0, public_key/0, certificate/0]).

%%--------------------------------------------------------------------
%% @doc
%% Publishes public key from certificate under ID determined from certificate.
%% @end
%%--------------------------------------------------------------------
-spec publish(#'OTPCertificate'{}) -> ok | {error, Reason :: term()}.
publish(#'OTPCertificate'{} = Certificate) ->
    ID = get_id(Certificate),
    PublicKey = get_public_key(Certificate),
    case plugins:apply(identity_repository, publish, [ID, PublicKey]) of
        ok -> plugins:apply(identity_cache, put, [ID, PublicKey]);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Uses certificate info to verify if certificate owner is present in repository
%% and if public key matches one present in repository.
%% @end
%%--------------------------------------------------------------------
-spec verify(#'OTPCertificate'{}) ->
    ok | {error, key_does_not_match} | {error, Reason :: term()}.
verify(#'OTPCertificate'{} = Certificate) ->
    ID = get_id(Certificate),
    PublicKeyToMatch = get_public_key(Certificate),
    case plugins:apply(identity_cache, get, [ID]) of
        {ok, PublicKeyToMatch} -> ok;
        _ ->
            case plugins:apply(identity_repository, get, [ID]) of
                {error, Reason} ->
                    ?warning("Cached key does not match and unable to refetch key for ~p", [ID]),
                    plugins:apply(identity_cache, invalidate, [ID]),
                    {error, Reason};
                {ok, PublicKeyToMatch} ->
                    ?info("Key changed for ~p", [ID]),
                    plugins:apply(identity_cache, put, [ID, PublicKeyToMatch]),
                    ok;
                {ok, _ActualPublicKey} ->
                    ?warning("Attempt to connect with wrong public key from ~p", [ID]),
                    plugins:apply(identity_cache, put, [ID, _ActualPublicKey]),
                    {error, key_does_not_match}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% This callback implements verify_fun specified in ssl options.
%% This implementation overrides peer identity verification as it uses repository
%% as source of truth.
%% @end
%%--------------------------------------------------------------------
-spec ssl_verify_fun_impl(OtpCert :: #'OTPCertificate'{},
    Event :: {bad_cert, Reason :: atom() | {revoked, atom()}} | {extension, #'Extension'{}} | valid | valid_peer,
    InitialUserState :: term()) ->
    {valid, UserState :: term()}
    | {valid_peer, UserState :: term()}
    | {fail, Reason :: term()}
    | {unknown, UserState :: term()}.
ssl_verify_fun_impl(_, {bad_cert, unknown_ca}, _UserState) ->
    {fail, only_selfigned_certs_are_allowed_in_interoz_communication};
ssl_verify_fun_impl(OtpCert, {bad_cert, _} = _Reason, UserState) ->
    verify_as_ssl_callback(OtpCert, UserState);
ssl_verify_fun_impl(OtpCert, {extension, _}, UserState) ->
    verify_as_ssl_callback(OtpCert, UserState);
ssl_verify_fun_impl(OtpCert, valid_peer, UserState) ->
    verify_as_ssl_callback(OtpCert, UserState);
ssl_verify_fun_impl(_, valid, UserState) ->
    {valid, UserState}.


%%--------------------------------------------------------------------
%% @doc
%% Ensures that self-signed identity certificates are created on filesystem.
%% @end
%%--------------------------------------------------------------------
-spec ensure_identity_cert_created(KeyFilePath :: string(), CertFilePath :: string(),
    DomainForCN :: string()) -> ok.
ensure_identity_cert_created(KeyFile, CertFile, DomainForCN) ->
    recreate_cert_files(KeyFile, DomainForCN, CertFile),
    try_creating_certs_doc(CertFile, KeyFile),
    create_certs_form_doc(CertFile, KeyFile).

%%--------------------------------------------------------------------
%% @doc
%% Reads certificate pem & decodes it.
%% @end
%%--------------------------------------------------------------------
-spec read_cert(CertFile :: file:name_all()) -> #'OTPCertificate'{}.
read_cert(CertFile) ->
    {ok, CertBin} = file:read_file(CertFile),
    Contents = public_key:pem_decode(CertBin),
    [{'Certificate', CertDer, not_encrypted} | _] = lists:dropwhile(fun
        ({'Certificate', _, not_encrypted}) -> false;
        (_) -> true
    end, Contents),
    public_key:pkix_decode_cert(CertDer, otp).

%%--------------------------------------------------------------------
%% @doc
%% Extract ID from certificate data.
%% @end
%%--------------------------------------------------------------------
-spec get_id(#'OTPCertificate'{}) -> CommonName :: binary().
get_id(#'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{subject = {rdnSequence, Subject}}}) ->
    case [Attribute#'AttributeTypeAndValue'.value || [Attribute] <- Subject,
        Attribute#'AttributeTypeAndValue'.type == ?'id-at-commonName'] of
        [{teletexString, Str}] -> list_to_binary(Str);
        [{printableString, Str}] -> list_to_binary(Str);
        [{utf8String, Bin}] -> Bin
    end.

%%--------------------------------------------------------------------
%% @doc
%% Extract public key from certificate data.
%% @end
%%--------------------------------------------------------------------
-spec get_public_key(#'OTPCertificate'{}) -> PublicKey :: identity:public_key().
get_public_key(#'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{
    subjectPublicKeyInfo = #'OTPSubjectPublicKeyInfo'{subjectPublicKey = Key}}}) ->
    Key.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec verify_as_ssl_callback(OtpCert :: #'OTPCertificate'{}, InitialUserState :: term()) ->
    {valid, UserState :: term()}
    | {valid_peer, UserState :: term()}
    | {fail, Reason :: term()}
    | {unknown, UserState :: term()}.
verify_as_ssl_callback(OtpCert, UserState) ->
    case identity:verify(OtpCert) of
        ok -> {valid, UserState};
        {error, key_does_not_match} -> {fail, rejected_by_repo_verification};
        {error, Reason} -> {fail, {repo_verification_unexpectedly_failed, Reason}}
    end.

-spec create_certs_form_doc(KeyFilePath :: string(), CertFilePath :: string()) -> ok.
create_certs_form_doc(CertFile, KeyFile) ->
    case synced_cert:get(?CERT_DB_KEY) of
        {ok, #document{value = #synced_cert{cert_file_content = DBCert, key_file_content = DBKey}}} ->
            ok = file:write_file(CertFile, DBCert),
            ok = file:write_file(KeyFile, DBKey);
        {error, Reason} ->
            ?error("Identity cert files not synced with DB due to ~p", [Reason])
    end.

-spec try_creating_certs_doc(KeyFilePath :: string(), CertFilePath :: string()) -> ok.
try_creating_certs_doc(CertFile, KeyFile) ->
    {ok, CertBin} = file:read_file(CertFile),
    {ok, KeyBin} = file:read_file(KeyFile),
    synced_cert:create(#document{key = ?CERT_DB_KEY, value = #synced_cert{
        cert_file_content = CertBin, key_file_content = KeyBin
    }}),
    ok.

-spec recreate_cert_files(KeyFilePath :: string(), CertFilePath :: string(),
    DomainForCN :: string()) -> ok.
recreate_cert_files(KeyFile, DomainForCN, CertFile) ->
    case file:read_file_info(KeyFile) of
        {ok, _} -> ok;
        {error, enoent} ->
            TmpDir = utils:mkdtemp(),
            PassFile = TmpDir ++ "/pass",
            CSRFile = TmpDir ++ "/csr",

            os:cmd(["openssl genrsa", " -des3 ", " -passout ", " pass:x ", " -out ", PassFile, " 2048 "]),
            os:cmd(["openssl rsa", " -passin ", " pass:x ", " -in ", PassFile, " -out ", KeyFile]),
            os:cmd(["openssl req", " -new ", " -key ", KeyFile, " -out ", CSRFile, " -subj ", "\"/CN=" ++ DomainForCN ++ "\""]),
            os:cmd(["openssl x509", " -req ", " -days ", " 365 ", " -in ", CSRFile, " -signkey ", KeyFile, " -out ", CertFile]),
            utils:rmtempdir(TmpDir)
    end.