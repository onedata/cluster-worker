%%%-------------------------------------------------------------------
%%% @author Michal Zmuda
%%% @copyright (C): 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module publishes own certificate info to DHT
%%% and verifies other certs with DHT.
%%% @end
%%%-------------------------------------------------------------------
-module(identity).
-author("Michal Zmuda").

-include_lib("ctool/include/logging.hrl").
-include_lib("public_key/include/public_key.hrl").

-type(id() :: binary()).
-type(public_key() :: term()).
-type(certificate() :: #'OTPCertificate'{}).

-export([publish_to_dht/1, verify_with_dht/1, ssl_verify_fun_impl/3,
    get_public_key/1, get_id/1,
    ensure_identity_cert_created/3, read_cert/1]).
-export_type([id/0, public_key/0, certificate/0]).

%%--------------------------------------------------------------------
%% @doc
%% Publishes public key from certificate under ID determined from certificate.
%% @end
%%--------------------------------------------------------------------
-spec publish_to_dht(#'OTPCertificate'{}) -> ok | {error, Reason :: term()}.
publish_to_dht(#'OTPCertificate'{} = Certificate) ->
    ID = get_id(Certificate),
    PublicKey = get_public_key(Certificate),
    plugins:apply(identity_repository, publish, [ID, PublicKey]).

%%--------------------------------------------------------------------
%% @doc
%% Uses certificate info to verify if certificate owner is present in DHT
%% and if public key matches one present in DHT.
%% @end
%%--------------------------------------------------------------------
-spec verify_with_dht(#'OTPCertificate'{}) ->
    ok | {error, key_does_not_match} | {error, Reason :: term()}.
verify_with_dht(#'OTPCertificate'{} = Certificate) ->
    ID = get_id(Certificate),
    PublicKeyToMatch = get_public_key(Certificate),
    case plugins:apply(identity_repository, get, [ID]) of
        {error, Reason} -> {error, Reason};
        {ok, PublicKeyToMatch} -> ok;
        {ok, _ActualPublicKey} -> {error, key_does_not_match}
    end.

%%--------------------------------------------------------------------
%% @doc
%% This callback implements verify_fun specified in ssl options.
%% This implementation overrides peer identity verification as it uses DHT
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
    verify_with_dht_as_ssl_callback(OtpCert, UserState);
ssl_verify_fun_impl(OtpCert, {extension, _}, UserState) ->
    verify_with_dht_as_ssl_callback(OtpCert, UserState);
ssl_verify_fun_impl(OtpCert, valid_peer, UserState) ->
    verify_with_dht_as_ssl_callback(OtpCert, UserState);
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

%%--------------------------------------------------------------------
%% @doc
%% Reads certificate pem & decodes it.
%% @end
%%--------------------------------------------------------------------
-spec read_cert(CertFile :: file:name_all()) -> #'OTPCertificate'{}.
read_cert(CertFile) ->
    {ok, CertBin} = file:read_file(CertFile),
    [Certificate] = public_key:pem_decode(CertBin),
    {'Certificate', CertDer, not_encrypted} = Certificate,
    public_key:pkix_decode_cert(CertDer, otp).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec get_id(#'OTPCertificate'{}) -> CommonName :: binary().
get_id(#'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{subject = {rdnSequence, Subject}}}) ->
    case [Attribute#'AttributeTypeAndValue'.value || [Attribute] <- Subject,
        Attribute#'AttributeTypeAndValue'.type == ?'id-at-commonName'] of
        [{teletexString, Str}] -> list_to_binary(Str);
        [{printableString, Str}] -> list_to_binary(Str);
        [{utf8String, Bin}] -> Bin
    end.

-spec get_public_key(#'OTPCertificate'{}) -> PublicKey :: term().
get_public_key(#'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{
    subjectPublicKeyInfo = #'OTPSubjectPublicKeyInfo'{subjectPublicKey = Key}}}) ->
    Key.

-spec verify_with_dht_as_ssl_callback(OtpCert :: #'OTPCertificate'{}, InitialUserState :: term()) ->
    {valid, UserState :: term()}
    | {valid_peer, UserState :: term()}
    | {fail, Reason :: term()}
    | {unknown, UserState :: term()}.
verify_with_dht_as_ssl_callback(OtpCert, UserState) ->
    ?emergency("~p",[OtpCert]),
    case identity:verify_with_dht(OtpCert) of
        ok -> {valid, UserState};
        {error, key_does_not_match} -> {fail, rejected_by_dht_verification};
        {error, DHTReason} -> {fail, {dht_verification_unexpectedly_failed, DHTReason}}
    end.