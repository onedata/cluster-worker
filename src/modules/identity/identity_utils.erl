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
-module(identity_utils).
-author("Michal Zmuda").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("public_key/include/public_key.hrl").

-export([encode/1, decode/1]).
-export([get_public_key/1, get_id/1]).
-export([ensure_synced_cert_present/3, read_cert/1]).

-type(filename() :: file:name_all()).

-define(CERT_DB_KEY, <<"identity_cert">>).

%%--------------------------------------------------------------------
%% @doc
%% Encodes public key.
%% @end
%%--------------------------------------------------------------------
-spec encode(identity:public_key()) -> identity:encoded_public_key().
encode(PublicKey) ->
    base64:encode(term_to_binary(PublicKey)).

%%--------------------------------------------------------------------
%% @doc
%% Decodes public key.
%% @end
%%--------------------------------------------------------------------
-spec decode(identity:encoded_public_key()) -> identity:public_key().
decode(PublicKey) ->
    binary_to_term(base64:decode(PublicKey)).

%%--------------------------------------------------------------------
%% @doc
%% Ensures that certificates used in identity verifications are present
%% in db and are synced to filesystem.
%% Creates self-signed certificate if none present.
%% @end
%%--------------------------------------------------------------------
-spec ensure_synced_cert_present(KeyFilePath :: filename(),
    CertFilePath :: filename(), DomainForCN :: string()) -> ok.
ensure_synced_cert_present(KeyFile, CertFile, DomainForCN) ->
    critical_section:run(?MODULE, fun() ->
        case synced_cert:get(?CERT_DB_KEY) of
            {ok, #document{value = #synced_cert{
                cert_file_content = DBCert, key_file_content = DBKey}}} ->
                ok = file:write_file(CertFile, DBCert),
                ok = file:write_file(KeyFile, DBKey),
                ok;
            {error, not_found} ->
                ok = ensure_certs_files_present(KeyFile, CertFile, DomainForCN),
                {ok, FSCert} = file:read_file(CertFile),
                {ok, FSKey} = file:read_file(KeyFile),
                {ok, _} = synced_cert:create(#document{
                    key = ?CERT_DB_KEY, value = #synced_cert{
                        cert_file_content = FSCert, key_file_content = FSKey
                    }}
                ),
                ok
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Reads certificate pem & decodes it.
%% @end
%%--------------------------------------------------------------------
-spec read_cert(CertFile :: filename()) -> identity:certificate().
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
-spec get_id(identity:certificate()) -> identity:id().
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
-spec get_public_key(identity:certificate()) -> PublicKey :: identity:public_key().
get_public_key(#'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{
    subjectPublicKeyInfo = #'OTPSubjectPublicKeyInfo'{subjectPublicKey = Key}}}) ->
    Key.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @private
%% Creates self-signed cert files if no cert files are present on filesystem.
%% @end
%%--------------------------------------------------------------------
-spec ensure_certs_files_present(KeyFilePath :: filename(),
    CertFilePath :: filename(), DomainForCN :: string()) -> ok.
ensure_certs_files_present(KeyFile, CertFile, DomainForCN) ->
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