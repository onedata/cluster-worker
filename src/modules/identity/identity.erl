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

-export([publish/1, verify/1, publish/2, verify/2, get/1]).
-export([ssl_verify_fun_impl/3]).

-type(id() :: binary()).
-type(public_key() :: term()).
-type(encoded_public_key() :: binary()).
-type(certificate() :: #'OTPCertificate'{}).
-export_type([id/0, public_key/0, certificate/0, encoded_public_key/0]).

-define(CERT_DB_KEY, <<"identity_cert">>).

%%--------------------------------------------------------------------
%% @doc
%% Publishes identity info (ID and public key) to the repository.
%% That info is used during identity verification.
%% Identity info can be directly supplied or inferred from certificate.
%% @end
%%--------------------------------------------------------------------
-spec publish(identity:certificate()) -> ok | {error, Reason :: term()}.
publish(#'OTPCertificate'{} = Certificate) ->
    ID = identity_utils:get_id(Certificate),
    PublicKey = identity_utils:get_public_key(Certificate),
    publish(ID, identity_utils:encode(PublicKey)).

-spec publish(identity:id(), identity:encoded_public_key()) -> ok | {error, Reason :: term()}.
publish(ID, EncodedPublicKey) ->
    case plugins:apply(identity_repository, publish, [ID, EncodedPublicKey]) of
        ok -> plugins:apply(identity_cache, put, [ID, EncodedPublicKey]);
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Uses identity info (ID and public key) to verify if it matches
%% data from the repository.
%% Repository contents may be cached and if cached data do not match,
%% the data would be re-fetched.
%% @end
%%--------------------------------------------------------------------
-spec verify(identity:certificate()) ->
    ok | {error, key_does_not_match} | {error, Reason :: term()}.
verify(#'OTPCertificate'{} = Certificate) ->
    ID = identity_utils:get_id(Certificate),
    PublicKeyToMatch = identity_utils:get_public_key(Certificate),
    verify(ID, identity_utils:encode(PublicKeyToMatch)).

-spec verify(identity:id(), identity:encoded_public_key()) ->
    ok | {error, key_does_not_match} | {error, Reason :: term()}.
verify(ID, EncodedPublicKeyToMatch) ->
    case plugins:apply(identity_cache, get, [ID]) of
        {ok, EncodedPublicKeyToMatch} -> ok;
        _ ->
            case plugins:apply(identity_repository, get, [ID]) of
                {error, Reason} ->
                    ?warning("Cached key does not match and unable to refetch key for ~p", [ID]),
                    plugins:apply(identity_cache, invalidate, [ID]),
                    {error, Reason};
                {ok, EncodedPublicKeyToMatch} ->
                    ?info("Key changed for ~p", [ID]),
                    plugins:apply(identity_cache, put, [ID, EncodedPublicKeyToMatch]),
                    ok;
                {ok, _ActualPublicKey} ->
                    ?warning("Attempt to connect with wrong public key from ~p", [ID]),
                    plugins:apply(identity_cache, put, [ID, _ActualPublicKey]),
                    {error, key_does_not_match}
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Retrieves public key for given ID from the repository.
%% @end
%%--------------------------------------------------------------------
-spec get(identity:id()) -> {ok, identity:encoded_public_key()} | {error, Reason :: term()}.
get(ID) ->
    plugins:apply(identity_repository, get, [ID]).

%%--------------------------------------------------------------------
%% @doc
%% This callback implements verify_fun specified in ssl options.
%% This implementation overrides peer identity verification
%% as it uses repository as source of truth (ignoring CA's).
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

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @private
%% Wraps verify function to return results conforming with verify_fun for ssl options.
%% @end
%%--------------------------------------------------------------------
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