%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module contains methods for encoding and decoding errors between
%%% internal expressions and JSON.
%%% @end
%%%-------------------------------------------------------------------
-module(gs_protocol_errors).
-author("Lukasz Opiola").

-include_lib("ctool/include/api_errors.hrl").

-export([error_to_json/2, json_to_error/2]).

%%%===================================================================
%%% API
%%%===================================================================

-spec error_to_json(gs_protocol:protocol_version(), undefined | gs_protocol:error()) ->
    null | gs_protocol:json_map().
error_to_json(_, undefined) ->
    null;
% This error can be sent before handshake is negotiated, so it must support all
% protocol versions.
error_to_json(_, ?ERROR_BAD_MESSAGE(MessageJSON)) ->
    #{
        <<"id">> => <<"badMessage">>,
        <<"details">> => #{
            <<"message">> => MessageJSON
        }
    };
% This error can be sent before handshake is negotiated, so it must support all
% protocol versions.
error_to_json(_, ?ERROR_BAD_VERSION(SupportedVersions)) ->
    #{
        <<"id">> => <<"badVersion">>,
        <<"details">> => #{
            <<"supportedVersions">> => SupportedVersions
        }
    };
% This error can be sent before handshake is negotiated, so it must support all
% protocol versions.
error_to_json(_, ?ERROR_EXPECTED_HANDSHAKE_MESSAGE) ->
    #{
        <<"id">> => <<"expectedHandshakeMessage">>
    };
error_to_json(_, ?ERROR_HANDSHAKE_ALREADY_DONE) ->
    #{
        <<"id">> => <<"handshakeAlreadyDone">>
    };
error_to_json(_, ?ERROR_UNKNOWN_ERROR(ErrorObject)) ->
    ErrorObject;
error_to_json(_, ?ERROR_BAD_TYPE) ->
    #{
        <<"id">> => <<"badType">>
    };
error_to_json(_, ?ERROR_NOT_SUBSCRIBABLE) ->
    #{
        <<"id">> => <<"notSubscribable">>
    };
error_to_json(_, ?ERROR_RPC_UNDEFINED) ->
    #{
        <<"id">> => <<"rpcUndefined">>
    };
error_to_json(_, ?ERROR_INTERNAL_SERVER_ERROR) ->
    #{
        <<"id">> => <<"internalServerError">>
    };
error_to_json(_, ?ERROR_NOT_IMPLEMENTED) ->
    #{
        <<"id">> => <<"notImplemented">>
    };
error_to_json(_, ?ERROR_NOT_SUPPORTED) ->
    #{
        <<"id">> => <<"notSupported">>
    };
error_to_json(_, ?ERROR_NOT_FOUND) ->
    #{
        <<"id">> => <<"notFound">>
    };
error_to_json(ProtoVersion, ?ERROR_UNAUTHORIZED(AuthError)) ->
    #{
        <<"id">> => <<"unauthorized">>,
        <<"details">> => #{
            <<"authError">> => error_to_json(ProtoVersion, AuthError)
        }
    };
error_to_json(_, ?ERROR_UNAUTHORIZED) ->
    #{
        <<"id">> => <<"unauthorized">>
    };
error_to_json(_, ?ERROR_FORBIDDEN) ->
    #{
        <<"id">> => <<"forbidden">>
    };
error_to_json(_, ?ERROR_ALREADY_EXISTS) ->
    #{
        <<"id">> => <<"alreadyExists">>
    };
error_to_json(_, ?ERROR_POSIX(Errno)) ->
    #{
        <<"id">> => <<"posix">>,
        <<"details">> => #{
            <<"errno">> => Errno
        }
    };
error_to_json(_, ?ERROR_BAD_MACAROON) ->
    #{
        <<"id">> => <<"badMacaroon">>
    };
error_to_json(_, ?ERROR_MACAROON_INVALID) ->
    #{
        <<"id">> => <<"macaroonInvalid">>
    };
error_to_json(_, ?ERROR_MACAROON_EXPIRED) ->
    #{
        <<"id">> => <<"macaroonExpired">>
    };
error_to_json(_, ?ERROR_MACAROON_TTL_TO_LONG(MaxTtl)) ->
    #{
        <<"id">> => <<"macaroonTtlTooLong">>,
        <<"details">> => #{
            <<"maxTtl">> => MaxTtl
        }
    };
error_to_json(_, ?ERROR_BAD_AUDIENCE_TOKEN) ->
    #{
        <<"id">> => <<"badAudienceToken">>
    };
error_to_json(_, ?ERROR_TOKEN_AUDIENCE_FORBIDDEN) ->
    #{
        <<"id">> => <<"tokenAudienceForbidden">>
    };
error_to_json(_, ?ERROR_TOKEN_SESSION_INVALID) ->
    #{
        <<"id">> => <<"tokenSessionInvalid">>
    };
error_to_json(_, ?ERROR_MALFORMED_DATA) ->
    #{
        <<"id">> => <<"malformedData">>
    };
error_to_json(_, ?ERROR_BAD_BASIC_CREDENTIALS) ->
    #{
        <<"id">> => <<"badBasicCredentials">>
    };
error_to_json(_, ?ERROR_MISSING_REQUIRED_VALUE(Key)) ->
    #{
        <<"id">> => <<"missingRequiredValue">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_IDP_ACCESS_TOKEN(IdP)) ->
    #{
        <<"id">> => <<"badIdpAccessToken">>,
        <<"details">> => #{
            <<"idp">> => IdP
        }
    };
error_to_json(_, ?ERROR_MISSING_AT_LEAST_ONE_VALUE(Keys)) ->
    #{
        <<"id">> => <<"missingAtLeastOneValue">>,
        <<"details">> => #{
            <<"keys">> => Keys
        }
    };
error_to_json(_, ?ERROR_BAD_DATA(Key)) ->
    #{
        <<"id">> => <<"badData">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_EMPTY(Key)) ->
    #{
        <<"id">> => <<"badValueEmpty">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
% We do not differentiate between atoms and binaries in JSON, so they are
% treated as the same.
error_to_json(_, ?ERROR_BAD_VALUE_BINARY(Key)) ->
    #{
        <<"id">> => <<"badValueString">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_LIST_OF_BINARIES(Key)) ->
    #{
        <<"id">> => <<"badValueListOfStrings">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_ATOM(Key)) ->
    #{
        <<"id">> => <<"badValueString">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_LIST_OF_ATOMS(Key)) ->
    #{
        <<"id">> => <<"badValueListOfStrings">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_BOOLEAN(Key)) ->
    #{
        <<"id">> => <<"badValueBoolean">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_INTEGER(Key)) ->
    #{
        <<"id">> => <<"badValueInteger">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_FLOAT(Key)) ->
    #{
        <<"id">> => <<"badValueFloat">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_JSON(Key)) ->
    #{
        <<"id">> => <<"badValueJSON">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_TOKEN(Key)) ->
    #{
        <<"id">> => <<"badValueToken">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_LIST_OF_IPV4_ADDRESSES(Key)) ->
    #{
        <<"id">> => <<"badValueListOfIPv4Addresses">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_DOMAIN(Key)) ->
    #{
        <<"id">> => <<"badValueDomain">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_SUBDOMAIN) ->
    #{
        <<"id">> => <<"badValueSubdomain">>
    };
error_to_json(_, ?ERROR_BAD_VALUE_EMAIL) ->
    #{
        <<"id">> => <<"badValueEmail">>
    };
error_to_json(_, ?ERROR_BAD_VALUE_TOO_LOW(Key, Threshold)) ->
    #{
        <<"id">> => <<"badValueTooLow">>,
        <<"details">> => #{
            <<"key">> => Key,
            <<"limit">> => Threshold
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_TOO_HIGH(Key, Threshold)) ->
    #{
        <<"id">> => <<"badValueTooHigh">>,
        <<"details">> => #{
            <<"key">> => Key,
            <<"limit">> => Threshold
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_NOT_IN_RANGE(Key, Low, High)) ->
    #{
        <<"id">> => <<"badValueNotInRange">>,
        <<"details">> => #{
            <<"key">> => Key,
            <<"low">> => Low,
            <<"high">> => High
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_NOT_ALLOWED(Key, AllowedValues)) ->
    #{
        <<"id">> => <<"badValueNotAllowed">>,
        <<"details">> => #{
            <<"key">> => Key,
            <<"allowed">> => AllowedValues
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_LIST_NOT_ALLOWED(Key, AllowedValues)) ->
    #{
        <<"id">> => <<"badValueListNotAllowed">>,
        <<"details">> => #{
            <<"key">> => Key,
            <<"allowed">> => AllowedValues
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_ID_NOT_FOUND(Key)) ->
    #{
        <<"id">> => <<"badValueIdNotFound">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_AMBIGUOUS_ID(Key)) ->
    #{
        <<"id">> => <<"badValueAmbiguousId">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_IDENTIFIER_OCCUPIED(Key)) ->
    #{
        <<"id">> => <<"badValueIdentifierOccupied">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_BAD_TOKEN_TYPE(Key)) ->
    #{
        <<"id">> => <<"badValueTokenType">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_IDENTIFIER(Key)) ->
    #{
        <<"id">> => <<"badValueIntentifier">>,
        <<"details">> => #{
            <<"key">> => Key
        }
    };
error_to_json(_, ?ERROR_BAD_VALUE_FULL_NAME) ->
    #{
        <<"id">> => <<"badValueFullName">>
    };
error_to_json(_, ?ERROR_BAD_VALUE_USERNAME) ->
    #{
        <<"id">> => <<"badValueUsername">>
    };
error_to_json(_, ?ERROR_BAD_VALUE_PASSWORD) ->
    #{
        <<"id">> => <<"badValuePassword">>
    };
error_to_json(_, ?ERROR_BAD_VALUE_NAME) ->
    #{
        <<"id">> => <<"badValueName">>
    };
error_to_json(_, ?ERROR_SUBDOMAIN_DELEGATION_NOT_SUPPORTED) ->
    #{
        <<"id">> => <<"subdomainDelegationNotSupported">>
    };
error_to_json(_, ?ERROR_SUBDOMAIN_DELEGATION_DISABLED) ->
    #{
        <<"id">> => <<"subdomainDelegationDisabled">>
    };
error_to_json(_, ?ERROR_BASIC_AUTH_NOT_SUPPORTED) ->
    #{
        <<"id">> => <<"basicAuthNotSupported">>
    };
error_to_json(_, ?ERROR_BASIC_AUTH_DISABLED) ->
    #{
        <<"id">> => <<"basicAuthDisabled">>
    };
error_to_json(_, ?ERROR_PROTECTED_GROUP) ->
    #{
        <<"id">> => <<"protectedGroup">>
    };
error_to_json(_, ?ERROR_SPACE_NOT_SUPPORTED_BY(ProviderId)) ->
    #{
        <<"id">> => <<"spaceNotSupportedBy">>,
        <<"details">> => #{
            <<"providerId">> => ProviderId
        }
    };
error_to_json(_, ?ERROR_VIEW_NOT_EXISTS_ON(ProviderId)) ->
    #{
        <<"id">> => <<"viewNotExistsOn">>,
        <<"details">> => #{
            <<"providerId">> => ProviderId
        }
    };
error_to_json(_, ?ERROR_TRANSFER_ALREADY_ENDED) ->
    #{
        <<"id">> => <<"transferAlreadyEnded">>
    };
error_to_json(_, ?ERROR_TRANSFER_NOT_ENDED) ->
    #{
        <<"id">> => <<"transferNotEnded">>
    };
error_to_json(_, ?ERROR_RELATION_DOES_NOT_EXIST(ChType, ChId, ParType, ParId)) ->
    #{
        <<"id">> => <<"relationDoesNotExist">>,
        <<"details">> => #{
            <<"childType">> => ChType,
            <<"childId">> => ChId,
            <<"parentType">> => ParType,
            <<"parentId">> => ParId
        }
    };
error_to_json(_, ?ERROR_RELATION_ALREADY_EXISTS(ChType, ChId, ParType, ParId)) ->
    #{
        <<"id">> => <<"relationAlreadyExists">>,
        <<"details">> => #{
            <<"childType">> => ChType,
            <<"childId">> => ChId,
            <<"parentType">> => ParType,
            <<"parentId">> => ParId
        }
    };
error_to_json(_, ?ERROR_CANNOT_DELETE_ENTITY(EntityType, EntityId)) ->
    #{
        <<"id">> => <<"cannotDeleteEntity">>,
        <<"details">> => #{
            <<"entityType">> => EntityType,
            <<"entityId">> => EntityId
        }
    };
error_to_json(_, ?ERROR_CANNOT_ADD_RELATION_TO_SELF) ->
    #{
        <<"id">> => <<"cannotAddRelationToSelf">>
    };

error_to_json(_, ?ERROR_TEMPORARY_FAILURE) ->
    #{
        <<"id">> => <<"temporaryFailure">>
    };

error_to_json(_, ?ERROR_BAD_GUI_PACKAGE) ->
    #{
        <<"id">> => <<"badGuiPackage">>
    };

error_to_json(_, ?ERROR_GUI_PACKAGE_TOO_LARGE) ->
    #{
        <<"id">> => <<"guiPackageTooLarge">>
    };

error_to_json(_, ?ERROR_GUI_PACKAGE_UNVERIFIED(ShaSum)) ->
    #{
        <<"id">> => <<"guiPackageUnverified">>,
        <<"details">> => #{
            <<"shaSum">> => ShaSum
        }
    }.


-spec json_to_error(gs_protocol:protocol_version(), null | gs_protocol:json_map()) ->
    undefined | gs_protocol:error().
json_to_error(_, null) ->
    undefined;

% This error can be sent before handshake is negotiated, so it must support all
% protocol versions.
json_to_error(_, #{<<"id">> := <<"badMessage">>,
    <<"details">> := #{<<"message">> := Message}}) ->
    ?ERROR_BAD_MESSAGE(Message);

% This error can be sent before handshake is negotiated, so it must support all
% protocol versions.
json_to_error(_, #{<<"id">> := <<"badVersion">>,
    <<"details">> := #{<<"supportedVersions">> := SupportedVersions}}) ->
    ?ERROR_BAD_VERSION(SupportedVersions);

% This error can be sent before handshake is negotiated, so it must support all
% protocol versions.
json_to_error(_, #{<<"id">> := <<"expectedHandshakeMessage">>}) ->
    ?ERROR_EXPECTED_HANDSHAKE_MESSAGE;

json_to_error(_, #{<<"id">> := <<"handshakeAlreadyDone">>}) ->
    ?ERROR_HANDSHAKE_ALREADY_DONE;

json_to_error(_, #{<<"id">> := <<"badType">>}) ->
    ?ERROR_BAD_TYPE;

json_to_error(_, #{<<"id">> := <<"notSubscribable">>}) ->
    ?ERROR_NOT_SUBSCRIBABLE;

json_to_error(_, #{<<"id">> := <<"rpcUndefined">>}) ->
    ?ERROR_RPC_UNDEFINED;

json_to_error(_, #{<<"id">> := <<"internalServerError">>}) ->
    ?ERROR_INTERNAL_SERVER_ERROR;

json_to_error(_, #{<<"id">> := <<"notImplemented">>}) ->
    ?ERROR_NOT_IMPLEMENTED;

json_to_error(_, #{<<"id">> := <<"notSupported">>}) ->
    ?ERROR_NOT_SUPPORTED;

json_to_error(_, #{<<"id">> := <<"notFound">>}) ->
    ?ERROR_NOT_FOUND;

json_to_error(ProtoVersion, #{<<"id">> := <<"unauthorized">>, <<"details">> := #{<<"authError">> := AuthError}}) ->
    ?ERROR_UNAUTHORIZED(json_to_error(ProtoVersion, AuthError));

json_to_error(_, #{<<"id">> := <<"unauthorized">>}) ->
    ?ERROR_UNAUTHORIZED;

json_to_error(_, #{<<"id">> := <<"forbidden">>}) ->
    ?ERROR_FORBIDDEN;

json_to_error(_, #{<<"id">> := <<"alreadyExists">>}) ->
    ?ERROR_ALREADY_EXISTS;

json_to_error(_, #{
    <<"id">> := <<"posix">>,
    <<"details">> := #{<<"errno">> := Errno}
}) ->
    ?ERROR_POSIX(binary_to_existing_atom(Errno, utf8));

json_to_error(_, #{<<"id">> := <<"badMacaroon">>}) ->
    ?ERROR_BAD_MACAROON;

json_to_error(_, #{<<"id">> := <<"macaroonInvalid">>}) ->
    ?ERROR_MACAROON_INVALID;

json_to_error(_, #{<<"id">> := <<"macaroonExpired">>}) ->
    ?ERROR_MACAROON_EXPIRED;

json_to_error(_, #{<<"id">> := <<"macaroonTtlTooLong">>,
    <<"details">> := #{<<"maxTtl">> := MaxTtl}}) ->
    ?ERROR_MACAROON_TTL_TO_LONG(MaxTtl);

json_to_error(_, #{<<"id">> := <<"badAudienceToken">>}) ->
    ?ERROR_BAD_AUDIENCE_TOKEN;

json_to_error(_, #{<<"id">> := <<"tokenAudienceForbidden">>}) ->
    ?ERROR_TOKEN_AUDIENCE_FORBIDDEN;

json_to_error(_, #{<<"id">> := <<"tokenSessionInvalid">>}) ->
    ?ERROR_TOKEN_SESSION_INVALID;

json_to_error(_, #{<<"id">> := <<"malformedData">>}) ->
    ?ERROR_MALFORMED_DATA;

json_to_error(_, #{<<"id">> := <<"badBasicCredentials">>}) ->
    ?ERROR_BAD_BASIC_CREDENTIALS;

json_to_error(_, #{<<"id">> := <<"missingRequiredValue">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_MISSING_REQUIRED_VALUE(Key);

json_to_error(_, #{<<"id">> := <<"badIdpAccessToken">>,
    <<"details">> := #{<<"idp">> := IdP}}) ->
    ?ERROR_BAD_IDP_ACCESS_TOKEN(IdP);

json_to_error(_, #{<<"id">> := <<"missingAtLeastOneValue">>,
    <<"details">> := #{<<"keys">> := Keys}}) ->
    ?ERROR_MISSING_AT_LEAST_ONE_VALUE(Keys);

json_to_error(_, #{<<"id">> := <<"badData">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_DATA(Key);

json_to_error(_, #{<<"id">> := <<"badValueEmpty">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_EMPTY(Key);

json_to_error(_, #{<<"id">> := <<"badValueString">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_BINARY(Key);

json_to_error(_, #{<<"id">> := <<"badValueListOfStrings">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_LIST_OF_BINARIES(Key);

json_to_error(_, #{<<"id">> := <<"badValueBoolean">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_BOOLEAN(Key);

json_to_error(_, #{<<"id">> := <<"badValueInteger">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_INTEGER(Key);

json_to_error(_, #{<<"id">> := <<"badValueFloat">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_FLOAT(Key);

json_to_error(_, #{<<"id">> := <<"badValueJSON">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_JSON(Key);

json_to_error(_, #{<<"id">> := <<"badValueToken">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_TOKEN(Key);

json_to_error(_, #{<<"id">> := <<"badValueListOfIPv4Addresses">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_LIST_OF_IPV4_ADDRESSES(Key);

json_to_error(_, #{<<"id">> := <<"badValueDomain">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_DOMAIN(Key);
json_to_error(_, #{<<"id">> := <<"badValueSubdomain">>}) ->
    ?ERROR_BAD_VALUE_SUBDOMAIN;
json_to_error(_, #{<<"id">> := <<"badValueEmail">>}) ->
    ?ERROR_BAD_VALUE_EMAIL;
json_to_error(_, #{<<"id">> := <<"badValueTooLow">>,
    <<"details">> := #{<<"key">> := Key, <<"limit">> := Limit}}) ->
    ?ERROR_BAD_VALUE_TOO_LOW(Key, Limit);

json_to_error(_, #{<<"id">> := <<"badValueTooHigh">>,
    <<"details">> := #{<<"key">> := Key, <<"limit">> := Limit}}) ->
    ?ERROR_BAD_VALUE_TOO_HIGH(Key, Limit);

json_to_error(_, #{<<"id">> := <<"badValueNotInRange">>,
    <<"details">> := #{<<"key">> := Key, <<"low">> := Low, <<"high">> := High}}) ->
    ?ERROR_BAD_VALUE_NOT_IN_RANGE(Key, Low, High);

json_to_error(_, #{<<"id">> := <<"badValueNotAllowed">>,
    <<"details">> := #{<<"key">> := Key, <<"allowed">> := Allowed}}) ->
    ?ERROR_BAD_VALUE_NOT_ALLOWED(Key, Allowed);

json_to_error(_, #{<<"id">> := <<"badValueListNotAllowed">>,
    <<"details">> := #{<<"key">> := Key, <<"allowed">> := Allowed}}) ->
    ?ERROR_BAD_VALUE_LIST_NOT_ALLOWED(Key, Allowed);

json_to_error(_, #{<<"id">> := <<"badValueIdNotFound">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_ID_NOT_FOUND(Key);

json_to_error(_, #{
    <<"id">> := <<"badValueAmbiguousId">>,
    <<"details">> := #{<<"key">> := Key}
}) ->
    ?ERROR_BAD_VALUE_AMBIGUOUS_ID(Key);

json_to_error(_, #{<<"id">> := <<"badValueIdentifierOccupied">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_IDENTIFIER_OCCUPIED(Key);

json_to_error(_, #{<<"id">> := <<"badValueTokenType">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_BAD_TOKEN_TYPE(Key);

json_to_error(_, #{<<"id">> := <<"badValueIntentifier">>,
    <<"details">> := #{<<"key">> := Key}}) ->
    ?ERROR_BAD_VALUE_IDENTIFIER(Key);

json_to_error(_, #{<<"id">> := <<"badValueFullName">>}) ->
    ?ERROR_BAD_VALUE_FULL_NAME;

json_to_error(_, #{<<"id">> := <<"badValueUsername">>}) ->
    ?ERROR_BAD_VALUE_USERNAME;

json_to_error(_, #{<<"id">> := <<"badValuePassword">>}) ->
    ?ERROR_BAD_VALUE_PASSWORD;

json_to_error(_, #{<<"id">> := <<"badValueName">>}) ->
    ?ERROR_BAD_VALUE_NAME;

json_to_error(_, #{<<"id">> := <<"subdomainDelegationNotSupported">>}) ->
    ?ERROR_SUBDOMAIN_DELEGATION_NOT_SUPPORTED;

json_to_error(_, #{<<"id">> := <<"subdomainDelegationDisabled">>}) ->
    ?ERROR_SUBDOMAIN_DELEGATION_DISABLED;

json_to_error(_, #{<<"id">> := <<"basicAuthNotSupported">>}) ->
    ?ERROR_BASIC_AUTH_NOT_SUPPORTED;

json_to_error(_, #{<<"id">> := <<"basicAuthDisabled">>}) ->
    ?ERROR_BASIC_AUTH_DISABLED;

json_to_error(_, #{<<"id">> := <<"protectedGroup">>}) ->
    ?ERROR_PROTECTED_GROUP;

json_to_error(_, #{
    <<"id">> := <<"spaceNotSupportedBy">>,
    <<"details">> := #{<<"providerId">> := ProviderId}
}) ->
    ?ERROR_SPACE_NOT_SUPPORTED_BY(ProviderId);

json_to_error(_, #{
    <<"id">> := <<"viewNotExistsOn">>,
    <<"details">> := #{<<"providerId">> := ProviderId}
}) ->
    ?ERROR_VIEW_NOT_EXISTS_ON(ProviderId);

json_to_error(_, #{<<"id">> := <<"transferAlreadyEnded">>}) ->
    ?ERROR_TRANSFER_ALREADY_ENDED;

json_to_error(_, #{<<"id">> := <<"transferNotEnded">>}) ->
    ?ERROR_TRANSFER_NOT_ENDED;

json_to_error(_, #{<<"id">> := <<"relationDoesNotExist">>,
    <<"details">> := #{
        <<"childType">> := ChType, <<"childId">> := ChId,
        <<"parentType">> := ParType, <<"parentId">> := ParId}}) ->
    ChTypeAtom = binary_to_existing_atom(ChType, utf8),
    ParTypeAtom = binary_to_existing_atom(ParType, utf8),
    ?ERROR_RELATION_DOES_NOT_EXIST(ChTypeAtom, ChId, ParTypeAtom, ParId);

json_to_error(_, #{<<"id">> := <<"relationAlreadyExists">>,
    <<"details">> := #{
        <<"childType">> := ChType, <<"childId">> := ChId,
        <<"parentType">> := ParType, <<"parentId">> := ParId}}) ->
    ChTypeAtom = binary_to_existing_atom(ChType, utf8),
    ParTypeAtom = binary_to_existing_atom(ParType, utf8),
    ?ERROR_RELATION_ALREADY_EXISTS(ChTypeAtom, ChId, ParTypeAtom, ParId);

json_to_error(_, #{<<"id">> := <<"cannotDeleteEntity">>,
    <<"details">> := #{<<"entityType">> := EntityType, <<"entityId">> := EntityId}}) ->
    ?ERROR_CANNOT_DELETE_ENTITY(binary_to_existing_atom(EntityType, utf8), EntityId);

json_to_error(_, #{<<"id">> := <<"cannotAddRelationToSelf">>}) ->
    ?ERROR_CANNOT_ADD_RELATION_TO_SELF;

json_to_error(_, #{<<"id">> := <<"temporaryFailure">>}) ->
    ?ERROR_TEMPORARY_FAILURE;

json_to_error(_, #{<<"id">> := <<"badGuiPackage">>}) ->
    ?ERROR_BAD_GUI_PACKAGE;

json_to_error(_, #{<<"id">> := <<"guiPackageTooLarge">>}) ->
    ?ERROR_GUI_PACKAGE_TOO_LARGE;

json_to_error(_, #{<<"id">> := <<"guiPackageUnverified">>, <<"details">> := #{<<"shaSum">> := ShaSum}}) ->
    ?ERROR_GUI_PACKAGE_UNVERIFIED(ShaSum);

json_to_error(_, ErrorObject) ->
    ?ERROR_UNKNOWN_ERROR(ErrorObject).
