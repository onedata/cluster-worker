%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Models definitions.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(DATASTORE_MODELS_HRL).
-define(DATASTORE_MODELS_HRL, 1).

%% Wrapper for all models' records
-record(document, {
    key :: datastore:key(),
    rev :: term(),
    value :: datastore:value(),
    links :: term()
}).

%% Models' definitions

%% sample model with example fields
-record(some_record, {
    field1 :: term(),
    field2 :: term(),
    field3 :: term()
}).

%% event manager model:
%% value - mapping from subscription ID to subscription
-record(subscription, {
    value :: event_manager:subscription()
}).

%% Identity containing user_id
-record(identity, {
    user_id :: onedata_user:id()
}).

%% session:
%% identity - user identity
-record(session, {
    identity :: #identity{},
    node :: node(),
    session_sup :: pid(),
    event_manager :: pid(),
    sequencer_manager :: pid(),
    communicator :: pid()
}).

%% Local, cached version of globalregistry user
-record(onedata_user, {
    name :: binary()
}).


-record(file, {
    name :: binary(),
    mode :: non_neg_integer()
}).

-endif.