%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides functions for handling with document revisions.
%%% @end
%%%-------------------------------------------------------------------
-module(datastore_rev).
-author("Krzysztof Trzepla").

%% API
-export([new/1, parse/1, is_greater/2]).

% a positive integer that increases with every modification made to a document
-type generation() :: binary().
% concatenation of the document generation and a random hash
-type rev() :: binary().

-export_type([rev/0]).

-define(REV_LENGTH, application:get_env(cluster_worker, datastore_doc_rev_length, 16)).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a new revision with provided generation and a random hash.
%% @end
%%--------------------------------------------------------------------
-spec new(generation()) -> rev().
new(Generation) ->
    Hash = str_utils:rand_hex(?REV_LENGTH),
    <<(integer_to_binary(Generation))/binary, "-", Hash/binary>>.

%%--------------------------------------------------------------------
%% @doc
%% Returns generation and hash of provided revision.
%% @end
%%--------------------------------------------------------------------
-spec parse(rev()) -> {generation(), binary()}.
parse(Rev) ->
    [Generation, Hash] = binary:split(Rev, <<"-">>),
    {binary_to_integer(Generation), Hash}.

%%--------------------------------------------------------------------
%% @doc
%% Returns 'true' if Rev1 is greater than Rev2, otherwise 'false'.
%% @end
%%--------------------------------------------------------------------
-spec is_greater(rev(), rev()) -> boolean().
is_greater(Rev1, Rev2) ->
    {Gen1, Hash1} = parse(Rev1),
    {Gen2, Hash2} = parse(Rev2),
    case {Gen1 > Gen2, Gen1 < Gen2, Hash1 > Hash2, Hash1 < Hash2} of
        {true, false, _, _} -> true;
        {false, true, _, _} -> false;
        {false, false, true, false} -> true;
        {false, false, false, true} -> false;
        % TODO VFS-4145 - change to false when remote driver flushes documents
        {false, false, false, false} -> true
    end.
