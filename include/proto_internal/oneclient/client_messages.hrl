%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Internal version of client protocol message.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(CLIENT_MESSAGES_HRL).
-define(CLIENT_MESSAGES_HRL, 1).

-include("cluster_elements/protocol_handler/credentials.hrl").

-record(client_message, {
    message_id :: non_neg_integer(),
    stream_id :: non_neg_integer(),
    seq_num :: non_neg_integer(),
    last_message :: boolean(),
    credentials :: #credentials{},
    client_message :: tuple()
}).

-endif.
