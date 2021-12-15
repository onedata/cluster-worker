%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros and records defining messages used by partition execution service.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(PES_PROTOCOL_HRL).
-define(PES_PROTOCOL_HRL, 1).


%%%===================================================================
%%% Type of calls to pes_server
%%%===================================================================

-define(PES_SYNC_CALL(Request), {pes_sync_call, Request}).
-define(PES_ASYNC_CALL(Request), {pes_async_call, Request}).
-define(PES_ASYNC_CALL_IGNORE_ANS(Request), {pes_async_call_ignore_ans, Request}).


%%%===================================================================
%%% Wrapping used to send requests from pes_server to pes_server_slave
%%%===================================================================

-record(pes_slave_request, {
    request :: term(),
    handler :: pes_server:request_handler(),
    from :: {pid(), Tag :: term()} | undefined % filed used to send answer to calling process - undefined when
                                               % answer is not required to be sent
}).


-record(pes_slave_request_batch, {
    requests :: [pes_server_slave:pes_slave_request()]
}).


-endif.
