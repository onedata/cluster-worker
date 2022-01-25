%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Macros and records defining messages used by PES.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(PES_PROTOCOL_HRL).
-define(PES_PROTOCOL_HRL, 1).


%%%===================================================================
%%% Type of messages to pes_server
%%%===================================================================

-define(PES_CALL(Request), {pes_call, Request}).
-define(PES_SUBMIT(Request), {pes_submit, Request}).
-define(PES_ACKNOWLEDGED_CAST(Request), {pes_acknowledged_cast, Request}).
-define(PES_CAST(Request), {pes_cast, Request}).
-define(PES_SELF_CAST(Request), {pes_self_cast, Request}).


%%%===================================================================
%%% Macro for async answers
%%%===================================================================

-define(SUBMIT_RESULT(Tag, Response), {submit_result, Tag, Response}).


%%%===================================================================
%%% Wrappers used to send requests from pes_server to pes_server_slave
%%%===================================================================

-record(pes_slave_task, {
    request :: term(),
    callback :: pes_server:execution_callback(),
    from :: {pid(), Tag :: term()} | undefined % field used to send answer to calling process - undefined when
                                               % answer is not required to be sent ; the field is set using
                                               % argument `From` of gen_server:handle_call function
}).


-record(pes_slave_task_batch, {
    tasks :: [pes_server_slave:pes_slave_task()]
}).


-endif.
