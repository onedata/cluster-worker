%%%--------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This header contains definitions of transaction process records and macros.
%%% @end
%%%--------------------------------------------------------------------
-ifndef(TP_HRL).
-define(TP_HRL, 1).

% data           - initial transaction process data
% idle_timeout   - how long transaction process can be in committed state
%                  before termination
% commit_delay   - minimal delay between consecutive commit operations
-record(tp_init, {
    data :: tp:data(),
    idle_timeout :: timeout(),
    min_commit_delay :: timeout(),
    max_commit_delay :: timeout()
}).

-define(TP_ROUTER_SUP, tp_router_sup).
-define(TP_ROUTING_TABLE, tp_routing_table).

-endif.