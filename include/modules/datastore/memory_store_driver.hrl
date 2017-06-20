%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc This header defines records used by memory_store_driver.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(MEMORY_STORE_DRIVER_HRL).
-define(MEMORY_STORE_DRIVER_HRL, 1).

-record(state, {
  key :: datastore:ext_key(),
  link_proc = false :: boolean(),
  cached = false :: boolean(),
  master_pid :: pid(),
  last_ctx :: undefined | datastore_context:ctx()
}).

-endif.
