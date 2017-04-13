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
  driver :: atom(),
  flush_driver :: atom(),
  model_config :: model_behaviour:model_config(),
  key :: datastore:ext_key(),
  current_value :: memory_store_driver:value_doc() | memory_store_driver:value_link(),
  link_proc = false :: boolean(),
  revisions_to_save :: memory_store_driver:revision_info()
}).

-endif.
