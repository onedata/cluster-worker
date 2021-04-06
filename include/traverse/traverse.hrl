%%%-------------------------------------------------------------------
%%% @author Michał Wrzeszcz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros and records used by
%%% traverse mechanism.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(TRAVERSE_HRL).
-define(TRAVERSE_HRL, 1).

% record containing chosen fields of #traverse_task{} record
% that are required to start task
-record(task_execution_info, {
    callback_module :: traverse:callback_module(),
    executor :: traverse:environment_id(),
    main_job_id :: traverse:job_id(),
    node :: undefined | node(),
    start_time = 0 :: traverse:timestamp(),
    single_master_job_mode :: boolean()
}).



-endif.