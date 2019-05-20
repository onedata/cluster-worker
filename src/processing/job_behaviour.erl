%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% It is the behaviour of job executes by traverse module.
%%% @end
%%%-------------------------------------------------------------------
-module(job_behaviour).
-author("Michal Wrzeszcz").

%%--------------------------------------------------------------------
%% @doc
%% Executes master job.
%% @end
%%--------------------------------------------------------------------
-callback do_master_job(traverse:job()) ->
    {ok, [SlaveJob :: traverse:job()], [MasterJob :: traverse:job()]} |
    {ok, [SlaveJob :: traverse:job()], [MasterJob :: traverse:job()], traverse:description()} |
    {error, term()}.

%%--------------------------------------------------------------------
%% @doc
%% Executes slave job.
%% @end
%%--------------------------------------------------------------------
-callback do_slave_job(traverse:job()) -> ok | {ok, traverse:description()} | {error, term()}.

%%--------------------------------------------------------------------
%% @doc
%% Is executed when whole task is finished.
%% @end
%%--------------------------------------------------------------------
-callback task_finished(traverse:id()) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Saves information about master job.
%% @end
%%--------------------------------------------------------------------
-callback save_job(traverse:job(), traverse:job_status()) -> {ok, traverse:job_id()}  | {error, term()}.