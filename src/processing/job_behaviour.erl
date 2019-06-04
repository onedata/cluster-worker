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

%%%===================================================================
%%% Traverse API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Executes master job.
%% @end
%%--------------------------------------------------------------------
-callback do_master_job(traverse:job()) -> {ok, traverse:master_job_map()} | {error, term()}.

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
% TODO - dodac callback on_task_start (catchowac jak go nie ma - to samo z finished)
-callback task_finished(traverse:id()) -> ok.

%%%===================================================================
%%% Job persistence API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves information about master job.
%% @end
%%--------------------------------------------------------------------
-callback save_job(traverse:job_id(), traverse:job(), traverse:pool(), traverse:id(), traverse:job_status()) ->
    {ok, traverse:job_id()}  | {error, term()}.

%%--------------------------------------------------------------------
%% @doc
%% Gets information about master job.
%% @end
%%--------------------------------------------------------------------
-callback get_job(traverse:job_id()) ->
    {ok, traverse:job(), traverse:pool(), traverse:id()}  | {error, term()}.

%%--------------------------------------------------------------------
%% @doc
%% Lists ongoing master jobs.
%% @end
%%--------------------------------------------------------------------
-callback list_ongoing_jobs() ->
    {ok, [traverse:job_id()]}  | {error, term()}.

%%%===================================================================
%%% Job sync and queuing API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Provides sync info that extends datastore context.
%% @end
%%--------------------------------------------------------------------
-callback get_sync_info(traverse:job()) ->
    {ok, datastore:ctx()}  | {error, term()}.

%%--------------------------------------------------------------------
%% @doc
%% Returns timestamp to be added to task. Used for tasks listing.
%% @end
%%--------------------------------------------------------------------
-callback get_timestamp() ->
    {ok, traverse:timestamp()}.