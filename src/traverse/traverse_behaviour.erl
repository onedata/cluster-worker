%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% It is the behaviour of traverse callback module (see traverse.erl).
%%% It defines way of handling of single traverse task and all jobs connected with it.
%%% @end
%%%-------------------------------------------------------------------
-module(traverse_behaviour).
-author("Michal Wrzeszcz").

-optional_callbacks([task_started/2, task_finished/2, task_canceled/2, on_cancel_init/2,
    task_restart_after_node_crash/2, get_sync_info/1, get_timestamp/0, to_string/1]).

%%%===================================================================
%%% Traverse API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Executes master job.
%% @end
%%--------------------------------------------------------------------
-callback do_master_job(traverse:job(), traverse:master_job_extended_args()) ->
    {ok, traverse:master_job_map()} | {error, term()}.

%%--------------------------------------------------------------------
%% @doc
%% Executes slave job.
%% @end
%%--------------------------------------------------------------------
-callback do_slave_job(traverse:job(), traverse:id()) -> ok | {ok, traverse:description()} | {error, term()}.

%%%===================================================================
%%% Job persistence API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Saves information about master job. Callback module is responsible for management of jobs and generation of ids.
%% When job is persisted first time the key is undefined or main_job (for first job used to init task).
%% @end
%%--------------------------------------------------------------------
-callback update_job_progress(undefined | main_job | traverse:job_id(),
    traverse:job(), traverse:pool(), traverse:id(), traverse:job_status()) ->
    {ok, traverse:job_id()}  | {error, term()}.

%%--------------------------------------------------------------------
%% @doc
%% Gets information about master job.
%% @end
%%--------------------------------------------------------------------
-callback get_job(traverse:job_id()) ->
    {ok, traverse:job(), traverse:pool(), traverse:id()}  | {error, term()}.

%%%===================================================================
%%% Optional task lifecycle API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Is executed when whole task is started.
%% @end
%%--------------------------------------------------------------------
-callback task_started(traverse:id(), traverse:pool()) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Is executed when whole task is finished.
%% @end
%%--------------------------------------------------------------------
-callback task_finished(traverse:id(), traverse:pool()) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Is executed when last job of canceled task has ended or immediately after cancel if task is scheduled.
%% @end
%%--------------------------------------------------------------------
-callback task_canceled(traverse:id(), traverse:pool()) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Is executed to allow user-defined init of cancel procedure (e.g., to send cancel signal to task related processes).
%% @end
%%--------------------------------------------------------------------
-callback on_cancel_init(traverse:id(), traverse:pool()) -> ok.

%%--------------------------------------------------------------------
%% @doc
%% Is executed after node crash to decide if task should be restarted or cancelled.
%% @end
%%--------------------------------------------------------------------
-callback task_restart_after_node_crash(traverse:id(), traverse:pool()) ->
    traverse:action_on_restart_after_node_crash().

%%%===================================================================
%%% Optional job sync and queuing API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Provides sync info that extends datastore context (to sync task documents between environments).
%% @end
%%--------------------------------------------------------------------
-callback get_sync_info(traverse:job()) -> {ok, traverse:sync_info()}  | {error, term()}.

%%--------------------------------------------------------------------
%% @doc
%% Returns timestamp to be added to task. Used for tasks listing.
%% @end
%%--------------------------------------------------------------------
-callback get_timestamp() -> {ok, traverse:timestamp()}.

%%%===================================================================
%%% Optional debug API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns term that will be used to describe job in lagger logs.
%% @end
%%--------------------------------------------------------------------
-callback to_string(traverse:job()) -> binary() | atom() | iolist().