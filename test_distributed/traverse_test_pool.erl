%%%-------------------------------------------------------------------
%%% @author Michal Wrzeszcz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains traverse exemplary pool callbacks used during tests.
%%% @end
%%%-------------------------------------------------------------------
-module(traverse_test_pool).
-author("Michal Wrzeszcz").

-include("global_definitions.hrl").

%% Pool callbacks
-export([do_master_job/2, do_slave_job/2, task_finished/1, update_job_progress/5, get_job/1]).
%% Helper functions
-export([get_slave_ans/1, get_node_slave_ans/2, get_expected/0]).

-define(POOL, <<"traverse_test_pool">>).

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

do_master_job({Master, 100, ID}, _) when ID == 100 ; ID == 101 ->
    timer:sleep(500),
    Master ! {stop, node()},
    timer:sleep(500),
    do_master_job_helper({Master, 100, 100});
do_master_job({Master, Num, ID}, _) ->
    do_master_job_helper({Master, Num, ID}).

do_master_job_helper({Master, Num, ID}) ->
    MasterJobs = case Num < 1000 of
                     true -> [{Master, 10 * Num, ID}, {Master, 10 * Num + 5, ID}];
                     _ -> []
                 end,

    SlaveJobs = [{Master, Num + 1, ID}, {Master, Num + 2, ID}, {Master, Num + 3, ID}],
    {ok, #{slave_jobs => SlaveJobs, master_jobs => MasterJobs}}.

do_slave_job({Master, Num, ID}, _) ->
    Master ! {slave, Num, ID, node()},
    ok.

task_finished(_) ->
    timer:sleep(1000),
    ok.

update_job_progress(ID0, Job, _, TaskID, waiting) when ID0 =:= undefined ; ID0 =:= main_job ->
    List = application:get_env(?CLUSTER_WORKER_APP_NAME, test_job, []),
    ID = list_to_binary(ref_to_list(make_ref())),
    application:set_env(?CLUSTER_WORKER_APP_NAME, test_job, [{ID, {Job, TaskID}} | List]),
    {ok, ID};
update_job_progress(ID0, Job, _, TaskID, on_pool) when ID0 =:= undefined ; ID0 =:= main_job ->
    ID = list_to_binary(ref_to_list(make_ref())),
    save_started_job(ID, Job, TaskID),
    {ok, ID};
update_job_progress(ID, Job, _, TaskID, on_pool) ->
    save_started_job(ID, Job, TaskID),
    {ok, ID};
update_job_progress(ID, _Job, _, _TaskID, Status) when Status =:= ended ; Status =:= canceled ->
    List = application:get_env(?CLUSTER_WORKER_APP_NAME, ongoing_job, []),
    application:set_env(?CLUSTER_WORKER_APP_NAME, ongoing_job, proplists:delete(ID, List)),
    {ok, ID}.

save_started_job(ID, Job, TaskID) ->
    List = application:get_env(?CLUSTER_WORKER_APP_NAME, ongoing_job, []),
    application:set_env(?CLUSTER_WORKER_APP_NAME, ongoing_job, [{ID, {Job, TaskID}} | proplists:delete(ID, List)]).

get_job(ID) ->
    Jobs = lists:foldl(fun(Node, Acc) ->
        Acc ++ rpc:call(Node, application, get_env, [?CLUSTER_WORKER_APP_NAME, test_job, []]) ++
            rpc:call(Node, application, get_env, [?CLUSTER_WORKER_APP_NAME, ongoing_job, []])
                       end, [], consistent_hashing:get_all_nodes()),
    {Job, TaskID} =  proplists:get_value(ID, Jobs, {undefined, undefined}),
    {ok, Job, ?POOL, TaskID}.

%%%===================================================================
%%% Helper functions
%%%===================================================================

get_slave_ans(AddID) ->
    get_node_slave_ans(undefined, AddID).

get_node_slave_ans(Node, AddID) ->
    receive
        {slave, Num, ID, AnsNode} when AnsNode =:= Node ; Node =:= undefined ->
            case AddID of
                true -> [{Num, ID} | get_node_slave_ans(Node, AddID)];
                _ -> [Num | get_node_slave_ans(Node, AddID)]
            end
    after
        10000 ->
            []
    end.

get_expected() ->
    Expected = [2,3,4,
        11,12,13,16,17,18,
        101,102,103,106,107,108,
        151,152,153,156,157,158,
        1001,1002,1003,1006,1007,1008,
        1051,1052,1053,1056,1057,1058,
        1501,1502, 1503,1506,1507,1508,
        1551,1552,1553,1556,1557, 1558],

    SJobsNum = length(Expected),
    MJobsNum = SJobsNum div 3,
    Description = #{
        slave_jobs_delegated => SJobsNum,
        slave_jobs_done => SJobsNum,
        slave_jobs_failed => 0,
        master_jobs_delegated => MJobsNum,
        master_jobs_done => MJobsNum
    },

    {Expected, Description}.