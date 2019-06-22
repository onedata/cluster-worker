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
-export([do_master_job/1, do_slave_job/1, task_finished/1, update_job_progress/5, get_job/1]).
%% Helper functions
-export([get_slave_ans/1, get_node_slave_ans/2]).

-define(POOL, <<"traverse_test_pool">>).

%%%===================================================================
%%% Pool callbacks
%%%===================================================================

do_master_job({Master, 100, ID}) when ID == 100 ; ID == 101 ->
    timer:sleep(500),
    Master ! {stop, node()},
    timer:sleep(500),
    do_master_job_helper({Master, 100, 100});
do_master_job({Master, Num, ID}) ->
    do_master_job_helper({Master, Num, ID}).

do_master_job_helper({Master, Num, ID}) ->
    MasterJobs = case Num < 1000 of
                     true -> [{Master, 10 * Num, ID}, {Master, 10 * Num + 5, ID}];
                     _ -> []
                 end,

    SlaveJobs = [{Master, Num + 1, ID}, {Master, Num + 2, ID}, {Master, Num + 3, ID}],
    {ok, #{slave_jobs => SlaveJobs, master_jobs => MasterJobs}}.

do_slave_job({Master, Num, ID}) ->
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
                       end, [], consistent_hasing:get_all_nodes()),
    {Job, TaskID} =  proplists:get_value(ID, Jobs, {undefined, undefined}),
    {ok, Job, ?POOL, TaskID}.

%%%===================================================================
%%% Helper functions
%%%===================================================================

get_slave_ans(AddID) ->
    get_node_slave_ans(node(), AddID).

get_node_slave_ans(Node, AddID) ->
    receive
        {slave, Num, ID, Node} ->
            case AddID of
                true -> [{Num, ID} | get_node_slave_ans(Node, AddID)];
                _ -> [Num | get_node_slave_ans(Node, AddID)]
            end
    after
        10000 ->
            []
    end.