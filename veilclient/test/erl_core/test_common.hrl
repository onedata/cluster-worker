%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This header provides commonly used definitions in all integration tests 
%%       (mainly in clusters' setup/teardown methods)         
%% @end
%% ===================================================================
-ifndef(TEST_COMMON_HRL).
-define(TEST_COMMON_HRL, 1).


-define(INFO(X, Y), io:format(X ++ "~n", Y)).
-define(CCM, central_cluster_manager).
-define(Dispatcher_Name, request_dispatcher).
-define(Modules_With_Args, [{central_logger, []}, {cluster_rengine, []}, {control_panel, []}, {dao, []}, {fslogic, []}, {gateway, []}, {rtransfer, []}, {rule_manager, []}, {dns_worker, []}, {remote_files_manager, []}]).


-define(COMMON_FILES_ROOT_VAR, "COMMON_FILES_ROOT").
-define(TEST_ROOT_VAR, "TEST_ROOT").
-define(LOCAL_COMMON_FILES_DIR, os:getenv(?TEST_ROOT_VAR) ++ "/test/integration_tests/common_files").

%% Returns absolute path to file from given path relative to "common_files" dir (only for cluster side calls)
-define(COMMON_FILE(X), os:getenv(?COMMON_FILES_ROOT_VAR) ++ "/" ++ X).

%% Returns absolute path to file from given path relative to "common_files" dir (only for client side calls)
-define(LOCAL_COMMON_FILE(X), ?LOCAL_COMMON_FILES_DIR ++ "/" ++ X).

-endif. %% TEST_COMMON_HRL