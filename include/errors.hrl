%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2015, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc This header file contains declarations of errors used across the project.
%% @end
%% ===================================================================

% TODO This module should list all errors that occur in the project
% Errors listed here should be based on POSIX and be used on all levels of API.
% For example
% -define(EACCESS, eaccess).

% Errors generated by storage_file_manager
% Errors that occur in storage_file_manager should all be reported as one type of error (%todo - think of a name for this error)
-define(SFM_ERROR, sfm_error).

% This define contains list of ErrorType, ErrorMessag pairs. ErrorMessage should be a human-readable string.
-define(ERROR_DESCRIPTIONS, [
    {?SFM_ERROR, "filesystem error"}
]).

% This macro allows turning error id into a human-readable string.
-define(ERROR_TO_STR(_ErrorID),
    proplists:get_value(_ErrorID, ?ERROR_DESCRIPTIONS)
).