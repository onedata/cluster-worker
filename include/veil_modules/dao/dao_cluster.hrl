%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: dao_cluster header
%% @end
%% ===================================================================

%% This record contains environmental variables send by FUSE client
%% Variables are stored in 'env_vars' list. Entry format: {Name :: atom(), Value :: string()}
-record(fuse_session, {uid, hostname = "", env_vars = [], valid_to = 0}).

%% This record represents single FUSE connection and its location.
-record(connection_info, {session_id, controlling_node, controlling_pid}).