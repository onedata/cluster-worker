%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains definitions of names used to identify
%% different parts of application (or whole application).
%% @end
%% ===================================================================


%% Name of the application.
-define(APP_Name, veil_cluster_node).

%% Global name of gen_server that provides ccm functionality.
-define(CCM, central_cluster_manager).

%% Local name (name and node is used to identify it) of gen_server that 
%% coordinates node life cycle.
-define(Node_Manager_Name, node_manager).

%% Local name (name and node is used to identify it) of supervisor that 
%% coordinates application at each node (one supervisor per node).
-define(Supervisor_Name, sup_name).
