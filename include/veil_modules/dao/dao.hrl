%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: dao header
%% @end
%% ===================================================================

-include_lib("records.hrl").
-include_lib("veil_modules/dao/dao_vfs.hrl").

%% record definition used in record registration example
-record(some_record, {field1 = "", field2 = "", field3 = ""}).

%% Helper macro. See macro ?dao_record_info/1 for more details.
-define(record_info_gen(X), {record_info(size, X), record_info(fields, X), #X{}}).

%% Every record that will be saved to DB have to be "registered" with this define.
%% Each registered record should be listed in defined below 'case' block as fallow:
%% record_name -> ?record_info_gen(record_name);
%% when 'record_name' is name of the record. 'some_record' is an example.
-define(dao_record_info(R),
    case R of
        some_record -> ?record_info_gen(some_record);
        cm_state -> ?record_info_gen(cm_state);
        host_state -> ?record_info_gen(host_state);
        node_state -> ?record_info_gen(node_state);
        file -> ?record_info_gen(file);
        file_location -> ?record_info_gen(file_location);
        file_descriptor -> ?record_info_gen(file_descriptor);
        file_meta -> ?record_info_gen(file_meta);
        file_lock -> ?record_info_gen(file_lock);
        %next_record -> ?record_info_gen(next_record);
        _ -> {error, unsupported_record}
    end).


%% Structure representing full document.
%% `uuid` is document UUID, `rev_info` is documents' current revision number
%% `record` is an record representing this document (its data) and `force_update` is a flag
%% that forces dao:save_record/1 to update this document even if rev_info isn't valid or up to date.
-record(document, {uuid = "", rev_info = 0, record = none, force_update = false}).

%% DB constants
-define(SYSTEM_DB_NAME, "system_data").
-define(RECORD_INSTANCES_DOC_PREFIX, "record_instances_").
-define(RECORD_FIELD_BINARY_PREFIX, "__bin__: ").
-define(RECORD_FIELD_ATOM_PREFIX, "__atom__: ").
-define(RECORD_FIELD_PID_PREFIX, "__pid__: ").
-define(RECORD_TUPLE_FIELD_NAME_PREFIX, "tuple_field_").
-define(RECORD_META_FIELD_NAME, "record__").