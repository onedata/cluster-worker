# Release notes for project cluster-worker


CHANGELOG
---------

### 3.0.0-alpha3

* VFS-1598, update of datastore update on cache
* VFS-1741 Split sync-gateway fetching and compilation.
* VFS-1670, Better support for synch cache in tests
* VFS-1670, Update cache cleaning
* VFS-1552 Adding finite stream test
* VFS-1670, Stress tests added
* VFS-1670, Use multiple records instead some_record during datastore tests


### 3.0.0-alpha2


* VFS-1581,  Minor cache update
* VFS-1234 Wait for CouchDB connection on datastore start
* VFS-1234 Ignore save's of documents that are up-to-date in couchdb_datastore_driver
* VFS-1581, Delete all links update.
* VFS-1234 Improve sync gateway restarter
* VFS-1234 Add links_key_to_doc_key/1 function to couchdb driver


### 2.0.1

* VFS-1528 Use monotonic time instead of system time
* VFS-1520 - all annotations deleted from ct_tests
* VFS-1528 Remove deprecated use of erlang:now/0


### 2.0.0


* Dependencies management update
* 1234 Use bundled sync-gateway
* Add vendor/sync_gateway submodule
* VFS-1234 make couchdb_datastore_driver default
* VFS-1405 lower the number of sync-gateways
* VFS-1405 update test env to use new DB driver
* Switch to Erlang 18.0
* VFS-1234 move dbsync related changes from op_worker
* VFS-1148 make sure applicaiton crashes if ports are not free
* VFS-1148 add port checking on application init
* VFS-1148 remove all code connected to http_worker
* VFS-1148 remove http_worker, move endpoints check to nagios
* VFS-1382 making ct work in cluster-worker
* VFS-1398 Reorganize xattr records definitions.
* VFS-1398 Add xattrs to onedata_file_api and cdmi_metadata implementation.
* VFS-1382 changing supervision tree
* VFS-1363 Default open mode to rdwr in translator
* VFS-1403 Remove usage of mimetype taken from attrs.
* VFS-1403 CDMI object PUT operation + tests.
* VFS-1407 Add mechanism that will remove inactive sessions after timeout.
* VFS-1397 Replace identity with auth.
* VFS-1338 Cdmi container put.
* VFS-1338 Cache file attrs.
* VFS-1363 Add user context to all storage_file_manager operations
* VFS-1341- handle deleting cdmi_object plus tests
* VFS-1341- space names added to Config in CT tests
* VFS-1289 Sending multiple events in one message.
* VFS-1382 removed nif helpers in cluster_worker
* VFS-1378 adjust to new ctool API
* Move storage creation to dedicated function.
* VFS-1382 integration of cluster_worker with op_worker - initial changes
* Create storages on provider.
* VFS-1382 op-worker related work removed from cluster-worker
* VFS-1382 node_manager config extracted
* VFS-1338 Implement mkdir operation, add tests of container creation to cdmi test
* VFS-1382 separated packages meant to form cluster repo
* VFS-1382 node_manager plugin - extracted behaviour & ported implementation
* Storage creation improvement
* VFS-1339 Move cdmi modules to different packages. Implement binary dir put callback.
* VFS-1218 check permissions while opening a file based on "open flags"
* VFS-1289 Add performance tests for events API.
* Fix pattern matching on maps.
* VFS-1289 Extend set of event and sequencer tests.
* VFS-1338 Extract api for protocol_plugins. Implement dir exists callback.
* VFS-1218 add lfm_utils:call_fslogic
* Refactor of malformed_request/2 and get_cdmi_capability/2.
* Map instead of dict.
* Include guard for cdmi_errors.hrl.
* Skeletons of capabilities handlers.
* VFS-1289 Extend event manager with client subscription mechanism.
* VFS-1327 Separate rest and cdmi as abstract protocol plugins.
* VFS-1291 Add routing to cdmi object/container modules and add some tests.
* Done users and groups; done getting token
* VFS-1291 Add rest pre_handler that deals with exceptions. Update ctool.
* VFS-1291 Rearrange http_worker modules hierarchy.
* VFS-1255 Bump Boost to 1.58 for compatibility with client.
* VFS-1218 merge delete_file with unlink
* VFS-1258, transactions skeleton
* VFS-1218 implement attributes and location notification
* VFS-1244 add possibility for client to update auth
* VFS-1218 fix lfm read/write test
* VFS-1242, Cache controller uses tasks
* VFS-1242, Task pool
* VFS-1242, Task manager skeleton
* VFS-1217 Use RoXeon/annotations.
* VFS-1218 add file_watcher model
* VFS-1194 add user context to StorageHelperCTX
* VFS-1193 better connection handling
* VFS-1194 initial helpers support
* VFS-1199, cache dump to disk management update
* VFS-1193 restart mcd_cluster after connection failure
* VFS-1199, forcing cache clearing once a period
* VFS-1199, Saving cache to disk status management
* VFS-1193 add configurable persistence driver
* VFS-1172, use botan on host machine rather than throw in so files
* VFS-1145 Integrate SSL2 into oneprovider.
* implement generic transactions in datastore ensure file_meta name uniqueness witihin its parent scope
* VFS-1178, Cache controller uses non-transactional saves
* move worker_host's state to ETS table
* use couchbase 4.0
* VFS-1147 Integration with new protocol.
* VFS-1147 Implementation of first operations on directories.
* add disable mnesia transactions option
* VFS-1129 Add deb build dependencies
* VFS-1118, local tests controller added
* implement mnesia links
* VFS-1118, global cache controller added
* VFS-1118, cache clearing skeleton
* VFS-1115 Allow building RPM package.
* VFS-1025, merge lb with develop
* VFS-1053 Selecting explicit node for mnesia to join, instead of finding it randomly
* VFS-1049 add check_permissions annotation
* VFS-1049 add initial fslogic file structure
* VFS-1051 change worker startup order
* implement datastore: 'delete with predicates' and list
* VFS-997 Add event stream periodic emission ct test.
* VFS-997 Add event stream crash ct test.
* VFS-997 Event manager ct test.
* VFS-997 Add event utils and unit test.
* VFS-1041, add send data endpoint to remote control
* checking endpoints during healthcheck of http_worker and dns_worker
* VFS-997 Change sequencer manager connection logic.
* move session definitions to separate header
* change location of message_id header
* extract certificate_info to separate header
* client_communicator lib
* VFS-1000, add logical and storage file manager's API design
* oneproxy CertificateInfo message
* new handshake
* VFS-1000, add sequence support for response mocking
* VFS-997 Add sequencer worker.
* translation improvements
* serialization improvements
* VFS-1010 Make test master node discoverable through DNS.
* client_auth + basic integration with protobuf
* VFS-997 Add sequencer dispatcher ct test.
* VFS-997 Sequencer logic.
* VFS-997 Add sequencer.
* move datastore init to node_manager
* change created beam location to target dir
* refactor worker_host header
* add input_dir/target_dir configuration
* enable init_cluster triggering when all nodes have appeared
* rest/ccdmi function headers
* remove request_dispatcher.hrl
* remove node_manager.hrl
* node_manager refactoring
* oneprovider app reformat + doc adjustment
* http_worker reformat + doc adjustment
* redirector reformat + doc adjustment
* session_logic and n2o_handler reformat + doc adjustment
* rest_handler reformat + doc adjustment
* cdmi_handler reformat + doc adjustment
* dns_worker reformat + doc adjustment
* logger_plugin reformat + doc adjustment
* worker_plugin_behavior reformat + doc adjustment
* worker_host reformat + doc adjustment
* client_handler and provider_handler reformat + doc adjustment
* request_dispatcher reformat + doc adjustment
* oneproxy reformat + doc adjustment
* gsi_nif reformat + doc adjustment
* gsi_handler reformat + doc adjustment
* node_manager_listener_starter reformat + doc adjustment
* node_manager reformat + doc adjustment
* cluster manager reformat + doc adjustment
* VFS-965, full functionality of spaces page
* Perform operations asynchronously in ws_handler.
* VFS-965, several funcionalities of page spaces
* VFS-965, visial aspects of spaces page
* VFS-965, first code for spaces page
* VFS-959 Not sending notifications for a fuse that modifies a file.
* set fuseID to CLUSTER_FUSE_ID during creation of file_location
* VFS-954, adjust to new file blocks API
* setting fslogic context
* VFS-939 Implement rtransfer.
* VFS-954, implementation of data distribution panel
* VFS-952 support for AttrUnsubscribe message
* VFS-593, GR push channel messages handling
* getting size from available blocks map, instead of storage
* creating file location for remote files
* creating file location for empty remote files moved to get_file_location
* VFS-940 Subscribing for container state events.
* VFS-940 Add rt_map specialization.
* informing client about available blocks
* VFS-940 Add provider id to rt_block + clang-format.
* VFS-940 Add rt_container abstraction.
* VFS-939 Basic draft of rtransfer worker.
* add get_file_size api
* VFS-937 Saving provider ID in CCM state.
* VFS-937 Add Global Registry channel.
* register for db_sync changes
* VFS-919 Module monitoring lifecycle.
* VFS-889 first working dbsync prototype based on BigCouch long poll Rest API
* remote location module - new data structure and basic api for sync purposes
* VFS-896 Redesign communication layer of the Gateway module.
* VFS-900 Change client download instructions.
* VFS-818 Copy authorization data for  onepanel.
* VFS-895 Add RPM package install files progress indicator.
* VFS-886, add radio buttons
* VFS-881 Add groups management.
* VFS-881 Add space privileges management page.
* VFS-880 special characters in cdmi, + some minor fixes
* VFS-881 Using privileges to enable/disable user actions.
* VFS-867 Set RPM package version.
* VFS-886, modify chmod panel to include ACLs
* VFS-888 Add file_block DAO record and move file_location into separate documents.
* doc update
* checking perms in cdmi
* checking acl perms in storge_files_manager
* VFs-859 Spaces and tokens web pages refactoring.
* VFS-676 Update GRPCA.
* VFS-855, change buttons to link to make them work without websocket
* VFS-855, add download_oneclient page
* send access token hash to user
* VFS-828 Allow user authentication through HTTP headers.
* Getting and setting user metadata for CDMI.
* Add user matadata to file attrs
* VFS-829: improve error recovery while moving files between spaces



### 1.6.0



* Security mechanism against attack for atoms table added
* Invalid use of WebGUI cache fixed

### 1.5.0


* WebGUI and FUSE client handler can use different certificates.
* Xss and csrf protection mechanisms added.
* Attack with symbolic links is not possible due to security mechanism update.



### 1.0.0


* support multiple nodes deployment, automatically discover cluster structure and reconfigure it if needed.
* handle requests from FUSE clients to show location of needed data. 
* provide needed data if storage system where data is located is not connected to client.
* provide Web GUI for users which offers data and account management functions. Management functions include certificates management.
* provide Web GUI for administrators which offers monitoring and logs preview (also Fuse clients logs).
* provide users' authentication via OpenID and certificates.
* provide rule management subsystem (version 1.0).
* reconfigure *oneclient* using callbacks.

________

Generated by sr-release. 
