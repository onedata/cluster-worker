%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Defines common macros and records that are used within whole datastore module.
%%%      This header shall not be included directly by any erl file.
%%% @end
%%%-------------------------------------------------------------------
-ifndef(DATASTORE_COMMON_INTERNAL_HRL).
-define(DATASTORE_COMMON_INTERNAL_HRL, 1).

%% Levels
-define(DISK_ONLY_LEVEL, disk_only).
-define(GLOBAL_ONLY_LEVEL, global_only).
-define(LOCAL_ONLY_LEVEL, local_only).
-define(GLOBALLY_CACHED_LEVEL, globally_cached).
-define(LOCALLY_CACHED_LEVEL, locally_cached).

-define(DEFAULT_STORE_LEVEL, ?GLOBALLY_CACHED_LEVEL).

%% Name of local only link scope (that shall not be synchronized)
%% This link scope always handles read operation like fetch and foreach
%% All write operations on other scopes all replicated to this scope
-define(LOCAL_ONLY_LINK_SCOPE, <<"#$LOCAL$#">>).

-define(DEFAULT_LINK_REPLICA_SCOPE, ?LOCAL_ONLY_LINK_SCOPE).

%% This record shall not be used outside datastore engine and shall not be instantiated
%% directly. Use MODEL_CONFIG macro instead.
-record(model_config, {
    version = 1 :: non_neg_integer(),
    name :: model_behaviour:model_type(),
    size = 0 :: non_neg_integer(),
    fields = [],
    defaults = {},
    hooks = [] :: [{model_behaviour:model_type(), model_behaviour:model_action()}],
    bucket :: datastore:bucket(),
    store_level = ?DEFAULT_STORE_LEVEL :: datastore:store_level(),
    link_store_level = ?DEFAULT_STORE_LEVEL :: datastore:store_level(),
    transactional_global_cache = true :: boolean(),
    sync_cache = false :: boolean(),
    link_replica_scope = ?DEFAULT_LINK_REPLICA_SCOPE :: links_utils:link_replica_scope(),
    link_duplication = false :: boolean(),
    sync_enabled = false :: boolean(),
    aggregate_db_writes = false :: boolean(),
    disable_remote_link_delete = false :: boolean()
}).

%% Helper macro for instantiating #model_config record.
%% Bucket           :: see #model_config.bucket
%% Hooks            :: see #model_config.hooks
%% StoreLevel       :: see #model_config.store_level (optional)
%% LinkStoreLevel   :: see #model_config.link_store_level (optional)
-define(MODEL_CONFIG(Bucket, Hooks), ?MODEL_CONFIG(Bucket, Hooks, ?DEFAULT_STORE_LEVEL, ?DEFAULT_STORE_LEVEL)).
-define(MODEL_CONFIG(Bucket, Hooks, StoreLevel), ?MODEL_CONFIG(Bucket, Hooks, StoreLevel, StoreLevel)).
-define(MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel),
    ?MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel, true)).
-define(MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel, Transactions),
    ?MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel, Transactions, false)).
-define(MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel, Transactions, SyncCache),
    ?MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel, Transactions, SyncCache,
        ?DEFAULT_LINK_REPLICA_SCOPE)).
-define(MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel, Transactions, SyncCache, LinkReplicaScope),
    ?MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel, Transactions, SyncCache, LinkReplicaScope, false)).
-define(MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel, Transactions, SyncCache, LinkReplicaScope, LinkDuplication),
    #model_config{
        name = ?MODULE,
        size = record_info(size, ?MODULE),
        fields = record_info(fields, ?MODULE),
        defaults = #?MODULE{},
        bucket = Bucket,
        hooks = Hooks,
        store_level = StoreLevel,
        link_store_level = LinkStoreLevel,
        transactional_global_cache = Transactions,
        sync_cache = SyncCache,
        % Function that returns scope for local operations on links
        link_replica_scope = LinkReplicaScope, % link_utils:mother_scope_fun()
        % Function that returns all scopes for links' operations
        link_duplication = LinkDuplication, % Allows for multiple link targets via datastore:add_links function
        sync_enabled = false % Models with sync enabled will be stored in non-default bucket to reduce DB load.
    }
).

%% Max link map size in single links record
-define(LINKS_MAP_MAX_SIZE, 32).
%% Number of children owned by each link record
-define(LINKS_TREE_BASE, 128).

%% Internal semi-model used by document that holds links between documents
-record(links, {
    doc_key,
    model,
    link_map = #{},
    children = #{},
    origin = ?LOCAL_ONLY_LINK_SCOPE %% Scope that is an origin to this link record
}).

%% Separator for link name and its scope
-define(LINK_NAME_SCOPE_SEPARATOR, "@provider@").

%% Special prefix for keys of documents that shall not be persisted in synchronized bucket
%% even if its model config says otherwise.
-define(NOSYNC_KEY_OVERRIDE_PREFIX, <<"nosync_">>).

-define(NOSYNC_WRAPPED_KEY_OVERRIDE(KEY), {nosync, KEY}).

%% Encoded record name field
-define(RECORD_TYPE_MARKER, "<record_type>").

%% Encoded record version field
-define(RECORD_VERSION_MARKER, "<record_version>").


-endif.
