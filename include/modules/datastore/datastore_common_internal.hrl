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

-define(MOTHER_SCOPE_DEF_FUN, fun() -> links end).

-define(OTHER_SCOPES_DEF_FUN, fun() -> [] end).

%% This record shall not be used outside datastore engine and shall not be instantiated
%% directly. Use MODEL_CONFIG macro instead.
-record(model_config, {
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
    mother_link_scope = ?MOTHER_SCOPE_DEF_FUN :: links_utils:mother_scope_fun(),
    other_link_scopes = ?OTHER_SCOPES_DEF_FUN :: links_utils:other_scopes_fun()
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
        ?MOTHER_SCOPE_DEF_FUN, ?OTHER_SCOPES_DEF_FUN)).
-define(MODEL_CONFIG(Bucket, Hooks, StoreLevel, LinkStoreLevel, Transactions, SyncCache, ScopeFun1, ScopeFun2),
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
        mother_link_scope = ScopeFun1, % link_utils:mother_scope_fun()
        % Function that returns all scopes for links' operations
        other_link_scopes = ScopeFun2 % link_utils:other_scopes_fun()
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
    children = #{}
}).

-endif.
