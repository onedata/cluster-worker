%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license 
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides set of path processing methods.
%% @end
%% ===================================================================
-module(fslogic_path).
-author("Rafal Slota").

-include("veil_modules/dao/dao.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("logging.hrl").

%% API
-export([get_user_file_name/1, get_user_file_name/2]).
-export([get_full_file_name/1, get_full_file_name/2, get_full_file_name/4]).
-export([verify_file_name/1]).
-export([strip_path_leaf/1, basename/1]).
-export([get_parent_and_name_from_path/2]).
-export([get_user_root/0, get_user_root/2, get_user_root/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% get_user_file_name/1
%% ====================================================================
%% @doc Gets user's file name. This method reverses get_full_file_name.
%%      Throws on error (e.g. file path was invalid). <br/>
%%      Note: this method requires user context! Without it you'll get input path.
%% @end
-spec get_user_file_name(FullFileName :: string()) -> Result when
    Result :: UserFileName :: string() | no_return().
%% ====================================================================
get_user_file_name(FullFileName) ->
    {_, UserDoc} = fslogic_objects:get_user(),
    get_user_file_name(FullFileName, UserDoc).


%% get_user_file_name/2
%% ====================================================================
%% @doc Gets user's file name. This method reverses get_full_file_name.
%%      Throws on error (e.g. file path was invalid). <br/>
%%      Uses UserDoc as context.
%% @end
-spec get_user_file_name(FullFileName :: string(), UserDoc :: #veil_document{}) -> Result when
    Result :: UserFileName :: string() | no_return().
%% ====================================================================
get_user_file_name(FullFileName, UserDoc) ->
    {ok, Tokens} = verify_file_name(FullFileName),

    UserRec = dao_lib:strip_wrappers(UserDoc),
    UserName = UserRec#user.login,

    case Tokens of
        [UserName | UserTokens] -> "/" ++ string:join(UserTokens, "/");
        _ -> "/" ++ string:join(Tokens, "/")
    end.


%% get_full_file_name/1
%% ====================================================================
%% @doc Gets file's full name (user's root is added to name, but only when asking about non-group dir).
%% @end
-spec get_full_file_name(FileName :: string()) -> Result when
    Result :: {ok, FullFileName} | {error, ErrorDesc},
    FullFileName :: string(),
    ErrorDesc :: atom.
%% ====================================================================

get_full_file_name(FileName) ->
    get_full_file_name(FileName, cluster_request).

%% get_full_file_name/2
%% ====================================================================
%% @doc Gets file's full name (user's root is added to name, but only when asking about non-group dir).
%% @end
-spec get_full_file_name(FileName :: string(), Request :: atom()) -> Result when
    Result :: {ok, FullFileName} | {error, ErrorDesc},
    FullFileName :: string(),
    ErrorDesc :: atom.
%% ====================================================================

get_full_file_name(FileName, Request) ->
    {UserDocStatus, UserDoc} = fslogic_objects:get_user(),
    get_full_file_name(FileName, Request, UserDocStatus, UserDoc).

%% get_full_file_name/4
%% ====================================================================
%% @doc Gets file's full name (user's root is added to name, but only when asking about non-group dir).
%% @end
-spec get_full_file_name(FileName :: string(), Request :: atom(), UserDocStatus :: atom(), UserDoc :: tuple()) -> Result when
    Result :: {ok, FullFileName} | {error, ErrorDesc},
    FullFileName :: string(),
    ErrorDesc :: atom.
%% ====================================================================

get_full_file_name(FileName, Request, UserDocStatus, UserDoc) ->
    {ok, Tokens} = verify_file_name(FileName),
    VerifiedFileName = string:join(Tokens, "/"),
    case UserDocStatus of
        ok ->
            case fslogic_perms:assert_group_access(UserDoc, Request, VerifiedFileName) of
                ok ->
                    case Tokens of %% Map all /groups/* requests to root of the file system (i.e. dont add any prefix)
                        [?GROUPS_BASE_DIR_NAME | _] ->
                            {ok, VerifiedFileName};
                        _ ->
                            Root = get_user_root(UserDoc),
                            {ok, Root ++ "/" ++ VerifiedFileName}
                    end;
                _ -> {error, invalid_group_access}
            end;
        _ ->
            {error, {user_doc_not_found, UserDoc}}
    end.


%% verify_file_name/1
%% ====================================================================
%% @doc Strips '.' from path. Also if '..' path element if present, path is considered invalid.
%% @end
-spec verify_file_name(FileName :: string()) -> Result when
    Result :: {ok, Tokens :: list()} | {error, wrong_filename}.
%% ====================================================================
verify_file_name(FileName) ->
    Tokens = lists:filter(fun(X) -> X =/= "." end, string:tokens(FileName, "/")),
    case lists:any(fun(X) -> X =:= ".." end, Tokens) of
        true -> {error, wrong_filename};
        _ -> {ok, Tokens}
    end.

%% strip_path_leaf/1
%% ====================================================================
%% @doc Strips file name from path
-spec strip_path_leaf(Path :: string()) -> string().
%% ==================================================================
strip_path_leaf(Path) when is_list(Path) ->
    strip_path_leaf({split, lists:reverse(string:tokens(Path, [?PATH_SEPARATOR]))});
strip_path_leaf({split, []}) -> [?PATH_SEPARATOR];
strip_path_leaf({split, [_ | Rest]}) ->
    [?PATH_SEPARATOR] ++ string:join(lists:reverse(Rest), [?PATH_SEPARATOR]).


%% basename/1
%% ====================================================================
%% @doc Gives file basename from given path
-spec basename(Path :: string()) -> string().
%% ==================================================================
basename(Path) ->
    case lists:reverse(string:tokens(Path, [?PATH_SEPARATOR])) of
        [Leaf | _] -> Leaf;
        _ -> [?PATH_SEPARATOR]
    end.

%% get_parent_and_name_from_path/2
%% ====================================================================
%% @doc Gets parent uuid and file name on the basis of absolute path.
%% @end
-spec get_parent_and_name_from_path(Path :: string(), ProtocolVersion :: term()) -> Result when
    Result :: tuple().
%% ====================================================================

get_parent_and_name_from_path(Path, ProtocolVersion) ->
    File = fslogic_path:basename(Path),
    Parent = fslogic_path:strip_path_leaf(Path),
    case Parent of
        [?PATH_SEPARATOR] -> {ok, {File, #veil_document{}}};
        _Other ->
            {Status, TmpAns} = dao_lib:apply(dao_vfs, get_file, [Parent], ProtocolVersion),
            case Status of
                ok -> {ok, {File, TmpAns}};
                _BadStatus ->
                    ?error("Cannot find parent for path: ~s", [Path]),
                    {error, "Error: cannot find parent: " ++ TmpAns}
            end
    end.

%% get_user_root/1
%% ====================================================================
%% @doc Gets user's root directory.
%% @end
-spec get_user_root(UserDoc :: term()) -> Result when
    Result :: {ok, RootDir} | {error, ErrorDesc},
    RootDir :: string(),
    ErrorDesc :: atom.
%% ====================================================================

get_user_root(#veil_document{uuid = ?CLUSTER_USER_ID}) ->
    "";
get_user_root(#veil_document{record = UserRec}) ->
    get_user_root(UserRec);
get_user_root(#user{login = Login}) ->
    "/" ++ Login.


%% get_user_root/2
%% ====================================================================
%% @doc Gets user's root directory.
%% @end
-spec get_user_root(UserDocStatus :: atom(), UserDoc :: term()) -> Result when
    Result :: {ok, RootDir} | {error, ErrorDesc},
    RootDir :: string(),
    ErrorDesc :: atom.
%% ====================================================================

get_user_root(ok, UserDoc) ->
    {ok, get_user_root(UserDoc)};
get_user_root(error, Reason) ->
    {error, Reason}.

%% get_user_root/0
%% ====================================================================
%% @doc Gets user's root directory.
%% @end
-spec get_user_root() -> Result when
    Result :: {ok, RootDir} | {error, ErrorDesc},
    RootDir :: string(),
    ErrorDesc :: atom.
%% ====================================================================

get_user_root() ->
    {UserDocStatus, UserDoc} = fslogic_objects:get_user(),
    get_user_root(UserDocStatus, UserDoc).

%% ====================================================================
%% Internal functions
%% ====================================================================
