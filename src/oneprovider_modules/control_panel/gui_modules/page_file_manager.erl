%% ===================================================================
%% @author Lukasz Opiola
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This file contains n2o website code.
%% The page is a file manager providing basic functionalities.
%% @end
%% ===================================================================

-module(page_file_manager).
-include("oneprovider_modules/control_panel/common.hrl").
-include("oneprovider_modules/fslogic/fslogic.hrl").
-include("oneprovider_modules/fslogic/fslogic_acl.hrl").
-include("oneprovider_modules/dao/dao_users.hrl").
-include("fuse_messages_pb.hrl").
-include_lib("ctool/include/logging.hrl").

% n2o API
-export([main/0, event/1, api_event/3]).
% Postback functions and other
-export([get_requested_hostname/0, comet_loop/1]).
-export([clear_manager/0, clear_workspace/0, sort_toggle/1, sort_reverse/0, navigate/1, up_one_level/0]).
-export([toggle_view/1, select_item/1, select_all/0, deselect_all/0, clear_clipboard/0, put_to_clipboard/1, paste_from_clipboard/0]).
-export([confirm_paste/0, submit_perms/2, show_permissions_info/0]).
-export([populate_acl_list/1, change_perms_type/1, add_acl/0, delete_acl/1, edit_acl/1, move_acl/2, submit_acl/6]).
-export([rename_item/2, create_directory/1, remove_selected/0, search/1, toggle_column/2, show_popup/1, hide_popup/0, path_navigator_body/1]).
-export([fs_list_dir/1, fs_mkdir/1, fs_remove/1, fs_remove_dir/1, fs_mv/2, fs_mv/3, fs_copy/2, fs_create_share/1]).
-export([fs_has_perms/2, fs_chmod/3, fs_get_acl/1, fs_set_acl/3]).

% All file attributes that are supported
-define(ALL_ATTRIBUTES, [perms, size, atime, mtime]).

% Attributes displayed by default
-define(DEFAULT_ATTRIBUTES, [size, atime, mtime]).

% How often should comet process check for changes in current dir
-define(AUTOREFRESH_PERIOD, 1000).


% Item is either a file or a dir represented in manager
-record(item, {
    id = <<"">>,
    path = <<"/">>,
    basename = <<"">>,
    is_shared = false,
    attr = #fileattributes{}}).


%% Check if user is logged in and has dn defined.
main() ->
    case opn_gui_utils:maybe_redirect(true, true) of
        true ->
            #dtl{file = "bare", app = ?APP_Name, bindings = [
                {title, <<"">>},
                {body, <<"">>},
                {custom, <<"">>},
                {css, <<"">>}
            ]};
        false ->
            #dtl{file = "bare", app = ?APP_Name, bindings = [
                {title, title()},
                {body, body()},
                {custom, custom()},
                {css, css()}
            ]}
    end.


%% Page title
title() -> <<"File manager">>.

%% This will be placed in the template instead of {{custom}} tag
custom() ->
    <<"<script src=\"/js/oneprovider_upload.js\" type=\"text/javascript\" charset=\"utf-8\"></script>\n",
    "    <script src=\"/js/file_manager.js\" type=\"text/javascript\" charset=\"utf-8\"></script>",
    "    <script src=\"/flatui/bootbox.min.js\" type=\"text/javascript\" charset=\"utf-8\"></script>\n">>.

%% This will be placed in the template instead of {{css}} tag
css() ->
    <<"<link rel=\"stylesheet\" href=\"/css/file_manager.css\" type=\"text/css\" media=\"screen\" charset=\"utf-8\" />">>.


%% This will be placed in the template instead of {{body}} tag
body() ->
    gui_jq:register_escape_event("escape_pressed_event"),
    gui_jq:wire(#api{name = "confirm_paste_event", tag = "confirm_paste_event"}, false),
    gui_jq:wire(#api{name = "change_perms_type_event", tag = "change_perms_type_event"}, false),
    gui_jq:wire(#api{name = "submit_perms_event", tag = "submit_perms_event"}, false),
    gui_jq:wire(#api{name = "add_acl_event", tag = "add_acl_event"}, false),
    gui_jq:wire(#api{name = "delete_acl_event", tag = "delete_acl_event"}, false),
    gui_jq:wire(#api{name = "edit_acl_event", tag = "edit_acl_event"}, false),
    gui_jq:wire(#api{name = "move_acl_event", tag = "move_acl_event"}, false),
    gui_jq:wire(#api{name = "submit_acl_event", tag = "submit_acl_event"}, false),
    Body = [
        #panel{id = <<"spinner">>, style = <<"position: absolute; top: 12px; left: 17px; z-index: 1234; width: 32px;">>, body = [
            #image{image = <<"/images/spinner.gif">>}
        ]},
        opn_gui_utils:top_menu(data_tab, manager_submenu()),
        manager_workspace(),
        footer_popup()
    ],
    Body.

% Submenu that will be glued below the top menu
manager_submenu() ->
    [
        #panel{class = <<"navbar-inner">>, style = <<"padding-top: 10px;">>, body = [
            #panel{class = <<"container">>, style = <<"position: relative; overflow: hidden;">>, body = [
                #list{class = <<"nav">>, body =
                tool_button_and_dummy(<<"tb_up_one_level">>, <<"Up one level">>, <<"padding: 10px 7px 10px 15px; font-size: 28px; margin: -2px 0 2px -8px;">>,
                    <<"icomoon-arrow-left">>, {action, up_one_level})},
                #panel{class = <<"breadcrumb-text breadcrumb-background">>, style = <<"overflow: hidden; margin-left: 15px;">>, body = [
                    #p{id = <<"path_navigator">>, class = <<"breadcrumb-content">>, body = <<"~">>}
                ]},
                #panel{class = <<"control-group">>, style = <<"position: absolute; right: 15px; top: 0;">>, body = [
                    #panel{class = <<"input-append">>, style = <<"; margin-bottom: 0px;">>, body = [
                        #textbox{id = wire_enter(<<"search_textbox">>, <<"search_button">>), class = <<"span2">>,
                            style = <<"width: 220px;">>, placeholder = <<"Search">>},
                        #panel{class = <<"btn-group">>, body = [
                            #button{id = wire_click(<<"search_button">>, {action, search, [{query_value, <<"search_textbox">>}]}, <<"search_textbox">>),
                                class = <<"btn">>, body = #span{class = <<"fui-search">>}}
                        ]}
                    ]}
                ]}
            ]}
        ]},
        #panel{class = <<"navbar-inner">>, style = <<"border-bottom: 1px solid gray; padding-bottom: 5px;">>, body = [
            #panel{class = <<"container">>, body = [
                #list{class = <<"nav">>, style = <<"margin-right: 30px;">>, body =
                [#li{id = wire_click(<<"tb_create_dir">>, {action, show_popup, [create_directory]}), body = #link{title = <<"Create directory">>,
                    style = <<"padding: 16px 12px;">>, body = #span{class = <<"icomoon-folder-open">>, style = <<"font-size: 24px;">>,
                        body = #span{class = <<"icomoon-plus">>, style = <<"position: absolute; font-size: 10px; right: 5px; top: 16px;">>}}}}] ++
                    tool_button(<<"tb_upload_files">>, <<"Upload file(s)">>, <<"padding: 16px 12px;">>,
                        <<"icomoon-upload">>, {action, show_popup, [file_upload]}) ++
                    tool_button_and_dummy(<<"tb_share_file">>, <<"Share">>, <<"padding: 16px 12px;">>,
                        <<"icomoon-share">>, {action, show_popup, [share_file]})

                },
                #list{class = <<"nav">>, style = <<"margin-right: 30px;">>, body =
                tool_button_and_dummy(<<"tb_rename">>, <<"Rename">>, <<"padding: 16px 12px;">>,
                    <<"icomoon-pencil2">>, {action, show_popup, [rename_item]}) ++
                    tool_button_and_dummy(<<"tb_chmod">>, <<"Change permissions">>, <<"padding: 16px 12px;">>,
                        <<"icomoon-lock">>, {action, show_popup, [chmod]}) ++
                    tool_button_and_dummy(<<"tb_remove">>, <<"Remove">>, <<"padding: 16px 12px;">>,
                        <<"icomoon-remove">>, {action, show_popup, [remove_selected]})
                },
                #list{class = <<"nav">>, style = <<"margin-right: 30px;">>, body =
                tool_button_and_dummy(<<"tb_cut">>, <<"Cut">>, <<"padding: 16px 12px;">>,
                    <<"icomoon-scissors">>, {action, put_to_clipboard, [cut]}) ++
                %tool_button_and_dummy(<<"tb_copy">>, <<"Copy">>, <<"padding: 16px 12px;">>,
                %    <<"fui-windows">>, {action, put_to_clipboard, [copy]}) ++

                [#li{id = wire_click(<<"tb_paste">>, {action, paste_from_clipboard}), body = #link{title = <<"Paste">>, style = <<"padding: 16px 12px;">>,
                    body = #span{class = <<"icomoon-copy2">>, style = <<"font-size: 24px;">>, body = #span{id = <<"clipboard_size_label">>, class = <<"iconbar-unread">>,
                        style = <<"right: -2px; top: 9px; background-color: rgb(26, 188, 156);">>,
                        body = <<"0">>}}}},
                    #li{id = <<"tb_paste_dummy">>, class = <<"disabled hidden">>, body = #link{title = <<"Paste">>, style = <<"padding: 16px 12px;">>,
                        body = #span{style = <<"color: rgb(200, 200, 200); font-size: 24px;">>, class = <<"icomoon-copy2 ">>}}}]
                },
                #list{class = <<"nav">>, style = <<"margin-right: 30px;">>, body =
                tool_button_and_dummy(<<"tb_select_all">>, <<"Select all">>, <<"padding: 16px 12px;">>,
                    <<"icomoon-checkbox-checked">>, {action, select_all}) ++
                tool_button_and_dummy(<<"tb_deselect_all">>, <<"Deselect all">>, <<"padding: 16px 12px;">>,
                    <<"icomoon-checkbox-unchecked">>, {action, deselect_all})
                },

                #panel{class = <<"btn-toolbar pull-right no-margin">>, style = <<"padding: 12px 15px; overflow: hidden;">>, body = [
                    #panel{class = <<"btn-group no-margin">>, body = [
                        #link{id = wire_click(<<"list_view_button">>, {action, toggle_view, [list]}),
                            title = <<"List view">>, class = <<"btn btn-small btn-inverse">>,
                            body = #span{class = <<"fui-list-columned">>}},
                        #link{id = wire_click(<<"grid_view_button">>, {action, toggle_view, [grid]}),
                            title = <<"Grid view">>, class = <<"btn btn-small btn-inverse">>,
                            body = #span{class = <<"fui-list-small-thumbnails">>}}
                    ]}
                ]},

                #panel{class = <<"btn-group pull-right">>, style = <<"margin: 12px 15px">>, body = [
                    <<"<i class=\"dropdown-arrow dropdown-arrow-inverse\"></i>">>,
                    #button{id = wire_click(<<"button_sort_reverse">>, {action, sort_reverse}), title = <<"Reverse sorting">>,
                        class = <<"btn btn-inverse btn-small">>, body = <<"Sort">>},
                    #button{title = <<"Sort by">>, class = <<"btn btn-inverse btn-small dropdown-toggle">>,
                        data_fields = [{<<"data-toggle">>, <<"dropdown">>}], body = #span{class = <<"caret">>}},
                    #list{id = <<"sort_dropdown">>, class = <<"dropdown-menu dropdown-inverse">>, body = []}
                ]}
            ]}
        ]}
    ].


% Working space of the explorer.
manager_workspace() ->
    #panel{id = <<"manager_workspace">>, style = <<"z-index: -1; margin: 170px 0 20px 0; overflow: hidden">>, body = []}.


% Footer popup to display prompts and forms.
footer_popup() ->
    #panel{id = <<"footer_popup">>, class = <<"dialog success-dialog wide hidden">>,
        style = <<"z-index: 2; position:fixed; bottom: 0; margin-bottom: 0px; padding: 20px 0px; width: 100%;">>, body = []
    }.


% Emits a button, properly wired (postback)
tool_button(ID, Title, Style, Icon, Postback) ->
    [
        #li{id = wire_click(ID, Postback), body = #link{title = Title, style = Style,
            body = #span{class = Icon, style = <<"font-size: 24px;">>}}}
    ].


% Emits a button, properly wired (postback) + its disabled clone
tool_button_and_dummy(ID, Title, Style, Icon, Postback) ->
    tool_button(ID, Title, Style, Icon, Postback) ++
    [
        #li{id = <<ID/binary, "_dummy">>, class = <<"disabled hidden">>, body = #link{title = Title, style = Style,
            body = #span{style = <<"color: rgb(200, 200, 200); font-size: 24px;">>, class = Icon}}}
    ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Wiring postbacks. Thanks to this wrapper every time a postback is initiated,
%% there will be spinner showing up in 150ms. It gets hidden when reply is received.
wire_click(ID, Tag) ->
    gui_jq:wire(gui_jq:postback_action(ID, Tag)),
    gui_jq:bind_element_click(ID, <<"function(e) { $('#spinner').delay(150).show(); }">>),
    ID.

wire_click(ID, Tag, Source) ->
    gui_jq:wire(gui_jq:form_submit_action(ID, Tag, Source)),
    gui_jq:bind_element_click(ID, <<"function(e) { $('#spinner').delay(150).show(); }">>),
    ID.

wire_enter(ID, ButtonToClickID) ->
    % No need to show the spinner, as this only performs a click on a submit button
    gui_jq:bind_enter_to_submit_button(ID, ButtonToClickID),
    ID.


%% Handling events
api_event("escape_pressed_event", _, _) ->
    event({action, hide_popup});

api_event("confirm_paste_event", _, _) ->
    event({action, confirm_paste});

api_event("submit_perms_event", Args, _Ctx) ->
    [Perms, Recursive] = mochijson2:decode(Args),
    event({action, submit_perms, [Perms, Recursive]});

api_event("change_perms_type_event", Args, _Ctx) ->
    EnableACL = mochijson2:decode(Args),
    event({action, change_perms_type, [EnableACL]});

api_event("add_acl_event", _Args, _) ->
    event({action, add_acl});

api_event("delete_acl_event", Args, _) ->
    IndexRaw = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    event({action, delete_acl, [Index]});

api_event("edit_acl_event", Args, _) ->
    IndexRaw = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    event({action, edit_acl, [Index]});

api_event("move_acl_event", Args, _) ->
    [IndexRaw, MoveUp] = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    event({action, move_acl, [Index, MoveUp]});

api_event("submit_acl_event", Args, _) ->
    [IndexRaw, Identifier, Type, Read, Write, Execute] = mochijson2:decode(Args),
    Index = case IndexRaw of
                I when is_integer(I) -> I;
                Bin when is_binary(Bin) -> binary_to_integer(Bin)
            end,
    event({action, submit_acl, [Index, Identifier, Type, Read, Write, Execute]}).


event(init) ->
    case gui_ctx:user_logged_in() and opn_gui_utils:storage_defined() of
        false ->
            skip;
        true ->
            GRUID = opn_gui_utils:get_global_user_id(),
            AccessToken = opn_gui_utils:get_access_token(),
            Hostname = gui_ctx:get_requested_hostname(),
            {ok, Pid} = gui_comet:spawn(fun() -> comet_loop_init(GRUID, AccessToken, Hostname) end),
            put(comet_pid, Pid)
    end;


event(terminate) ->
    ok;


event({action, Fun}) ->
    event({action, Fun, []});


event({action, Fun, Args}) ->
    NewArgs = lists:map(
        fun(Arg) ->
            case Arg of
                {query_value, FieldName} ->
                    % This tuple means, that element with id=FieldName has to be queried
                    % and the result be put in function args
                    gui_ctx:postback_param(FieldName);
                Other ->
                    Other
            end
        end, Args),
    opn_gui_utils:apply_or_redirect(erlang, send, [get(comet_pid), {action, Fun, NewArgs}]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Comet loop and functions evaluated by comet
comet_loop_init(GRUID, UserAccessToken, RequestedHostname) ->
    % Initialize page state
    fslogic_context:set_gr_auth(GRUID, UserAccessToken),

    set_requested_hostname(RequestedHostname),
    set_working_directory(<<"/">>),
    set_selected_items([]),
    set_display_style(list),
    set_sort_by(name),
    set_sort_ascending(true),
    set_item_counter(1),
    set_item_list(fs_list_dir(get_working_directory())),
    set_item_list_rev(item_list_md5(get_item_list())),
    set_clipboard_items([]),
    set_clipboard_type(none),
    refresh_workspace(),
    gui_jq:hide(<<"spinner">>),
    gui_comet:flush(),

    % Enter comet loop for event processing and autorefreshing
    comet_loop(false).


comet_loop(IsUploadInProgress) ->
    NewIsUploadInProgress =
        try
            receive
                {action, Fun, Args} ->
                    case IsUploadInProgress of
                        true ->
                            gui_jq:wire(#alert{text = <<"Please wait for the upload to finish.">>}), gui_comet:flush();
                        false ->
                            erlang:apply(?MODULE, Fun, Args)
                    end,
                    gui_jq:hide(<<"spinner">>),
                    gui_comet:flush(),
                    IsUploadInProgress;
                upload_started ->
                    true;
                upload_finished ->
                    false;
                Other ->
                    ?debug("Unrecognized comet message in page_file_manager: ~p", [Other]),
                    IsUploadInProgress

            after ?AUTOREFRESH_PERIOD ->
                % Refresh file list if it has changed
                CurrentItemList = fs_list_dir(get_working_directory()),
                CurrentMD5 = item_list_md5(CurrentItemList),
                case get_item_list_rev() of
                    CurrentMD5 ->
                        skip;
                    _ ->
                        set_item_list(CurrentItemList),
                        set_item_list_rev(CurrentMD5),
                        refresh_workspace(),
                        gui_comet:flush()
                end,
                IsUploadInProgress
            end

        catch Type:Message ->
            ?error_stacktrace("Error in file_manager comet_loop - ~p:~p", [Type, Message]),
            page_error:redirect_with_error(?error_internal_server_error),
            gui_comet:flush(),
            error
        end,
    case NewIsUploadInProgress of
        error -> ok; % Comet process will terminate
        _ -> ?MODULE:comet_loop(NewIsUploadInProgress)
    end.


%%%%%%%%%%%%%%%
%% Event handling
clear_manager() ->
    hide_popup(),
    gui_jq:update(<<"path_navigator">>, path_navigator_body(get_working_directory())),
    clear_workspace().


clear_workspace() ->
    set_item_list(fs_list_dir(get_working_directory())),
    set_item_list_rev(item_list_md5(get_item_list())),
    refresh_workspace().


refresh_workspace() ->
    set_selected_items([]),
    refresh_tool_buttons(),
    sort_item_list(),
    NewBody = case get_display_style() of
                  list -> list_view_body();
                  grid -> grid_view_body()
              end,
    gui_jq:update(<<"manager_workspace">>, NewBody).


sort_item_list() ->
    AllItems = get_item_list(),
    {SpacesDirList, ItemList} = lists:partition(
        fun(I) ->
            is_space_dir(item_path(I))
        end, AllItems),
    Attr = get_sort_by(),
    SortAscending = get_sort_ascending(),
    SortedItems = lists:sort(
        fun(Item1, Item2) ->
            {V1, V2} = case item_attr(Attr, Item1) of
                           L when is_list(L) ->
                               {string:to_lower(item_attr(Attr, Item1)),
                                   string:to_lower(item_attr(Attr, Item2))};
                           _ ->
                               {item_attr(Attr, Item1), item_attr(Attr, Item2)}
                       end,
            if
                V1 < V2 -> true;
                V1 > V2 -> false;
                V1 =:= V2 -> item_attr(name, Item1) =< item_attr(name, Item2)
            end
        end, ItemList),
    Result = case SortAscending of
                 true -> SortedItems;
                 false -> lists:reverse(SortedItems)
             end,
    FinalResult = case Attr of
                      name ->
                          {Dirs, Files} = lists:partition(fun(I) -> item_is_dir(I) end, Result),
                          Dirs ++ Files;
                      _ -> Result
                  end,
    set_item_list(SpacesDirList ++ FinalResult).


sort_toggle(Type) ->
    case get_sort_by() of
        Type -> sort_reverse();
        _ ->
            set_sort_by(Type),
            set_sort_ascending(true),
            refresh_workspace()
    end.


sort_reverse() ->
    set_sort_ascending(not get_sort_ascending()),
    refresh_workspace().


refresh_tool_buttons() ->
    % View toggling buttons
    {EnableID, DisableID} = case get_display_style() of
                                list -> {<<"list_view_button">>, <<"grid_view_button">>};
                                grid -> {<<"grid_view_button">>, <<"list_view_button">>}
                            end,
    gui_jq:add_class(EnableID, <<"active">>),
    gui_jq:remove_class(DisableID, <<"active">>),

    % Sort dropdown
    DropdownBody = case get_display_style() of
                       grid ->
                           #li{id = wire_click(<<"grid_sort_by_name">>, {action, sort_toggle, [name]}),
                               class = <<"active">>, body = #link{body = <<"Name">>}};
                       list ->
                           lists:foldl(
                               fun(Attr, Acc) ->
                                   Class = case get_sort_by() of
                                               Attr -> <<"active">>;
                                               _ -> <<"">>
                                           end,
                                   Acc ++ [#li{id = wire_click(<<"list_sort_by_", (atom_to_binary(Attr, latin1))/binary>>, {action, sort_toggle, [Attr]}),
                                       class = Class, body = #link{body = attr_to_name(Attr)}}]
                               end, [], [name | get_displayed_file_attributes()])
                   end,
    gui_jq:update(<<"sort_dropdown">>, DropdownBody),

    Count = length(get_selected_items()),
    NFiles = length(get_item_list()),
    IsDir = try item_is_dir(item_find(element(1, lists:nth(1, get_selected_items())))) catch _:_ -> false end,
    enable_tool_button(<<"tb_up_one_level">>, get_working_directory() /= <<"/">>),
    enable_tool_button(<<"tb_share_file">>, (Count =:= 1) andalso (not IsDir)),
    enable_tool_button(<<"tb_rename">>, Count =:= 1),
    enable_tool_button(<<"tb_chmod">>, Count > 0),
    enable_tool_button(<<"tb_remove">>, Count > 0),
    enable_tool_button(<<"tb_cut">>, Count > 0),
    enable_tool_button(<<"tb_copy">>, false),
    enable_tool_button(<<"tb_paste">>, length(get_clipboard_items()) > 0),
    gui_jq:update(<<"clipboard_size_label">>, integer_to_binary(length(get_clipboard_items()))),
    enable_tool_button(<<"tb_select_all">>, Count < NFiles),
    enable_tool_button(<<"tb_deselect_all">>, Count > 0).


enable_tool_button(ID, Flag) ->
    case Flag of
        true ->
            gui_jq:remove_class(ID, <<"hidden">>),
            gui_jq:add_class(<<ID/binary, "_dummy">>, <<"hidden">>),
            gui_jq:show(ID),
            gui_jq:hide(<<ID/binary, "_dummy">>);
        false ->
            gui_jq:add_class(ID, <<"hidden">>),
            gui_jq:remove_class(<<ID/binary, "_dummy">>, <<"hidden">>),
            gui_jq:hide(ID),
            gui_jq:show(<<ID/binary, "_dummy">>)
    end.


navigate(Path) ->
    set_working_directory(Path),
    clear_manager().


up_one_level() ->
    navigate(filename:dirname(filename:absname(get_working_directory()))).


toggle_view(Type) ->
    set_display_style(Type),
    set_sort_by(name),
    set_sort_ascending(true),
    clear_workspace().


select_item(Path) ->
    case item_find(Path) of
        undefined ->
            skip;
        Item ->
            SelectedItems = get_selected_items(),
            Basename = item_basename(Item),
            case lists:member({Path, Basename}, SelectedItems) of
                false ->
                    set_selected_items(SelectedItems ++ [{Path, Basename}]),
                    gui_jq:add_class(item_id(Item), <<"selected-item">>);
                true ->
                    set_selected_items(SelectedItems -- [{Path, Basename}]),
                    gui_jq:remove_class(item_id(Item), <<"selected-item">>)
            end
    end,
    refresh_tool_buttons().


select_all() ->
    set_selected_items([]),
    lists:foreach(
        fun(Item) ->
            set_selected_items(get_selected_items() ++ [{item_path(Item), item_basename(Item)}]),
            gui_jq:add_class(item_id(Item), <<"selected-item">>)
        end, get_item_list()),
    refresh_tool_buttons().


deselect_all() ->
    lists:foreach(
        fun(Item) ->
            gui_jq:remove_class(item_id(Item), <<"selected-item">>)
        end, get_item_list()),
    set_selected_items([]),
    refresh_tool_buttons().


clear_clipboard() ->
    set_clipboard_items([]),
    set_clipboard_type(none).


put_to_clipboard(Type) ->
    SelectedItems = get_selected_items(),
    set_clipboard_type(Type),
    set_clipboard_items(SelectedItems),
    clear_workspace().


paste_from_clipboard() ->
    [{FirstPath, _} | _] = get_clipboard_items(),
    % Check if mv is between different spaces
    case is_the_same_space(filename:dirname(FirstPath), get_working_directory()) of
        false ->
            gui_jq:confirm_popup(<<"Do you really want to move your file(s) between spaces and ",
            "share them with all members of the target space?">>, <<"confirm_paste_event();">>);
        true ->
            confirm_paste()
    end.


confirm_paste() ->
    ClipboardItems = get_clipboard_items(),
    ClipboardType = get_clipboard_type(),
    WorkingDirectory = get_working_directory(),
    clear_clipboard(),
    ErrorMessage = lists:foldl(
        fun({Path, Basename}, Acc) ->
            case ClipboardType of
                cut ->
                    case fs_mv(Path, WorkingDirectory) of
                        ok ->
                            Acc;
                        {logical_file_system_error, "eperm"} ->
                            <<Acc/binary, "Unable to move ", Basename/binary, " - insufficient permissions.\r\n">>;
                        {logical_file_system_error, "eexist"} ->
                            <<Acc/binary, "Unable to move ", Basename/binary, " - file exists.\r\n">>;
                        {logical_file_system_error, "eacces"} ->
                            <<Acc/binary, "Unable to move ", Basename/binary, " - insufficient permissions.\r\n">>;
                        _ ->
                            <<Acc/binary, "Unable to move ", Basename/binary, " - error occured.\r\n">>
                    end;
                copy ->
                    % Not yet implemented
                    fs_copy(Path, WorkingDirectory),
                    Acc
            end
        end, <<"">>, ClipboardItems),
    case ErrorMessage of
        <<"">> ->
            ok;
        _ ->
            gui_jq:wire(#alert{text = ErrorMessage})
    end,
    clear_workspace().


submit_perms(Perms, Recursive) ->
    {Files, ACLEnabled, ACLEntries} = get_perms_state(),
    {Failed, Message} = case ACLEnabled of
                            true ->
                                FailedFiles = lists:foldl(
                                    fun(Path, Acc) ->
                                        {_Successful, Failed} = fs_set_acl(Path, ACLEntries, Recursive),
                                        Acc ++ Failed
                                    end, [], Files),
                                {FailedFiles, <<"Unable to set ACL for following file(s):">>};
                            false ->
                                FailedFiles = lists:foldl(
                                    fun(Path, Acc) ->
                                        {_Successful, Failed} = fs_chmod(Path, Perms, Recursive),
                                        Acc ++ Failed
                                    end, [], Files),
                                {FailedFiles, <<"Unable to change permissions for following file(s):">>}
                        end,
    case Failed of
        [] ->
            ok;
        _ ->
            FailedList = lists:foldl(
                fun({Path, Reason}, Acc) ->
                    ReasonBin = case Reason of
                                    {logical_file_system_error, "eacces"} ->
                                        <<"insufficient permissions">>;
                                    _ ->
                                        <<"error occured">>
                                end,
                    <<Acc/binary, Path/binary, ": ", ReasonBin/binary, "\r\n">>
                end, <<"">>, Failed),
            gui_jq:wire(#alert{text = <<Message/binary, "\r\n\r\n", FailedList/binary>>})
    end,
    clear_manager().


rename_item(OldPath, NewName) ->
    OldName = filename:basename(OldPath),
    case NewName of
        [] -> hide_popup();
        undefined -> hide_popup();
        OldName -> hide_popup();
        _ ->
            NewPath = filename:absname(NewName, get_working_directory()),
            case fs_mv(OldPath, get_working_directory(), NewName) of
                ok ->
                    clear_clipboard(),
                    clear_manager(),
                    select_item(NewPath);
                {logical_file_system_error, "eperm"} ->
                    gui_jq:wire(#alert{text = <<"Unable to rename ", (gui_str:to_binary(OldName))/binary, " - insufficient permissions.">>});
                {logical_file_system_error, "eexist"} ->
                    gui_jq:wire(#alert{text = <<"Unable to rename ", (gui_str:to_binary(OldName))/binary, " - file exists.">>});
                {logical_file_system_error, "eacces"} ->
                    gui_jq:wire(#alert{text = <<"Unable to rename ", (gui_str:to_binary(OldName))/binary, " - insufficient permissions.">>});
                _ ->
                    gui_jq:wire(#alert{text = <<"Unable to rename ", (gui_str:to_binary(OldName))/binary, " - error occured.">>})
            end
    end.


create_directory(Name) ->
    case Name of
        [] -> hide_popup();
        undefined -> hide_popup();
        _ ->
            FullPath = filename:absname(Name, get_working_directory()),
            case fs_mkdir(FullPath) of
                ok ->
                    clear_manager(),
                    select_item(FullPath);
                _ ->
                    case item_find(FullPath) of
                        undefined ->
                            gui_jq:wire(#alert{text = <<"Cannot create directory - disallowed name.">>});
                        _ ->
                            gui_jq:wire(#alert{text = <<"Cannot create directory - file exists.">>})
                    end,
                    hide_popup()
            end
    end.


remove_selected() ->
    SelectedItems = get_selected_items(),
    lists:foreach(
        fun({Path, _}) ->
            fs_remove(Path)
        end, SelectedItems),
    clear_clipboard(),
    clear_manager().


search(SearchString) ->
    case SearchString of
        <<"">> ->
            deselect_all();
        _ ->
            deselect_all(),
            lists:foreach(
                fun(Item) ->
                    case binary:match(item_basename(Item), SearchString) of
                        nomatch -> skip;
                        _ -> select_item(item_path(Item))
                    end
                end, get_item_list())
    end.


toggle_column(Attr, Flag) ->
    DisplayedAttrs = get_displayed_file_attributes(),
    case Flag of
        true ->
            set_displayed_file_attributes(DisplayedAttrs ++ [Attr]);
        false ->
            set_displayed_file_attributes(DisplayedAttrs -- [Attr])
    end,
    refresh_workspace().


show_permissions_info() ->
    gui_jq:info_popup(<<"POSIX permissions and ACLs">>, <<"Basic POSIX permissions and ACLs are two ways of controlling ",
    "the access to your data. You can choose to use one of them for each file. They cannot be used together. <br /><br />",
    "<strong>POSIX permissions</strong> - basic file permissions, can be used to enable certain types ",
    "of users to read, write or execute given file. The types are: user (the owner of the file), group (all users ",
    "sharing the space where the file resides), other (not aplicable in GUI, but used in oneclient).<br /><br />",
    "<strong>ACL</strong> (Access Control List) - CDMI standard (compliant with NFSv4 ACLs), allows ",
    "defining ordered lists of permissions-granting or permissions-denying entries for users or groups. ",
    "ACLs are processed from top to bottom - entries higher on list will have higher priority.">>, <<"">>).


populate_acl_list(SelectedIndex) ->
    {_Files, _EnableACL, ACLEntries} = get_perms_state(),
    JSON = rest_utils:encode_to_json(lists:map(
        fun(#accesscontrolentity{acetype = ACEType, aceflags = _ACEFlags, identifier = Identifier, acemask = ACEMask}) ->
            [
                {<<"identifier">>, Identifier},
                {<<"allow">>, ACEType =:= ?allow_mask},
                {<<"read">>, ACEMask band ?read_mask > 0},
                {<<"write">>, ACEMask band ?write_mask > 0},
                {<<"exec">>, ACEMask band ?execute_mask > 0}
            ]
        end, ACLEntries)),
    gui_jq:hide(<<"acl-form">>),
    gui_jq:wire(<<"clicked_index = -2;">>),
    gui_jq:wire(<<"populate_acl_list(", JSON/binary, ", ", (integer_to_binary(SelectedIndex))/binary, ");">>).


change_perms_type(EnableACL) ->
    {Files, _CurrentEnableACL, ACLEntries} = get_perms_state(),
    set_perms_state({Files, EnableACL, ACLEntries}),
    case EnableACL of
        true ->
            gui_jq:show(<<"tab_acl">>),
            gui_jq:hide(<<"tab_posix">>);
        _ ->
            gui_jq:hide(<<"tab_acl">>),
            gui_jq:show(<<"tab_posix">>)
    end.


add_acl() ->
    gui_jq:show(<<"acl-form">>),
    gui_jq:wire(<<"$('#acl_textbox').val('');">>),
    gui_jq:wire(<<"$('#acl_type_checkbox').checkbox('check');">>),
    gui_jq:wire(<<"$('#acl_read_checkbox').checkbox('uncheck');">>),
    gui_jq:wire(<<"$('#acl_write_checkbox').checkbox('uncheck');">>),
    gui_jq:wire(<<"$('#acl_exec_checkbox').checkbox('uncheck');">>).


delete_acl(Index) ->
    {Files, EnableACL, ACLEntries} = get_perms_state(),
    {Head, [_ | Tail]} = lists:split(Index, ACLEntries),
    set_perms_state({Files, EnableACL, Head ++ Tail}),
    populate_acl_list(-1).


edit_acl(Index) ->
    gui_jq:show(<<"acl-form">>),
    {_Files, _EnableACL, ACLEntries} = get_perms_state(),
    #accesscontrolentity{acetype = ACEType, aceflags = _ACEFlags,
        identifier = Identifier, acemask = ACEMask} = lists:nth(Index + 1, ACLEntries),
    gui_jq:set_value(<<"acl_textbox">>, Identifier),
    CheckJS = <<"').checkbox('check');">>,
    UncheckJS = <<"').checkbox('uncheck');">>,
    case ACEType of
        ?allow_mask -> gui_jq:wire(<<"$('#acl_type_checkbox", CheckJS/binary>>);
        _ -> gui_jq:wire(<<"$('#acl_type_checkbox", UncheckJS/binary>>)
    end,
    case ACEMask band ?read_mask of
        0 -> gui_jq:wire(<<"$('#acl_read_checkbox", UncheckJS/binary>>);
        _ -> gui_jq:wire(<<"$('#acl_read_checkbox", CheckJS/binary>>)
    end,
    case ACEMask band ?write_mask of
        0 -> gui_jq:wire(<<"$('#acl_write_checkbox", UncheckJS/binary>>);
        _ -> gui_jq:wire(<<"$('#acl_write_checkbox", CheckJS/binary>>)
    end,
    case ACEMask band ?execute_mask of
        0 -> gui_jq:wire(<<"$('#acl_exec_checkbox", UncheckJS/binary>>);
        _ -> gui_jq:wire(<<"$('#acl_exec_checkbox", CheckJS/binary>>)
    end.


submit_acl(Index, Name, Type, ReadFlag, WriteFlag, ExecFlag) ->
    {Files, EnableACL, ACLEntries} = get_perms_state(),
    ACEMask = (case ReadFlag of true -> ?read_mask; _ -> 0 end) bor
        (case WriteFlag of true -> ?write_mask; _ -> 0 end) bor
        (case ExecFlag of true -> ?execute_mask; _ -> 0 end),
    case ACEMask of
        0 ->
            gui_jq:info_popup(<<"Invalid values">>,
                <<"Acess List entry must allow or deny at least one permission.">>, <<"">>);
        _ ->
            NewEntity = #accesscontrolentity{
                acetype = (case Type of true -> ?allow_mask; _ -> ?deny_mask end),
                aceflags = ?no_flags_mask,
                identifier = fslogic_acl:name_to_gruid(Name),
                acemask = ACEMask},
            case Index of
                -1 ->
                    set_perms_state({Files, EnableACL, ACLEntries ++ [NewEntity]});
                _ ->
                    {Head, [_Ident | Tail]} = lists:split(Index, ACLEntries),
                    set_perms_state({Files, EnableACL, Head ++ [NewEntity] ++ Tail})
            end,
            populate_acl_list(-1)
    end.


move_acl(Index, MoveUp) ->
    {Files, EnableACL, ACLEntries} = get_perms_state(),
    MaxIndex = length(ACLEntries) - 1,
    {NewEntries, SelectedIndex} = case {Index, MoveUp} of
                                      {0, true} ->
                                          {ACLEntries, Index};
                                      {MaxIndex, false} ->
                                          {ACLEntries, Index};
                                      _ ->
                                          {Head, [Ident | Tail]} = lists:split(Index, ACLEntries),
                                          case MoveUp of
                                              true ->
                                                  % Head length is at least 1, because Index is not 0
                                                  {AllButLast, Last} = lists:split(length(Head) - 1, Head),
                                                  {AllButLast ++ [Ident] ++ Last ++ Tail, Index - 1};
                                              false ->
                                                  % Tail length is at least 1, because Index is not MaxIndex
                                                  [First | AllButFirst] = Tail,
                                                  {Head ++ [First] ++ [Ident] ++ AllButFirst, Index + 1}
                                          end
                                  end,
    set_perms_state({Files, EnableACL, NewEntries}),
    populate_acl_list(SelectedIndex).


% Shows popup with a prompt, form, etc.
show_popup(Type) ->
    {FooterBody, Script, CloseButtonAction} =
        case Type of
            create_directory ->
                Body = [
                    #p{body = <<"Create directory">>},
                    #form{class = <<"control-group">>, body = [
                        #textbox{id = wire_enter(<<"create_dir_textbox">>, <<"create_dir_submit">>), class = <<"flat">>,
                            style = <<"width: 350px;">>, placeholder = <<"New directory name">>},
                        #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>,
                            id = wire_click(<<"create_dir_submit">>,
                                {action, create_directory, [{query_value, <<"create_dir_textbox">>}]},
                                <<"create_dir_textbox">>)}
                    ]}
                ],
                {Body, <<"$('#create_dir_textbox').focus();">>, {action, hide_popup}};

            rename_item ->
                case fs_has_perms(get_working_directory(), write) of
                    false ->
                        gui_jq:wire(#alert{text = <<"You need write permissions in this directory to rename files.">>}),
                        {[], undefined, undefined};
                    true ->
                        case length(get_selected_items()) =:= 1 of
                            false ->
                                {[], undefined, undefined};
                            _ ->
                                [{OldLocation, Filename}] = get_selected_items(),
                                SelectionLength = byte_size(filename:rootname(Filename)),
                                Body = [
                                    #p{body = <<"Rename <b>", (gui_str:html_encode(Filename))/binary, "</b>">>},
                                    #form{class = <<"control-group">>, body = [
                                        #textbox{id = wire_enter(<<"new_name_textbox">>, <<"new_name_submit">>), class = <<"flat">>,
                                            style = <<"width: 350px;">>, placeholder = <<"New name">>, value = gui_str:html_encode(Filename)},

                                        #button{class = <<"btn btn-success btn-wide">>, body = <<"Ok">>,
                                            id = wire_click(<<"new_name_submit">>,
                                                {action, rename_item, [OldLocation, {query_value, <<"new_name_textbox">>}]},
                                                <<"new_name_textbox">>)}
                                    ]}
                                ],

                                FocusScript = <<"setTimeout(function() { ",
                                "document.getElementById('new_name_textbox').focus(); ",
                                "if( $('#new_name_textbox').createTextRange ) { ",
                                "var selRange = $('#new_name_textbox').createTextRange(); ",
                                "selRange.collapse(true); ",
                                "selRange.moveStart('character', 0); ",
                                "selRange.moveEnd('character', ", (integer_to_binary(SelectionLength))/binary, "); ",
                                "selRange.select(); ",
                                "} else if( document.getElementById('new_name_textbox').setSelectionRange ) { ",
                                "document.getElementById('new_name_textbox').setSelectionRange(0, ", (integer_to_binary(SelectionLength))/binary, "); ",
                                "} else if( $('#new_name_textbox').selectionStart ) { ",
                                "$('#new_name_textbox').selectionStart = 0; ",
                                "$('#new_name_textbox').selectionEnd = ", (integer_to_binary(SelectionLength))/binary, "; ",
                                "} }, 1); ">>,

                                {Body, FocusScript, {action, hide_popup}}
                        end
                end;

            chmod ->
                Files = lists:map(fun({ItmPath, _}) -> ItmPath end, get_selected_items()),
                [FirstPath | Items] = Files,

                GetTypeAndValue =
                    fun(ItemPath) ->
                        Item = item_find(ItemPath),
                        case item_attr(has_acl, Item) of
                            true ->
                                {acl, fs_get_acl(ItemPath)};
                            _ ->
                                {posix, item_attr(perms, Item)}
                        end
                    end,
                {FirstType, FirstValue} = GetTypeAndValue(FirstPath),

                % CommonType can be undefined|acl|posix
                % CommonValue can be undefined|list()|integer()
                {CommonType, CommonValue} = lists:foldl(
                    fun(ItemPath, {AccCommonType, AccCommonValue}) ->
                        {ItemType, ItemValue} = GetTypeAndValue(ItemPath),
                        case ItemType of
                            AccCommonType ->
                                case ItemValue of
                                    AccCommonValue -> {AccCommonType, AccCommonValue};
                                    _ -> {AccCommonType, undefined}
                                end;
                            _ ->
                                {undefined, undefined}
                        end
                    end, {FirstType, FirstValue}, Items),

                EnableACL = (CommonType =:= acl) or (CommonType =:= undefined),
                CommonPerms = case {CommonType, CommonValue} of
                                  {posix, Int} when is_integer(Int) -> Int;
                                  _ -> 0
                              end,
                CommonACL = case {CommonType, CommonValue} of
                                {acl, List} when is_list(List) -> List;
                                _ -> []
                            end,
                set_perms_state({Files, EnableACL, CommonACL}),

                PathToCheck = case FirstPath of
                                  <<"/", ?SPACES_BASE_DIR_NAME>> -> <<"/">>;
                                  _ -> FirstPath
                              end,
                {ok, FullFilePath} = fslogic_path:get_full_file_name(gui_str:binary_to_unicode_list(PathToCheck)),
                {ok, #space_info{users = Users}} = fslogic_utils:get_space_info_for_path(FullFilePath),
                Identifiers = gruids_to_identifiers(Users),

                gui_jq:wire(<<"init_chmod_table(", (integer_to_binary(CommonPerms))/binary, ");">>),
                {POSIXTabStyle, ACLTabStyle} = case EnableACL of
                                                   true -> {<<"display: none;">>, <<"">>};
                                                   false -> {<<"">>, <<"display: none;">>}
                                               end,
                Body = [
                    #panel{id = <<"perms_wrapper">>, body = [
                        #panel{id = <<"perms_header">>, body = [
                            #p{id = <<"perms_header_info">>, body = <<"Permissions type:">>},
                            #span{id = <<"perms_radios">>, body = [
                                #flatui_radio{id = <<"perms_radio_posix">>, name = <<"perms_radio">>,
                                    label_class = <<"radio perms-radio-label">>, body = <<"POSIX">>, checked = not EnableACL},
                                #flatui_radio{id = <<"perms_radio_acl">>, name = <<"perms_radio">>,
                                    label_class = <<"radio perms-radio-label">>, body = <<"ACL">>, checked = EnableACL}
                            ]},
                            #link{id = wire_click(<<"perms_info_button">>, {action, show_permissions_info}),
                                title = <<"Learn about permissions">>, class = <<"glyph-link">>,
                                body = #span{class = <<"icomoon-question">>}}
                        ]},
                        #panel{id = <<"tab_posix">>, style = POSIXTabStyle, body = [
                            #table{class = <<"table table-bordered">>, id = <<"posix_table">>, header = [
                                #tr{cells = [
                                    #th{body = <<"">>, class = <<"posix-cell">>},
                                    #th{body = <<"read">>, class = <<"posix-cell">>},
                                    #th{body = <<"write">>, class = <<"posix-cell">>},
                                    #th{body = <<"execute">>, class = <<"posix-cell">>}
                                ]}
                            ], body = #tbody{body = [
                                #tr{cells = [
                                    #td{body = <<"user">>, class = <<"posix-cell fw700">>},
                                    #td{class = <<"posix-cell">>, body = [
                                        #flatui_checkbox{id = <<"chbx_ur">>,
                                            label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                                    ]},
                                    #td{class = <<"posix-cell">>, body = [
                                        #flatui_checkbox{id = <<"chbx_uw">>,
                                            label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                                    ]},
                                    #td{class = <<"posix-cell">>, body = [
                                        #flatui_checkbox{id = <<"chbx_ux">>,
                                            label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                                    ]}
                                ]},
                                #tr{cells = [
                                    #td{body = <<"group">>, class = <<"posix-cell fw700">>},
                                    #td{class = <<"posix-cell">>, body = [
                                        #flatui_checkbox{id = <<"chbx_gr">>,
                                            label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                                    ]},
                                    #td{class = <<"posix-cell">>, body = [
                                        #flatui_checkbox{id = <<"chbx_gw">>,
                                            label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                                    ]},
                                    #td{class = <<"posix-cell">>, body = [
                                        #flatui_checkbox{id = <<"chbx_gx">>,
                                            label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                                    ]}
                                ]},
                                #tr{cells = [
                                    #td{body = <<"other">>, class = <<"posix-cell fw700">>},
                                    #td{class = <<"posix-cell">>, body = [
                                        #flatui_checkbox{id = <<"chbx_or">>,
                                            label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                                    ]},
                                    #td{class = <<"posix-cell">>, body = [
                                        #flatui_checkbox{id = <<"chbx_ow">>,
                                            label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                                    ]},
                                    #td{class = <<"posix-cell">>, body = [
                                        #flatui_checkbox{id = <<"chbx_ox">>,
                                            label_class = <<"checkbox no-label posix-checkbox">>, value = <<"">>}
                                    ]}
                                ]}
                            ]}
                            },
                            #panel{class = <<"posix-octal-form-wrapper">>, body = [
                                #p{class = <<"inline-block">>, body = <<"octal form:">>,
                                    title = <<"Type in octal representation of perms to automatically adjust checkboxes">>},
                                #textbox{id = <<"posix_octal_form_textbox">>, class = <<"span2">>,
                                    placeholder = <<"000">>, value = <<"">>}
                            ]}
                        ]},
                        #panel{id = <<"tab_acl">>, style = ACLTabStyle, body = [
                            #panel{class = <<"acl-info">>, body = [
                                #p{body = <<"proccessing">>},
                                #p{body = <<"order">>},
                                #span{class = <<"icomoon-arrow-down">>}
                            ]},
                            #panel{id = <<"acl_list">>},
                            #panel{id = <<"acl-form">>, body = [
                                #table{id = <<"acl-form-table">>, body = [
                                    #tr{cells = [
                                        #td{body = [
                                            #label{class = <<"label label-inverse acl-label">>, body = <<"Identifier">>}
                                        ]},
                                        #td{style = <<"padding-right: 20px;">>, body = [
                                            #select{id = <<"acl_select_name">>, class = <<"select-block">>, body = [
                                                lists:map(fun(Ident) -> #option{body = Ident} end, Identifiers)
                                            ]}
                                        ]}
                                    ]},
                                    #tr{cells = [
                                        #td{body = [
                                            #label{class = <<"label label-inverse acl-label">>, body = <<"Type">>}

                                        ]},
                                        #td{body = [
                                            #flatui_checkbox{label_class = <<"checkbox acl-checkbox">>, id = <<"acl_type_checkbox">>,
                                                checked = true, body = #span{id = <<"acl_type_checkbox_label">>, body = <<"allow">>}}
                                        ]}
                                    ]},
                                    #tr{cells = [
                                        #td{body = [
                                            #label{class = <<"label label-inverse acl-label">>, body = <<"Perms">>}

                                        ]},
                                        #td{body = [
                                            #flatui_checkbox{label_class = <<"checkbox acl-checkbox">>, id = <<"acl_read_checkbox">>, checked = true, body = <<"read">>},
                                            #flatui_checkbox{label_class = <<"checkbox acl-checkbox">>, id = <<"acl_write_checkbox">>, checked = true, body = <<"write">>},
                                            #flatui_checkbox{label_class = <<"checkbox acl-checkbox">>, id = <<"acl_exec_checkbox">>, checked = true, body = <<"execute">>}
                                        ]}
                                    ]},
                                    #tr{cells = [
                                        #td{body = [
                                            #button{id = <<"button_save_acl">>, class = <<"btn btn-success acl-form-button">>,
                                                body = <<"Save">>}
                                        ]},
                                        #td{body = [
                                            #button{id = <<"button_discard_acl">>, class = <<"btn btn-danger acl-form-button">>,
                                                body = <<"Discard">>, postback = {action, populate_acl_list, [-1]}}
                                        ]}
                                    ]}
                                ]}
                            ]},
                            #panel{class = <<"acl-info">>}
                        ]}
                    ]},
                    #panel{class = <<"clearfix">>},
                    #panel{id = <<"perms_warning_different">>, class = <<"perms-warning">>, body = [
                        #span{class = <<"icomoon-warning">>},
                        #p{body = <<"Selected files have different permissions. They will be overwritten by chosen permissions.">>}
                    ]},
                    #panel{id = <<"perms_warning_overwrite">>, class = <<"perms-warning">>, body = [
                        #span{class = <<"icomoon-warning">>},
                        #p{body = <<"Changing permissions recursively will overwrite <strong>ALL</strong> permissions in subdirectories.">>}
                    ]},
                    #form{class = <<"control-group">>, id = <<"perms_form">>, body = [
                        #flatui_checkbox{id = <<"chbx_recursive">>, label_class = <<"checkbox">>,
                            value = <<"">>, checked = false, body = <<"recursive">>,
                            label_id = <<"perms_recursive_label">>,
                            label_title = <<"Change perms in all subdirectories, recursively">>},
                        #button{id = <<"ok_button">>, class = <<"btn btn-success btn-wide">>, body = <<"Ok">>},
                        #button{class = <<"btn btn-danger btn-wide">>, body = <<"Cancel">>, postback = {action, hide_popup}}
                    ]}
                ],
                flatui_checkbox:init_checkbox(<<"acl_type_checkbox">>),
                flatui_checkbox:init_checkbox(<<"acl_read_checkbox">>),
                flatui_checkbox:init_checkbox(<<"acl_write_checkbox">>),
                flatui_checkbox:init_checkbox(<<"acl_exec_checkbox">>),
                flatui_radio:init_radio_button(<<"perms_radio_posix">>),
                gui_jq:wire(<<"$('#acl_select_name').selectpicker({style: 'btn-small', menuStyle: 'dropdown-inverse'});">>),
                gui_jq:wire(<<"$('#perms_radio_acl').change(function(e){change_perms_type_event($(this).is(':checked'));});">>),
                flatui_radio:init_radio_button(<<"perms_radio_acl">>),
                gui_jq:bind_element_click(<<"button_save_acl">>, <<"function() { submit_acl(); }">>),
                gui_jq:bind_element_click(<<"ok_button">>, <<"function() { submit_perms(); }">>),
                case CommonValue of
                    undefined -> gui_jq:show(<<"perms_warning_different">>);
                    _ -> ok
                end,
                populate_acl_list(-1),
                {Body, undefined, {action, hide_popup}};

            share_file ->
                case length(get_selected_items()) of
                    1 ->
                        [{Path, Filename}] = get_selected_items(),
                        {Status, ShareID} = case fs_get_share_uuid_by_filepath(Path) of
                                                undefined -> {new, fs_create_share(Path)};
                                                UUID -> {exists, UUID}
                                            end,
                        clear_workspace(),
                        select_item(Path),
                        AddressPrefix = <<"https://", (get_requested_hostname())/binary, ?shared_files_download_path>>,
                        Body = [
                            case Status of
                                exists ->
                                    #p{body = <<"<b>", (gui_str:html_encode(Filename))/binary,
                                    "</b> is already shared. Visit <b>Shared files</b> tab for more.">>};
                                new ->
                                    #p{body = <<"<b>", (gui_str:html_encode(Filename))/binary,
                                    "</b> successfully shared. Visit <b>Shared files</b> tab for more.">>}
                            end,
                            #form{class = <<"control-group">>, body = [
                                #textbox{id = wire_enter(<<"shared_link_textbox">>, <<"shared_link_submit">>), class = <<"flat">>, style = <<"width: 700px;">>,
                                    value = gui_str:html_encode(<<AddressPrefix/binary, ShareID/binary>>), placeholder = <<"Download link">>},
                                #button{id = wire_click(<<"shared_link_submit">>, {action, hide_popup}),
                                    class = <<"btn btn-success btn-wide">>, body = <<"Ok">>}
                            ]}
                        ],
                        {Body, <<"$('#shared_link_textbox').focus(); $('#shared_link_textbox').select();">>, {action, hide_popup}};
                    _ ->
                        {[], undefined, undefined}
                end;

            file_upload ->
                case fs_has_perms(get_working_directory(), write) of
                    true ->
                        Body = [
                            #oneprovider_upload{subscriber_pid = self(), target_dir = get_working_directory()}
                        ],
                        {Body, undefined, {action, clear_manager}};
                    false ->
                        gui_jq:wire(#alert{text = <<"You need write permissions in this directory to upload files.">>}),
                        {[], undefined, undefined}
                end;

            remove_selected ->
                {_FB, _S, _A} =
                    case fs_has_perms(get_working_directory(), write) of
                        false ->
                            gui_jq:wire(#alert{text = <<"You need write permissions in this directory to delete files.">>}),
                            {[], undefined, undefined};
                        true ->
                            case get_selected_items() of
                                [] ->
                                    {[], undefined, undefined};
                                Paths ->
                                    {NumFiles, NumDirs} = lists:foldl(
                                        fun({Path, _}, {NFiles, NDirs}) ->
                                            case item_is_dir(item_find(Path)) of
                                                true -> {NFiles, NDirs + 1};
                                                false -> {NFiles + 1, NDirs}
                                            end
                                        end, {0, 0}, Paths),
                                    FilesString = if
                                                      (NumFiles =:= 1) ->
                                                          <<"<b>", (integer_to_binary(NumFiles))/binary, " file</b>">>;
                                                      (NumFiles > 1) ->
                                                          <<"<b>", (integer_to_binary(NumFiles))/binary, " files</b>">>;
                                                      true -> <<"">>
                                                  end,
                                    DirsString = if
                                                     (NumDirs =:= 1) ->
                                                         <<"<b>", (integer_to_binary(NumDirs))/binary, " directory</b> and all its content">>;
                                                     (NumDirs > 1) ->
                                                         <<"<b>", (integer_to_binary(NumDirs))/binary, " directories</b> and all their content">>;
                                                     true -> <<"">>
                                                 end,
                                    Punctuation = if
                                                      (FilesString /= <<"">>) and (DirsString /= <<"">>) ->
                                                          <<", ">>;
                                                      true -> <<"">>
                                                  end,
                                    Body = [
                                        #p{body = <<"Remove ", FilesString/binary, Punctuation/binary, DirsString/binary, "?">>},
                                        #form{class = <<"control-group">>, body = [
                                            #button{id = wire_click(<<"ok_button">>, {action, remove_selected}),
                                                class = <<"btn btn-success btn-wide">>, body = <<"Ok">>},
                                            #button{id = wire_click(<<"cancel_button">>, {action, hide_popup}),
                                                class = <<"btn btn-danger btn-wide">>, body = <<"Cancel">>}
                                        ]}
                                    ],
                                    {Body, <<"$('#ok_button').focus();">>, {action, hide_popup}}
                            end
                    end;

            _ ->
                {[], undefined, undefined}
        end,
    case FooterBody of
        [] -> skip;
        _ ->
            CloseButton = #link{id = wire_click(<<"close_button">>, CloseButtonAction), title = <<"Hide">>, class = <<"glyph-link">>,
                style = <<"position: absolute; top: 8px; right: 8px; z-index: 3;">>,
                body = #span{class = <<"fui-cross">>, style = <<"font-size: 20px;">>}},
            gui_jq:update(<<"footer_popup">>, [CloseButton | FooterBody]),
            gui_jq:remove_class(<<"footer_popup">>, <<"hidden">>),
            gui_jq:slide_down(<<"footer_popup">>, 200)
    end,
    case Script of
        undefined ->
            ok;
        _ ->
            gui_jq:wire(Script, false)
    end.


% Hides the footer popup
hide_popup() ->
    gui_jq:update(<<"footer_popup">>, []),
    gui_jq:add_class(<<"footer_popup">>, <<"hidden">>),
    gui_jq:slide_up(<<"footer_popup">>, 200).


% Render path navigator
path_navigator_body(WorkingDirectory) ->
    case WorkingDirectory of
        <<"/">> -> gui_str:format_bin("~~", []);
        _ ->
            FirstLink = #link{id = wire_click(<<"nav_top">>, {action, navigate, [<<"/">>]}), body = <<"~">>},
            [<<"">> | PathElements] = binary:split(WorkingDirectory, <<"/">>, [global]),
            {LinkList, _} = lists:mapfoldl(
                fun(Element, {CurrentPath, Counter}) ->
                    PathToElement = <<CurrentPath/binary, "/", Element/binary>>,
                    Link = #link{id = wire_click(<<"nav_", (integer_to_binary(Counter))/binary>>, {action, navigate, [PathToElement]}),
                        body = gui_str:html_encode(Element)},
                    {Link, {PathToElement, Counter + 1}}
                end, {<<"">>, 1}, lists:sublist(PathElements, length(PathElements) - 1)),
            [FirstLink | LinkList] ++ [gui_str:html_encode(lists:last(PathElements))]
    end.


% Render grid view workspace
grid_view_body() ->
    {Tiles, _} = lists:mapfoldl(
        fun(Item, Counter) ->
            FullPath = item_path(Item),
            Basename = item_basename(Item),
            ImageStyle = case get_clipboard_type() of
                             cut ->
                                 case lists:member({FullPath, Basename}, get_clipboard_items()) of
                                     true -> <<"opacity:0.3; filter:alpha(opacity=30);">>;
                                     _ -> <<"">>
                                 end;
                             _ -> <<"">>
                         end,

            ImageUrl = case item_is_dir(Item) of
                           true ->
                               case is_space_dir(FullPath) of
                                   true -> <<"/images/folder_space64.png">>;
                                   false -> <<"/images/folder64.png">>
                               end;
                           false ->
                               <<"/images/file64.png">>
                       end,

            LinkID = <<"grid_item_", (integer_to_binary(Counter))/binary>>,
            % Item won't hightlight if the link is clicked.
            gui_jq:bind_element_click(LinkID, <<"function(e) { e.stopPropagation(); }">>),
            Tile = #panel{
                id = wire_click(item_id(Item), {action, select_item, [FullPath]}),
                style = <<"width: 100px; height: 116px; overflow:hidden; position: relative; margin: 0; padding: 5px 10px; display: inline-block;">>,
                body = case item_is_dir(Item) of
                           true ->
                               [
                                   #panel{style = <<"margin: 0 auto; text-align: center;">>, body = [
                                       #image{style = ImageStyle, image = ImageUrl}
                                   ]},
                                   #panel{style = <<"margin: 5px auto 0; text-align: center; word-wrap: break-word;">>, body = [
                                       #link{title = gui_str:html_encode(Basename), id = wire_click(LinkID, {action, navigate, [FullPath]}),
                                           body = gui_str:html_encode(Basename)}
                                   ]}
                               ];
                           false ->
                               ShareIcon = case item_is_shared(Item) of
                                               true ->
                                                   #span{style = <<"font-size: 36px; position: absolute; top: 0px; left: 0; z-index: 1;">>,
                                                       class = <<"icomoon-link">>};
                                               false -> []
                                           end,
                               [
                                   #panel{style = <<"margin: 0 auto; text-align: center;">>, body = [
                                       #panel{style = <<"display: inline-block; position: relative;">>, body = [
                                           ShareIcon,
                                           #image{style = ImageStyle, image = ImageUrl}
                                       ]}
                                   ]},
                                   #panel{style = <<"margin: 5px auto 0; text-align: center; word-wrap: break-word;">>, body = [
                                       #link{title = gui_str:html_encode(Basename), id = LinkID, body = gui_str:html_encode(Basename), target = <<"_blank">>,
                                           url = <<?user_content_download_path, "/", (gui_str:url_encode(FullPath))/binary>>}
                                   ]}
                               ]
                       end
            },
            {Tile, Counter + 1}
        end, 1, get_item_list()),
    Body = case Tiles of
               [] -> #p{style = <<"margin: 15px;">>, body = <<"This directory is empty">>};
               Other -> Other
           end,
    #panel{style = <<"margin-top: 15px;">>, body = Body}.


% Render list view workspace
list_view_body() ->
    NumAttr = erlang:max(1, length(get_displayed_file_attributes())),
    CellWidth = <<"width: 150px;">>,
    HiddenAttrs = ?ALL_ATTRIBUTES -- get_displayed_file_attributes(),
    HeaderTable = [
        #table{class = <<"no-margin table">>, style = <<"position: fixed; top: 173px; z-index: 10;",
        "background: white; border: 2px solid #bbbdc0; border-collapse: collapse;">>, header = [
            #tr{cells =
            [
                #th{style = <<"border: 2px solid #aaacae; color: rgb(64, 89, 116);">>, body = [
                    #panel{style = <<"position: relative;">>, body = [
                        <<"Name">>,
                        #panel{style = <<"position: absolute; right: -22px; top: 0; ">>, body =
                        lists:map(fun(Attr) ->
                            #span{style = <<"font-size: 12px; font-weight: normal; background-color: #EBEDEF; ",
                            "border: 1px solid #34495E; padding: 1px 3px; margin-right: 4px; cursor: pointer;">>,
                                id = wire_click(<<"toggle_column_", (gui_str:to_binary(Attr))/binary>>, {action, toggle_column, [Attr, true]}),
                                body = attr_to_name(Attr)}
                        end, HiddenAttrs)
                        }
                    ]}
                ]}
            ] ++
            lists:map(
                fun(Attr) ->
                    #th{style = <<"border: 2px solid #aaacae; color: rgb(64, 89, 116); position: relative;", CellWidth/binary>>,
                        body = [
                            #panel{style = <<"position: relative;">>, body = [
                                attr_to_name(Attr),
                                #link{id = wire_click(<<"toggle_column_", (gui_str:to_binary(Attr))/binary>>, {action, toggle_column, [Attr, false]}),
                                    class = <<"glyph-link">>, style = <<"font-size: 12px;", "position: absolute; top: -2px; right: -20px;">>,
                                    body = #span{class = <<"fui-cross">>}}
                            ]}
                        ]}
                end, get_displayed_file_attributes())
            }
        ]}
    ],
    DirUpRow = case get_working_directory() of
                   <<"/">> -> [];
                   Path ->
                       PrevDir = filename:dirname(filename:absname(Path)),
                       Item = item_new(PrevDir),
                       [
                           #tr{cells = [
                               #td{style = <<"vertical-align: middle;">>, body = #span{style = <<"word-wrap: break-word;">>,
                                   class = <<"table-cell">>, body = [
                                       #panel{style = <<"display: inline-block; vertical-align: middle;">>, body = [
                                           #link{id = wire_click(<<"prev_dir_link_image">>, {action, navigate, [PrevDir]}), body = [
                                               #image{class = <<"list-icon">>, image = <<"/images/folder32.png">>}
                                           ]}
                                       ]},
                                       #panel{style = <<"max-width: 230px; word-wrap: break-word; display: inline-block;vertical-align: middle;">>, body = [
                                           #link{id = wire_click(<<"prev_dir_link_text">>, {action, navigate, [PrevDir]}), body = <<"..&nbsp;&nbsp;&nbsp;">>}
                                       ]}
                                   ]}}] ++
                           lists:map(
                               fun(Attr) ->
                                   #td{style = CellWidth, class = <<"table-cell">>, body = item_attr_value(Attr, Item)}
                               end, get_displayed_file_attributes())
                           }
                       ]
               end,
    {TableRows, _} = lists:mapfoldl(
        fun(Item, Counter) ->
            FullPath = item_path(Item),
            Basename = item_basename(Item),
            ImageStyle = case get_clipboard_type() of
                             cut ->
                                 case lists:member({FullPath, Basename}, get_clipboard_items()) of
                                     true -> <<"opacity:0.3; filter:alpha(opacity=30);">>;
                                     _ -> <<"">>
                                 end;
                             _ -> <<"">>
                         end,

            ImageUrl = case item_is_dir(Item) of
                           true ->
                               case is_space_dir(FullPath) of
                                   true -> <<"/images/folder_space32.png">>;
                                   false -> <<"/images/folder32.png">>
                               end;
                           false -> <<"/images/file32.png">>
                       end,

            LinkID = <<"list_item_", (integer_to_binary(Counter))/binary>>,
            % Item won't hightlight if the link is clicked.
            gui_jq:bind_element_click(LinkID, <<"function(e) { e.stopPropagation(); }">>),
            ImageID = <<"image_", (integer_to_binary(Counter))/binary>>,
            % Image won't hightlight if the image is clicked.
            gui_jq:bind_element_click(ImageID, <<"function(e) { e.stopPropagation(); }">>),
            TableRow = #tr{
                id = wire_click(item_id(Item), {action, select_item, [FullPath]}),
                cells = [
                    case item_is_dir(Item) of
                        true ->
                            #td{style = <<"vertical-align: middle;">>, body = #span{style = <<"word-wrap: break-word;">>,
                                class = <<"table-cell">>, body = [
                                    #panel{style = <<"display: inline-block; vertical-align: middle;">>, body = [
                                        #link{id = wire_click(ImageID, {action, navigate, [FullPath]}), body =
                                        #image{class = <<"list-icon">>, style = ImageStyle, image = ImageUrl}}
                                    ]},
                                    #panel{class = <<"filename_row">>,
                                        style = <<"max-width: 400px; word-wrap: break-word; display: inline-block;vertical-align: middle;">>, body = [
                                            #link{id = wire_click(LinkID, {action, navigate, [FullPath]}), body = gui_str:html_encode(Basename)}
                                        ]}
                                ]}};
                        false ->
                            ShareIcon = case item_is_shared(Item) of
                                            true -> #span{class = <<"icomoon-link">>,
                                                style = <<"font-size: 18px; position: absolute; top: 0px; left: 0; z-index: 1; color: rgb(82, 100, 118);">>};
                                            false -> <<"">>
                                        end,
                            #td{body = #span{class = <<"table-cell">>, body = [
                                #panel{style = <<"display: inline-block; vertical-align: middle; position: relative;">>, body = [
                                    #link{id = ImageID, target = <<"_blank">>,
                                        url = <<?user_content_download_path, "/", (gui_str:url_encode(FullPath))/binary>>, body = [
                                            ShareIcon,
                                            #image{class = <<"list-icon">>, style = ImageStyle, image = ImageUrl}
                                        ]}
                                ]},
                                #panel{class = <<"filename_row">>, style = <<"word-wrap: break-word; display: inline-block;vertical-align: middle;">>, body = [
                                    #link{id = LinkID, body = gui_str:html_encode(Basename), target = <<"_blank">>,
                                        url = <<?user_content_download_path, "/", (gui_str:url_encode(FullPath))/binary>>}
                                ]}
                            ]}}
                    end
                ] ++
                lists:map(
                    fun(Attr) ->
                        #td{style = CellWidth, class = <<"table-cell">>, body = item_attr_value(Attr, Item)}
                    end, get_displayed_file_attributes())
            },
            {TableRow, Counter + 1}
        end, 1, get_item_list()),
    % Set filename containers width
    ContentWithoutFilename = 100 + (51 + round(90 * (2 + NumAttr) / NumAttr)) * NumAttr, % 51 is padding + border
    gui_jq:wire(<<"window.onresize = function(e) { $('.filename_row').css('max-width', ",
    "'' +($(window).width() - ", (integer_to_binary(ContentWithoutFilename))/binary, ") + 'px'); }; $(window).resize();">>),
    [
        HeaderTable,
        #table{id = <<"main_table">>, class = <<"table table-bordered">>,
            style = <<"border-radius: 0; margin-top: 49px; margin-bottom: 0; width: 100%; ">>, body = #tbody{body = DirUpRow ++ TableRows}}
    ].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Item manipulation functions
item_new(Dir, File) ->
    FullPath = filename:absname(File, Dir),
    item_new(FullPath).

item_new(FullPath) ->
    #fileattributes{type = Type, mode = Perms} = FA = fs_get_attributes(FullPath),
    % Set size to -1 if the file is a dir, and remove sticky bit from mode representation
    FileAttr = case Type of
                   "DIR" -> FA#fileattributes{size = -1, mode = Perms band 2#111111111};
                   _ -> FA#fileattributes{mode = Perms band 2#111111111}
               end,
    IsShared = case fs_get_share_uuid_by_filepath(FullPath) of
                   undefined -> false;
                   _ -> true
               end,
    #item{id = <<"item_", (get_item_counter())/binary>>,
        path = gui_str:unicode_list_to_binary(FullPath),
        basename = gui_str:unicode_list_to_binary(filename:basename(FullPath)),
        is_shared = IsShared,
        attr = FileAttr
    }.

item_find(Path) ->
    case lists:keyfind(Path, 3, get_item_list()) of
        false -> undefined;
        Item -> Item
    end.

item_is_dir(#item{attr = #fileattributes{type = Type}}) ->
    "DIR" =:= Type.

item_is_shared(#item{is_shared = IsShared}) ->
    IsShared.

item_id(#item{id = ID}) ->
    ID.

item_path(#item{path = Path}) ->
    Path.

item_basename(#item{basename = Basename}) ->
    Basename.

item_attr(name, Item) -> gui_str:binary_to_unicode_list(item_basename(Item));
item_attr(perms, #item{attr = #fileattributes{mode = Value}}) -> Value;
item_attr(uid, #item{attr = #fileattributes{uid = Value}}) -> Value;
item_attr(gid, #item{attr = #fileattributes{gid = Value}}) -> Value;
item_attr(atime, #item{attr = #fileattributes{atime = Value}}) -> Value;
item_attr(mtime, #item{attr = #fileattributes{mtime = Value}}) -> Value;
item_attr(ctime, #item{attr = #fileattributes{ctime = Value}}) -> Value;
item_attr(type, #item{attr = #fileattributes{type = Value}}) -> Value;
item_attr(size, #item{attr = #fileattributes{size = Value}}) -> Value;
item_attr(uname, #item{attr = #fileattributes{uname = Value}}) -> Value;
item_attr(gname, #item{attr = #fileattributes{gname = Value}}) -> Value;
item_attr(has_acl, #item{attr = #fileattributes{has_acl = Value}}) -> Value.

item_attr_value(name, Item) -> gui_str:to_binary(item_basename(Item));
item_attr_value(uname, Item) -> gui_str:to_binary(item_attr(uname, Item));
item_attr_value(atime, Item) -> gui_str:to_binary(time_to_string(item_attr(atime, Item)));
item_attr_value(mtime, Item) -> gui_str:to_binary(time_to_string(item_attr(mtime, Item)));
item_attr_value(ctime, Item) -> gui_str:to_binary(time_to_string(item_attr(ctime, Item)));
item_attr_value(size, Item) ->
    case item_is_dir(Item) of
        true -> <<"">>;
        false -> size_to_printable(item_attr(size, Item))
    end;
item_attr_value(perms, Item) ->
    case item_attr(has_acl, Item) of
        true ->
            #panel{style = <<"position: relative;">>, body = <<"ACL">>};
        false ->
            Perms = item_attr(perms, Item),
            Format = [<<"r">>, <<"w">>, <<"x">>, <<"r">>, <<"w">>, <<"x">>, <<"r">>, <<"w">>, <<"x">>],
            HasPerm = [
                Perms band 2#100000000 /= 0,
                Perms band 2#010000000 /= 0,
                Perms band 2#001000000 /= 0,
                Perms band 2#000100000 /= 0,
                Perms band 2#000010000 /= 0,
                Perms band 2#000001000 /= 0,
                Perms band 2#000000100 /= 0,
                Perms band 2#000000010 /= 0,
                Perms band 2#000000001 /= 0
            ],
            PermsTiles = lists:zipwith(
                fun(X, Y) ->
                    Char = case Y of
                               true -> X;
                               false -> <<"-">>
                           end,
                    #span{class = <<"perms-letter">>, body = Char}
                end, Format, HasPerm),
            PermsStr = case Perms of
                           0 -> <<"000">>;
                           _ -> gui_str:format_bin("~.8B", [Perms])
                       end,
            #panel{style = <<"position: relative;">>, body = [PermsTiles, <<"&nbsp;[", PermsStr/binary, "]">>]}
    end.


attr_to_name(name) -> <<"Name">>;
attr_to_name(size) -> <<"Size">>;
attr_to_name(perms) -> <<"Permissions">>;
attr_to_name(uname) -> <<"Owner">>;
attr_to_name(atime) -> <<"Access">>;
attr_to_name(mtime) -> <<"Modification">>;
attr_to_name(ctime) -> <<"State change">>.

time_to_string(Time) ->
    Timestamp = {Time div 1000000, Time rem 1000000, 0},
    {{YY, MM, DD}, {Hour, Min, Sec}} = calendar:now_to_local_time(Timestamp),
    io_lib:format("~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",
        [YY, MM, DD, Hour, Min, Sec]).

size_to_printable(Size) ->
    gui_str:to_binary(size_to_printable(Size, ["B", "KB", "MB", "GB", "TB"])).

size_to_printable(Size, [Current | Bigger]) ->
    case Size > 1024 of
        true -> size_to_printable(Size / 1024, Bigger);
        false ->
            case is_float(Size) of
                true -> lists:flatten(io_lib:format("~.2f ~s", [Size, Current]));
                false -> lists:flatten(io_lib:format("~B ~s", [Size, Current]))
            end
    end.

item_list_md5(ItemList) ->
    _Hash = lists:foldl(
        fun(#item{path = Path, is_shared = Shared, attr = Attrs}, Acc) ->
            TTB = term_to_binary({Path, Shared, Attrs}),
            erlang:md5(<<TTB/binary, Acc/binary>>)
        end, <<"">>, ItemList).


is_space_dir(<<"/", Path/binary>>) ->
    case Path of
        <<?SPACES_BASE_DIR_NAME>> ->
            true;
        <<?SPACES_BASE_DIR_NAME, Rest/binary>> ->
            case length(binary:split(Rest, <<"/">>, [global])) of
                2 -> true;
                _ -> false
            end;
        _ ->
            false
    end.


is_the_same_space(Path1, Path2) ->
    get_space_from_path(Path1) =:= get_space_from_path(Path2).


get_space_from_path(<<"/", Path/binary>>) ->
    case Path of
        <<?SPACES_BASE_DIR_NAME>> ->
            [#space_info{name = SpaceName} | _] = user_logic:get_spaces(fslogic_context:get_user_query()),
            gui_str:unicode_list_to_binary(SpaceName);
        <<?SPACES_BASE_DIR_NAME, Rest/binary>> ->
            Tokens = binary:split(Rest, <<"/">>, [global]),
            lists:nth(2, Tokens);
        _ ->
            [#space_info{name = SpaceName} | _] = user_logic:get_spaces(fslogic_context:get_user_query()),
            gui_str:unicode_list_to_binary(SpaceName)
    end.


gruids_to_identifiers(GRUIDs) ->
    NamesWithGRUIDs = lists:map(
        fun(GRUID) ->
            {ok, #db_document{record = #user{name = Name}}} = fslogic_objects:get_user({global_id, GRUID}),
            {Name, GRUID}
        end, GRUIDs),
    SortedNames = lists:keysort(1, NamesWithGRUIDs),
    {_, Identifiers} = lists:foldl(fun({Name, GRUID}, {Temp, Acc}) ->
        case Temp of
            [] ->
                {[{Name, GRUID}], Acc};
            [{FirstTemp, _} | _] ->
                case Name of
                    FirstTemp ->
                        {Temp ++ [{Name, GRUID}], Acc};
                    _ ->
                        case length(Temp) of
                            1 ->
                                {[{Name, GRUID}], Acc ++ [gui_str:unicode_list_to_binary(FirstTemp)]};
                            _ ->
                                {[{Name, GRUID}], Acc ++ lists:map(
                                    fun({Nam, GRU}) ->
                                        <<(gui_str:unicode_list_to_binary(Nam))/binary, " (#", GRU/binary, ")">>
                                    end, Temp)}
                        end
                end
        end
    end, {[], []}, SortedNames ++ [{<<"">>, <<"">>}]),
    Identifiers.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% logical_files_manager interfacing
fs_get_attributes(Path) ->
    {ok, FileAttr} = logical_files_manager:getfileattr(gui_str:binary_to_unicode_list(Path)),
    FileAttr.


fs_mkdir(Path) ->
    logical_files_manager:mkdir(gui_str:binary_to_unicode_list(Path)).


fs_remove(BinPath) ->
    Path = gui_str:binary_to_unicode_list(BinPath),
    Item = item_new(Path),
    case item_is_dir(Item) of
        true -> fs_remove_dir(BinPath);
        false -> logical_files_manager:delete(Path)
    end.


fs_remove_dir(BinDirPath) ->
    DirPath = gui_str:binary_to_unicode_list(BinDirPath),
    case is_space_dir(BinDirPath) of
        true ->
            skip;
        false ->
            ItemList = fs_list_dir(DirPath),
            lists:foreach(
                fun(Item) ->
                    fs_remove(item_path(Item))
                end, ItemList),
            logical_files_manager:rmdir(DirPath)
    end.


fs_list_dir(BinDir) ->
    case fs_list_dir(BinDir, 0, 10, []) of
        DirContent when is_list(DirContent) ->
            _ItemList = lists:foldl(
                fun(File, Acc) ->
                    try
                        Acc ++ [item_new(BinDir, File)]
                    catch _:_ ->
                        Acc
                    end
                end, [], DirContent);
        Other ->
            Other
    end.


fs_list_dir(BinDir, Offset, Count, Result) ->
    Path = gui_str:binary_to_unicode_list(BinDir),
    case logical_files_manager:ls(Path, Count, Offset) of
        {ok, FileList} ->
            FileList1 = lists:map(fun(#dir_entry{name = Name}) -> Name end, FileList),
            case length(FileList1) of
                Count -> fs_list_dir(Path, Offset + Count, Count * 10, Result ++ FileList1);
                _ -> Result ++ FileList1
            end;
        _ ->
            {error, not_a_dir}
    end.

fs_mv(BinPath, TargetDirBin) ->
    Path = gui_str:binary_to_unicode_list(BinPath),
    TargetDir = gui_str:binary_to_unicode_list(TargetDirBin),
    fs_mv(Path, TargetDir, filename:basename(Path)).

fs_mv(BinPath, TargetDirBin, TargetNameBin) ->
    Path = gui_str:binary_to_unicode_list(BinPath),
    TargetDir = gui_str:binary_to_unicode_list(TargetDirBin),
    TargetName = gui_str:binary_to_unicode_list(TargetNameBin),
    TargetPath = filename:absname(TargetName, TargetDir),
    case Path of
        TargetPath ->
            ok;
        _ ->
            logical_files_manager:mv(Path, TargetPath)
    end.


fs_copy(_Path, _TargetPath) ->
    throw(not_yet_implemented).

fs_create_share(Filepath) ->
    {ok, ID} = logical_files_manager:create_standard_share(gui_str:binary_to_unicode_list(Filepath)),
    gui_str:to_binary(ID).

fs_get_share_uuid_by_filepath(Filepath) ->
    case logical_files_manager:get_share({file, gui_str:binary_to_unicode_list(Filepath)}) of
        {ok, #db_document{uuid = UUID}} ->
            gui_str:to_binary(UUID);
        _ ->
            undefined
    end.

% Returns a tuple {Successful, Failed}, where Succesfull is a list of
% paths for which command succeded and Failed is a list of tuples {Path, Reason}
% for paths that the command failed.
fs_chmod(Path, Perms, Recursive) ->
    fs_chmod(Path, Perms, Recursive, {[], []}).

fs_chmod(Path, Perms, Recursive, {Successful, Failed}) ->
    IsDir = item_is_dir(item_new(Path)),
    {NewSuccessful, NewFailed} =
        case Recursive of
            false ->
                case logical_files_manager:change_file_perm(gui_str:binary_to_unicode_list(Path), Perms, not IsDir) of
                    ok -> {[Path], []};
                    Err1 -> {[], [{Path, Err1}]}
                end;
            true ->
                case logical_files_manager:change_file_perm(gui_str:binary_to_unicode_list(Path), Perms, not IsDir) of
                    ok ->
                        case IsDir of
                            false ->
                                {[Path], []};
                            true ->
                                lists:foldl(
                                    fun(#item{path = ItemPath}, {SuccAcc, FailAcc}) ->
                                        {Succ, Fail} = fs_chmod(ItemPath, Perms, Recursive),
                                        {SuccAcc ++ Succ, FailAcc ++ Fail}
                                    end, {[Path], []}, fs_list_dir(Path))
                        end;
                    Err3 ->
                        {[], [{Path, Err3}]}
                end
        end,
    {Successful ++ NewSuccessful, Failed ++ NewFailed}.


fs_has_perms(Path, CheckType) ->
    logical_files_manager:check_file_perm(gui_str:binary_to_unicode_list(Path), CheckType).

fs_get_acl(Path) ->
    case logical_files_manager:get_acl(gui_str:binary_to_unicode_list(Path)) of
        {ok, List} -> List;
        _ -> []
    end.

% Returns a tuple {Successful, Failed}, where Succesfull is a list of
% paths for which command succeded and Failed is a list of tuples {Path, Reason}
% for paths that the command failed.
fs_set_acl(Path, ACLEntries, Recursive) ->
    fs_set_acl(Path, ACLEntries, Recursive, {[], []}).

fs_set_acl(Path, ACLEntries, Recursive, {Successful, Failed}) ->
    IsDir = item_is_dir(item_new(Path)),
    {NewSuccessful, NewFailed} =
        case Recursive of
            false ->
                case logical_files_manager:set_acl(gui_str:binary_to_unicode_list(Path), ACLEntries) of
                    ok -> {[Path], []};
                    Err1 -> {[], [{Path, Err1}]}
                end;
            true ->
                case logical_files_manager:set_acl(gui_str:binary_to_unicode_list(Path), ACLEntries) of
                    ok ->
                        case IsDir of
                            false ->
                                {[Path], []};
                            true ->
                                lists:foldl(
                                    fun(#item{path = ItemPath}, {SuccAcc, FailAcc}) ->
                                        {Succ, Fail} = fs_set_acl(ItemPath, ACLEntries, Recursive),
                                        {SuccAcc ++ Succ, FailAcc ++ Fail}
                                    end, {[Path], []}, fs_list_dir(Path))
                        end;
                    Err3 ->
                        {[], [{Path, Err3}]}
                end
        end,
    {Successful ++ NewSuccessful, Failed ++ NewFailed}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Functions to save and retrieve page state
set_requested_hostname(Host) -> put(rh, Host).
get_requested_hostname() -> get(rh).

set_working_directory(Dir) -> put(wd, Dir).
get_working_directory() -> get(wd).

% Holds a list o tuples {FilePath, FileName}
set_selected_items(List) -> put(sel_items, List).
get_selected_items() -> get(sel_items).

set_display_style(Style) -> put(display_style, Style).
get_display_style() -> get(display_style).

% These preferences are saved in session memory for user convenience
set_displayed_file_attributes(Attrs) ->
    SortedAttrs = lists:filter(
        fun(Attr) ->
            lists:member(Attr, Attrs)
        end, ?ALL_ATTRIBUTES),
    gui_ctx:put(dfa, SortedAttrs).
get_displayed_file_attributes() ->
    case gui_ctx:get(dfa) of
        undefined ->
            ?DEFAULT_ATTRIBUTES;
        Attrs ->
            Attrs
    end.

set_sort_by(Type) -> put(sort_by, Type).
get_sort_by() -> get(sort_by).

set_sort_ascending(Flag) -> put(sort_ascending, Flag).
get_sort_ascending() -> get(sort_ascending).

set_item_list(List) -> put(item_list, List).
get_item_list() -> get(item_list).

set_item_list_rev(MD5) -> put(item_list_rev, MD5).
get_item_list_rev() -> get(item_list_rev).

% Holds a list o tuples {FilePath, FileName}
set_clipboard_items(List) -> put(clipboard_items, List).
get_clipboard_items() -> get(clipboard_items).

set_clipboard_type(Type) -> put(clipboard_type, Type).
get_clipboard_type() -> get(clipboard_type).

set_item_counter(Counter) -> put(item_counter, Counter).
get_item_counter() ->
    Val = get(item_counter),
    put(item_counter, Val + 1),
    integer_to_binary(Val).  % Return binary as this is used for making element IDs

% Holds information what files' ACLs are being edited and what is the current state
set_perms_state(State) -> put(acl_state, State).
get_perms_state() -> get(acl_state).
