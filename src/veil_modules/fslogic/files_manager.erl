%% ===================================================================
%% @author Michal Wrzeszcz
%% @copyright (C): 2013 ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module provides high level file system operations.
%% @end
%% ===================================================================

%% TODO zinegrować ze zmienionym fslogic (znającym usera)

-module(files_manager).

-include("registered_names.hrl").
-include("communication_protocol_pb.hrl").
-include("fuse_messages_pb.hrl").
-include("veil_modules/fslogic/fslogic.hrl").
-include("veil_modules/dao/dao_vfs.hrl").

-define(NewFileLogicMode, 8#744).
-define(NewFileStorageMode, 8#744).

%% ====================================================================
%% API
%% ====================================================================
%% Logical file organization management (only db is used)
-export([mkdir/1, rmdir/1, mv/2, chown/0, change_file_perm/2, ls/3]).
%% File access (db and helper are used)
-export([read/3, write/3, write/2, create/1, delete/1]).
%% Physical files organization management (to better organize files on storage;
%% the user does not see results of these operations)
-export([mkdir_storage_system/0, mv_storage_system/0, delete_dir_storage_system/0]).
%% Physical files access (used to create temporary copies for remote files)
-export([read_storage_system/4, write_storage_system/4, write_storage_system/3, create_file_storage_system/2, delete_file_storage_system/2, ls_storage_system/0]).

%% ====================================================================
%% Logical file organization management (only db is used)
%% ====================================================================

%% mkdir/1
%% ====================================================================
%% @doc Creates directory (in db)
%% @end
-spec mkdir(DirName :: string()) -> Result when
  Result :: ok | {ErrorGeneral, ErrorDetail},
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
mkdir(DirName) ->
  Record = #createdir{dir_logic_name = DirName, mode = ?NewFileLogicMode},
  {Status, TmpAns} = contact_fslogic(Record),
  case Status of
    ok ->
      Response = TmpAns#atom.value,
      case Response of
        ?VOK -> ok;
        _ -> {logical_file_system_error, Response}
      end;
    _ -> {Status, TmpAns}
  end.

%% rmdir/1
%% ====================================================================
%% @doc Deletes directory (in db)
%% @end
-spec rmdir(DirName :: string()) -> Result when
  Result :: ok | {ErrorGeneral, ErrorDetail},
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
rmdir(DirName) ->
  Record = #deletefile{file_logic_name = DirName},
  {Status, TmpAns} = contact_fslogic(Record),
  case Status of
    ok ->
      Response = TmpAns#atom.value,
      case Response of
        ?VOK -> ok;
        _ -> {logical_file_system_error, Response}
      end;
    _ -> {Status, TmpAns}
  end.

%% mv/2
%% ====================================================================
%% @doc Moves directory (in db)
%% @end
-spec mv(From :: string(), To :: string()) -> Result when
  Result :: ok | {ErrorGeneral, ErrorDetail},
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
mv(From, To) ->
  Record = #renamefile{from_file_logic_name = From, to_file_logic_name  = To},
  {Status, TmpAns} = contact_fslogic(Record),
  case Status of
    ok ->
      Response = TmpAns#atom.value,
      case Response of
        ?VOK -> ok;
        _ -> {logical_file_system_error, Response}
      end;
    _ -> {Status, TmpAns}
  end.

%% chown/0
%% ====================================================================
%% @doc Changes owner of file (in db)
%% @end
-spec chown() -> {error, not_implemented_yet}.
%% ====================================================================
chown() ->
  {error, not_implemented_yet}.

%% change_file_perm/2
%% ====================================================================
%% @doc Changes file's permissions in db
%% @end
-spec change_file_perm(FileName :: string(), NewPerms :: integer()) -> Result when
  Result :: ok | {ErrorGeneral, ErrorDetail},
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
change_file_perm(FileName, NewPerms) ->
  Record = #changefileperms{logic_file_name = FileName, perms = NewPerms},
  {Status, TmpAns} = contact_fslogic(Record),
  case Status of
    ok ->
      Response = TmpAns#atom.value,
      case Response of
        ?VOK -> ok;
        _ -> {logical_file_system_error, Response}
      end;
    _ -> {Status, TmpAns}
  end.

%% ls/3
%% ====================================================================
%% @doc Lists directory (uses data from db)
%% @end
-spec ls(DirName :: string(), ChildrenNum :: integer(), Offset :: integer()) -> Result when
  Result :: {ok, FilesList} | {ErrorGeneral, ErrorDetail},
  FilesList :: list(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
ls(DirName, ChildrenNum, Offset) ->
  Record = #getfilechildren{dir_logic_name = DirName, children_num = ChildrenNum, offset = Offset},
  {Status, TmpAns} = contact_fslogic(Record),
  case Status of
    ok ->
      Response = TmpAns#filechildren.answer,
      case Response of
        ?VOK -> {ok, TmpAns#filechildren.child_logic_name};
        _ -> {logical_file_system_error, Response}
      end;
    _ -> {Status, TmpAns}
  end.

%% ====================================================================
%% File access (db and helper are used)
%% ====================================================================

%% read/3
%% ====================================================================
%% @doc Reads file (uses logical name of file). First it gets information
%% about storage helper and file id at helper. Next it uses storage helper
%% to read data from file.
%% @end
-spec read(File :: string(), Offset :: integer(), Size :: integer()) -> Result when
  Result :: {ok, Bytes} | {ErrorGeneral, ErrorDetail},
  Bytes :: binary(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
read(File, Offset, Size) ->
  Record = #getfilelocation{file_logic_name = File},
  {Status, TmpAns} = contact_fslogic(Record),
  case Status of
    ok ->
      Response = TmpAns#filelocation.answer,
      case Response of
        ?VOK ->
          Storage_helper_info = #storage_helper_info{name = TmpAns#filelocation.storage_helper_name, init_args = TmpAns#filelocation.storage_helper_args},
          read_storage_system(Storage_helper_info, TmpAns#filelocation.file_id, Offset, Size);
        _ -> {logical_file_system_error, Response}
      end;
    _ -> {Status, TmpAns}
  end.

%% write/2
%% ====================================================================
%% @doc Appends data to the end of file (uses logical name of file).
%% First it gets information about storage helper and file id at helper.
%% Next it uses storage helper to write data to file.
%% @end
-spec write(File :: string(), Buf :: binary()) -> Result when
  Result :: BytesWritten | {ErrorGeneral, ErrorDetail},
  BytesWritten :: integer(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
write(File, Buf) ->
  Record = #getfilelocation{file_logic_name = File},
  {Status, TmpAns} = contact_fslogic(Record),
  case Status of
    ok ->
      Response = TmpAns#filelocation.answer,
      case Response of
        ?VOK ->
          Storage_helper_info = #storage_helper_info{name = TmpAns#filelocation.storage_helper_name, init_args = TmpAns#filelocation.storage_helper_args},
          write_storage_system(Storage_helper_info, TmpAns#filelocation.file_id, Buf);
        _ -> {logical_file_system_error, Response}
      end;
    _ -> {Status, TmpAns}
  end.

%% write/3
%% ====================================================================
%% @doc Writes data to file (uses logical name of file). First it gets
%% information about storage helper and file id at helper. Next it uses
%% storage helper to write data to file.
%% @end
-spec write(File :: string(), Offset :: integer(), Buf :: binary()) -> Result when
  Result :: BytesWritten | {ErrorGeneral, ErrorDetail},
  BytesWritten :: integer(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
write(File, Offset, Buf) ->
  Record = #getfilelocation{file_logic_name = File},
  {Status, TmpAns} = contact_fslogic(Record),
  case Status of
    ok ->
      Response = TmpAns#filelocation.answer,
      case Response of
        ?VOK ->
          Storage_helper_info = #storage_helper_info{name = TmpAns#filelocation.storage_helper_name, init_args = TmpAns#filelocation.storage_helper_args},
          write_storage_system(Storage_helper_info, TmpAns#filelocation.file_id, Offset, Buf);
        _ -> {logical_file_system_error, Response}
      end;
    _ -> {Status, TmpAns}
  end.

%% create/1
%% ====================================================================
%% @doc Creates file (uses logical name of file). First it creates file
%% in db and gets information about storage helper and file id at helper.
%% Next it uses storage helper to create file on storage.
%% @end
-spec create(File :: string()) -> Result when
  Result :: ok | {ErrorGeneral, ErrorDetail},
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
create(File) ->
  Record = #getnewfilelocation{file_logic_name = File, mode = ?NewFileLogicMode},
  {Status, TmpAns} = contact_fslogic(Record),
  case Status of
    ok ->
      Response = TmpAns#filelocation.answer,
      case Response of
        ?VOK ->
          Storage_helper_info = #storage_helper_info{name = TmpAns#filelocation.storage_helper_name, init_args = TmpAns#filelocation.storage_helper_args},
          create_file_storage_system(Storage_helper_info, TmpAns#filelocation.file_id);
        _ -> {logical_file_system_error, Response}
      end;
    _ -> {Status, TmpAns}
  end.

%% delete/1
%% ====================================================================
%% @doc Deletes file (uses logical name of file). First it gets
%% information about storage helper and file id at helper. Next it uses
%% storage helper to delete file from storage. Afterwards it deletes
%% information about file from db.
%% @end
-spec delete(File :: string()) -> Result when
  Result :: ok | {ErrorGeneral, ErrorDetail},
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
delete(File) ->
  Record = #getfilelocation{file_logic_name = File},
  {Status, TmpAns} = contact_fslogic(Record),
  case Status of
    ok ->
      Response = TmpAns#filelocation.answer,
      case Response of
        ?VOK ->
          Storage_helper_info = #storage_helper_info{name = TmpAns#filelocation.storage_helper_name, init_args = TmpAns#filelocation.storage_helper_args},
          TmpAns2 = delete_file_storage_system(Storage_helper_info, TmpAns#filelocation.file_id),

          case TmpAns2 of
            ok ->
              Record2 = #deletefile{file_logic_name = File},
              {Status3, TmpAns3} = contact_fslogic(Record2),
              case Status3 of
                ok ->
                  Response2 = TmpAns3#atom.value,
                  case Response2 of
                    ?VOK -> ok;
                    _ -> {logical_file_system_error, Response2}
                  end;
                _ -> {Status3, TmpAns3}
              end;
            _ -> TmpAns2
          end;
        _ -> {logical_file_system_error, Response}
      end;
    _ -> {Status, TmpAns}
  end.

%% ====================================================================
%% Physical files organization management (to better organize files on storage;
%% the user does not see results of these operations)
%% ====================================================================

%% mkdir_storage_system/0
%% ====================================================================
%% @doc Creates dir on storage
%% @end
-spec mkdir_storage_system() -> {error, not_implemented_yet}.
%% ====================================================================
mkdir_storage_system() ->
  {error, not_implemented_yet}.

%% mv_storage_system/0
%% ====================================================================
%% @doc Moves file on storage
%% @end
-spec mv_storage_system() -> {error, not_implemented_yet}.
%% ====================================================================
mv_storage_system() ->
  {error, not_implemented_yet}.

%% delete_dir_storage_system/0
%% ====================================================================
%% @doc Deletes dir on storage
%% @end
-spec delete_dir_storage_system() -> {error, not_implemented_yet}.
%% ====================================================================
delete_dir_storage_system() ->
  {error, not_implemented_yet}.

%% ====================================================================
%% Physical files access (used to create temporary copies for remote files)
%% ====================================================================

%% read_storage_system/4
%% ====================================================================
%% @doc Reads file (operates only on storage). First it checks file
%% attributes (file type and file size). If everything is ok,
%% it reads data from file.
%% @end
-spec read_storage_system(Storage_helper_info :: record(), File :: string(), Offset :: integer(), Size :: integer()) -> Result when
  Result :: {ok, Bytes} | {ErrorGeneral, ErrorDetail},
  Bytes :: binary(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
read_storage_system(Storage_helper_info, File, Offset, Size) ->
  {ErrorCode, Stat} = veilhelpers:exec(getattr, Storage_helper_info, [File]),
  case ErrorCode of
    0 ->
      case veilhelpers:exec(is_reg, [Stat#st_stat.st_mode]) of
        true ->
          FSize = Stat#st_stat.st_size,
          case FSize < Offset of
            false ->
              Flag = veilhelpers:exec(get_flag, [o_rdonly]),
              {ErrorCode2, FFI} = veilhelpers:exec(open, Storage_helper_info, [File, #st_fuse_file_info{flags = Flag}]),
              case ErrorCode2 of
                0 ->
                  Size2 = case Offset + Size > FSize of
                            true -> FSize - Offset;
                            false -> Size
                          end,
                  {ReadAns, Bytes} = read_bytes(Storage_helper_info, File, Offset, Size2, FFI),

                  ErrorCode3 = veilhelpers:exec(release, Storage_helper_info, [File, FFI]),
                  case ErrorCode3 of
                    0 -> {ReadAns, Bytes};
                    {error, 'NIF_not_loaded'} -> ErrorCode3;
                    _ -> {wrong_release_return_code, ErrorCode3}
                  end;
                error -> {ErrorCode, FFI};
                _ -> {wrong_open_return_code, ErrorCode2}
              end;
            true  -> {error, file_too_small}
          end;
        false -> {error, not_regular_file}
      end;
    error -> {ErrorCode, Stat};
    _ -> {wrong_getatt_return_code, ErrorCode}
  end.

%% write_storage_system/4
%% ====================================================================
%% @doc Writes data to file (operates only on storage). First it checks file
%% attributes (file type and file size). If everything is ok,
%% it reads data from file.
%% @end
-spec write_storage_system(Storage_helper_info :: record(), File :: string(), Offset :: integer(), Buf :: binary()) -> Result when
  Result :: BytesWritten | {ErrorGeneral, ErrorDetail},
  BytesWritten :: integer(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
write_storage_system(Storage_helper_info, File, Offset, Buf) ->
  {ErrorCode, Stat} = veilhelpers:exec(getattr, Storage_helper_info, [File]),
  case ErrorCode of
    0 ->
      case veilhelpers:exec(is_reg, [Stat#st_stat.st_mode]) of
        true ->
          FSize = Stat#st_stat.st_size,
          case FSize =< Offset of
            false ->
              Flag = veilhelpers:exec(get_flag, [o_wronly]),
              {ErrorCode2, FFI} = veilhelpers:exec(open, Storage_helper_info, [File, #st_fuse_file_info{flags = Flag}]),
              case ErrorCode2 of
                0 ->
                  BytesWritten = write_bytes(Storage_helper_info, File, Offset, Buf, FFI),

                  ErrorCode3 = veilhelpers:exec(release, Storage_helper_info, [File, FFI]),
                  case ErrorCode3 of
                    0 -> BytesWritten;
                    {error, 'NIF_not_loaded'} -> ErrorCode3;
                    _ -> {wrong_release_return_code, ErrorCode3}
                  end;
                error -> {ErrorCode, FFI};
                _ -> {wrong_open_return_code, ErrorCode2}
              end;
            true  -> {error, file_too_small}
          end;
        false -> {error, not_regular_file}
      end;
    error -> {ErrorCode, Stat};
    _ -> {wrong_getatt_return_code, ErrorCode}
  end.

%% write_storage_system/3
%% ====================================================================
%% @doc Appends data to the end of file (operates only on storage).
%% First it checks file attributes (file type and file size).
%% If everything is ok, it reads data from file.
%% @end
-spec write_storage_system(Storage_helper_info :: record(), File :: string(), Buf :: binary()) -> Result when
  Result :: BytesWritten | {ErrorGeneral, ErrorDetail},
  BytesWritten :: integer(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
write_storage_system(Storage_helper_info, File, Buf) ->
  {ErrorCode, Stat} = veilhelpers:exec(getattr, Storage_helper_info, [File]),
  case ErrorCode of
    0 ->
      case veilhelpers:exec(is_reg, [Stat#st_stat.st_mode]) of
        true ->
          Offset = Stat#st_stat.st_size,
              Flag = veilhelpers:exec(get_flag, [o_wronly]),
              {ErrorCode2, FFI} = veilhelpers:exec(open, Storage_helper_info, [File, #st_fuse_file_info{flags = Flag}]),
              case ErrorCode2 of
                0 ->
                  BytesWritten = write_bytes(Storage_helper_info, File, Offset, Buf, FFI),

                  ErrorCode3 = veilhelpers:exec(release, Storage_helper_info, [File, FFI]),
                  case ErrorCode3 of
                    0 -> BytesWritten;
                    {error, 'NIF_not_loaded'} -> ErrorCode3;
                    _ -> {wrong_release_return_code, ErrorCode3}
                  end;
                error -> {ErrorCode, FFI};
                _ -> {wrong_open_return_code, ErrorCode2}
              end;
        false -> {error, not_regular_file}
      end;
    error -> {ErrorCode, Stat};
    _ -> {wrong_getatt_return_code, ErrorCode}
  end.

%% create_file_storage_system/2
%% ====================================================================
%% @doc Creates file (operates only on storage). First it checks if file
%% exists. If not, it creates file.
%% @end
-spec create_file_storage_system(Storage_helper_info :: record(), File :: string()) -> Result when
  Result :: ok | {ErrorGeneral, ErrorDetail},
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
create_file_storage_system(Storage_helper_info, File) ->
  {ErrorCode, Stat} = veilhelpers:exec(getattr, Storage_helper_info, [File]),
  case ErrorCode of
    0 -> {error, file_exists};
    error -> {ErrorCode, Stat};
    _ ->
      ErrorCode2 = veilhelpers:exec(mknod, Storage_helper_info, [File, ?NewFileStorageMode, 0]),
      case ErrorCode2 of
        0 ->
          ErrorCode3 = veilhelpers:exec(truncate, Storage_helper_info, [File, 0]),
          case ErrorCode3 of
            0 -> ok;
            {error, 'NIF_not_loaded'} -> ErrorCode3;
            _ -> {wrong_truncate_return_code, ErrorCode3}
          end;
        {error, 'NIF_not_loaded'} -> ErrorCode2;
        _ -> {wrong_mknod_return_code, ErrorCode2}
      end
  end.

%% delete_file_storage_system/2
%% ====================================================================
%% @doc Deletes file (operates only on storage). First it checks if file
%% exists and is regular file. If everything is ok, it deletes file.
%% @end
-spec delete_file_storage_system(Storage_helper_info :: record(), File :: string()) -> Result when
  Result :: ok | {ErrorGeneral, ErrorDetail},
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
delete_file_storage_system(Storage_helper_info, File) ->
  {ErrorCode, Stat} = veilhelpers:exec(getattr, Storage_helper_info, [File]),
  case ErrorCode of
    0 ->
      case veilhelpers:exec(is_reg, [Stat#st_stat.st_mode]) of
        true ->
          ErrorCode2 = veilhelpers:exec(unlink, Storage_helper_info, [File]),
          case ErrorCode2 of
            0 -> ok;
            {error, 'NIF_not_loaded'} -> ErrorCode2;
            _ -> {wrong_unlink_return_code, ErrorCode2}
          end;
        false -> {error, not_regular_file}
      end;
    error -> {ErrorCode, Stat};
    _ -> {wrong_getatt_return_code, ErrorCode}
  end.

%% ls_storage_system/0
%% ====================================================================
%% @doc Lists files in directory on storage
%% @end
-spec ls_storage_system() -> {error, not_implemented_yet}.
%% ====================================================================
ls_storage_system() ->
  %% czy taka funkcja jest nam do czegoś potrzebna - w końcu znane będą pliki z bazy jak i kopie tymczasowe?
  {error, not_implemented_yet}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% contact_fslogic/1
%% ====================================================================
%% @doc Sends request to and receives answer from fslogic
%% @end
-spec contact_fslogic(Record :: record()) -> Result when
  Result :: {ok, FSLogicAns} | {ErrorGeneral, ErrorDetail},
  FSLogicAns :: record(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
contact_fslogic(Record) ->
  MsgId = case get(files_manager_msg_id) of
            ID when is_integer(ID) ->
              put(files_manager_msg_id, ID + 1);
            _ -> put(files_manager_msg_id, 0)
          end,

  CallAns = gen_server:call(?Dispatcher_Name, {fslogic, 1, self(), MsgId, {internal_call, Record}}),
  case CallAns of
    ok ->
      receive
        {worker_answer, MsgId, Resp} -> {ok, Resp}
      after 1000 ->
        {error, timeout}
      end;
    _ -> {error, CallAns}
  end.

%% read_bytes/5
%% ====================================================================
%% @doc Reads file (operates only on storage).It contains loop that reads
%% data until all requested data is read (storage may not be able to provide
%% all requested data at once).
%% @end
-spec read_bytes(Storage_helper_info :: record(), File :: string(), Offset :: integer(), Size :: integer(), FFI :: #st_fuse_file_info{}) -> Result when
  Result :: {ok, Bytes} | {ErrorGeneral, ErrorDetail},
  Bytes :: binary(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
read_bytes(_Storage_helper_info, _File, _Offset, 0, _FFI) ->
  {ok, <<>>};

read_bytes(Storage_helper_info, File, Offset, Size, FFI) ->
  {ErrorCode, Bytes} = veilhelpers:exec(read, Storage_helper_info, [File, Size, Offset, FFI]),
  case ErrorCode of
    0 -> {error, bytes_cannot_be_read};
    BytesNum when is_integer(BytesNum) ->
      {TmpErrorCode, TmpBytes} = read_bytes(Storage_helper_info, File, Offset + BytesNum, Size - BytesNum, FFI),
      case TmpErrorCode of
        ok -> {ok, <<Bytes/binary, TmpBytes/binary>>};
        _ -> {TmpErrorCode, TmpBytes}
      end;
    error -> {ErrorCode, Bytes};
    _ -> {error, wrong_read_return_code}
  end.

%% write_bytes/5
%% ====================================================================
%% @doc Writes data to file (operates only on storage). It contains loop
%% that writes data until all data is written (storage may not be able to
%% save all data at once).
%% @end
-spec write_bytes(Storage_helper_info :: record(), File :: string(), Offset :: integer(), Buf :: binary(), FFI :: #st_fuse_file_info{}) -> Result when
  Result :: BytesWritten | {ErrorGeneral, ErrorDetail},
  BytesWritten :: integer(),
  ErrorGeneral :: atom(),
  ErrorDetail :: term().
%% ====================================================================
write_bytes(_Storage_helper_info, _File, _Offset, <<>>, _FFI) ->
  0;

write_bytes(Storage_helper_info, File, Offset, Buf, FFI) ->
  ErrorCode = veilhelpers:exec(write, Storage_helper_info, [File, Buf, Offset, FFI]),
  case ErrorCode of
    0 -> {error, bytes_cannot_be_written};
    BytesNum when is_integer(BytesNum) ->
      <<_:BytesNum/binary, NewBuf/binary>> = Buf,
      TmpErrorCode = write_bytes(Storage_helper_info, File, Offset + BytesNum, NewBuf, FFI),
      case TmpErrorCode of
        BytesNum2 when is_integer(BytesNum2) -> BytesNum2 + BytesNum;
        _ -> TmpErrorCode
      end;
    {error, 'NIF_not_loaded'} -> ErrorCode;
    _ -> {error, wrong_write_return_code}
  end.