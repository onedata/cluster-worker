/*********************************************************************
*  @author Rafal Slota
*  @copyright (C): 2013 ACK CYFRONET AGH
*  This software is released under the MIT license
*  cited in 'LICENSE.txt'.
*********************************************************************/

#include "erl_nif.h"
#include <fuse.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <vector>
#include <boost/shared_ptr.hpp>

#include "helpers/IStorageHelper.hh"
#include "helpers/storageHelperFactory.hh"

#include "term_translator.h"

#define BADARG enif_make_badarg(env)
#define INIT    if(!check_common_args(env, argc, argv)) \
                    return BADARG; \
                shared_ptr<IStorageHelper> sh = SHFactory.getStorageHelper(get_string(env, argv[0]), get_str_vector(env, argv[1])); \
                if(!sh) \
                    return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_atom(env, "unknown_storage_helper"));

using namespace std;
using namespace boost;

StorageHelperFactory SHFactory;     // StorageHelperFactory instance

/*********************************************************************
*
*                          WRAPPERS (NIF based)
*       All functions below are described in veilhelpers_nif.erl
*
*********************************************************************/

static ERL_NIF_TERM sh_getattr(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    struct stat st;
    int ret = sh->sh_getattr(get_string(env, argv[2]).c_str(), &st);
    
    return enif_make_tuple2(env, enif_make_int(env, ret), make_stat(env, st));
}

static ERL_NIF_TERM sh_access(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[3]))
        return BADARG;

    return enif_make_int(env, sh->sh_access(get_string(env, argv[2]).c_str(), get_int(env, argv[3])));
}

static ERL_NIF_TERM sh_mknod(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[3]) || !is_int(env, argv[4]))
        return BADARG;

    return enif_make_int(env, sh->sh_mknod(get_string(env, argv[2]).c_str(), get_int(env, argv[3]), get_int(env, argv[4])));
}

static ERL_NIF_TERM sh_unlink(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    return enif_make_int(env, sh->sh_unlink(get_string(env, argv[2]).c_str()));
}

static ERL_NIF_TERM sh_rename(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    return enif_make_int(env, sh->sh_rename(get_string(env, argv[2]).c_str(), get_string(env, argv[3]).c_str()));
}

static ERL_NIF_TERM sh_chmod(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[3]))
        return BADARG;

    return enif_make_int(env, sh->sh_chmod(get_string(env, argv[2]).c_str(), get_int(env, argv[3])));
}

static ERL_NIF_TERM sh_chown(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[3]) || !is_int(env, argv[4]))
        return BADARG;

    return enif_make_int(env, sh->sh_chown(get_string(env, argv[2]).c_str(), get_int(env, argv[3]), get_int(env, argv[4])));
}

static ERL_NIF_TERM sh_truncate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[3]))
        return BADARG;

    return enif_make_int(env, sh->sh_truncate(get_string(env, argv[2]).c_str(), get_int(env, argv[3])));
}

static ERL_NIF_TERM sh_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;
  
    struct fuse_file_info ffi = get_ffi(env, argv[4]);
    int ret = sh->sh_open(get_string(env, argv[2]).c_str(), &ffi);

    return enif_make_tuple2(env, enif_make_int(env, ret), make_ffi(env, ffi));
}

static ERL_NIF_TERM sh_read(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[3]) || !is_int(env, argv[4]))
        return BADARG;

    struct fuse_file_info ffi = get_ffi(env, argv[5]);
    unsigned int size = get_int(env, argv[3]);
    ERL_NIF_TERM bin;
    char *buff = new char[size];

    int ret = sh->sh_read(get_string(env, argv[2]).c_str(), buff, size, get_int(env, argv[4]), &ffi);
    char *tmp = (char *) enif_make_new_binary(env, (ret > 0 ? ret : 0), &bin);
    memcpy(tmp, buff, (ret > 0 ? ret : 0));

    return enif_make_tuple2(env, enif_make_int(env, ret), bin);
}

static ERL_NIF_TERM sh_write(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[4]))
        return BADARG;

    struct fuse_file_info ffi = get_ffi(env, argv[5]);
    ErlNifBinary bin;
    if(!enif_inspect_binary(env, argv[3], &bin))
        return BADARG;

    return enif_make_int(env, sh->sh_write(get_string(env, argv[2]).c_str(), (const char*)bin.data, bin.size, get_int(env, argv[4]), &ffi));
}

static ERL_NIF_TERM sh_statfs(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    struct statvfs stat;
    int ret = sh->sh_statfs(get_string(env, argv[2]).c_str(), &stat);

    return enif_make_tuple2(env, enif_make_int(env, ret), make_statvfs(env, stat));
}

static ERL_NIF_TERM sh_release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    struct fuse_file_info ffi = get_ffi(env, argv[3]);
    return enif_make_int(env, sh->sh_release(get_string(env, argv[2]).c_str(), &ffi));
}

static ERL_NIF_TERM sh_fsync(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[3]))
        return BADARG;

    struct fuse_file_info ffi = get_ffi(env, argv[4]);

    return enif_make_int(env, sh->sh_fsync(get_string(env, argv[2]).c_str(), get_int(env, argv[3]), &ffi));
}

static ERL_NIF_TERM sh_mkdir(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    if(!is_int(env, argv[3]))
        return BADARG;

    return enif_make_int(env, sh->sh_mkdir(get_string(env, argv[2]).c_str(), get_int(env, argv[3])));
}

static ERL_NIF_TERM sh_rmdir(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    INIT;

    return enif_make_int(env, sh->sh_rmdir(get_string(env, argv[2]).c_str()));
}

static ErlNifFunc nif_funcs[] =
{
    {"getattr",     3, sh_getattr},
    {"access",      4, sh_access},
    {"mknod",       5, sh_mknod},
    {"unlink",      3, sh_unlink},
    {"rename",      4, sh_rename},
    {"chmod",       4, sh_chmod},
    {"chown",       5, sh_chown},
    {"truncate",    4, sh_truncate},
    {"open",        4, sh_open},
    {"read",        6, sh_read},
    {"write",       6, sh_write},
    {"statfs",      3, sh_statfs},
    {"release",     4, sh_release},
    {"fsync",       5, sh_fsync},
    {"mkdir",       4, sh_mkdir},
    {"rmdir",       3, sh_rmdir}
};


ERL_NIF_INIT(veilhelpers_nif, nif_funcs, NULL,NULL,NULL,NULL);