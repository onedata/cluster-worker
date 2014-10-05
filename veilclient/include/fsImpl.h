/**
 * @file fsImpl.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef ONECLIENT_FS_IMPL_H
#define ONECLIENT_FS_IMPL_H


#include "ISchedulable.h"

#include <boost/thread/shared_mutex.hpp>

#include <fuse.h>

#include <list>
#include <map>
#include <memory>
#include <unordered_map>

namespace one
{

/// The name of default global config file
static constexpr const char *GLOBAL_CONFIG_FILE = "oneclient.conf";

/**
 * How many dirent should be fetch from cluster at once.
 * Note that each opendir syscall will query at least DIR_BATCH_SIZE dirents
 */
static constexpr int DIR_BATCH_SIZE = 10;

namespace helpers
{
class IStorageHelper;
class StorageHelperFactory;
}

namespace client
{

class Context;
class FslogicProxy;
class LocalStorageManager;
class MetaCache;
class StorageMapper;

/// forward declarations
namespace events
{
class EventCommunicator;
}

/**
 * The FsImpl main class.
 * This class contains FUSE all callbacks, so it basically is an heart of the filesystem.
 * Technically FsImpl is an singleton created on programm start and registred in FUSE
 * daemon.
 */
class FsImpl: public ISchedulable
{
public:
        FsImpl(std::string path, std::shared_ptr<Context> context,
               std::shared_ptr<FslogicProxy> fslogic, std::shared_ptr<MetaCache> metaCache,
               std::shared_ptr<LocalStorageManager> sManager,
               std::shared_ptr<helpers::StorageHelperFactory> sh_factory,
               std::shared_ptr<events::EventCommunicator> eventCommunicator); ///< FsImpl constructor.
        virtual ~FsImpl();

        int access(const char *path, int mask); /**< *access* FUSE callback. Not implemented yet. */
        int getattr(const char *path, struct stat *statbuf, bool fuse_ctx = true); /**< *getattr* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int readlink(const char *path, char *link, size_t size); /**< *readlink* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int mknod(const char *path, mode_t mode, dev_t dev); /**< *mknod* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int mkdir(const char *path, mode_t mode); /**< mkdir FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int unlink(const char *path); /**< *unlink* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int rmdir(const char *path); /**< *rmdir* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int symlink(const char *path, const char *link); /**< *symlink* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int rename(const char *path, const char *newpath); /**< *rename* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int link(const char *path, const char *newpath); /**< *link* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int chmod(const char *path, mode_t mode); /**< *chmod* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int chown(const char *path, uid_t uid, gid_t gid); /**< *chown* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int truncate(const char *path, off_t newSize); /**< *truncate* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int utime(const char *path, struct utimbuf *ubuf); /**< *utime* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int open(const char *path, struct fuse_file_info *fileInfo); /**< *open* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo); /**< *read* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo); /**< *write* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int statfs(const char *path, struct statvfs *statInfo); /**< *statfs* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int flush(const char *path, struct fuse_file_info *fileInfo); /**< *flush* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int release(const char *path, struct fuse_file_info *fileInfo); /**< *release* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int fsync(const char *path, int datasync, struct fuse_file_info *fi); /**< *fsync* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int setxattr(const char *path, const char *name, const char *value, size_t size, int flags); /**< *setxattr* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int getxattr(const char *path, const char *name, char *value, size_t size); /**< *getxattr* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int listxattr(const char *path, char *list, size_t size); /**< *listxattr* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int removexattr(const char *path, const char *name); /**< *removexattr* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int opendir(const char *path, struct fuse_file_info *fileInfo); /**< *opendir* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fileInfo); /**< *readdir* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int releasedir(const char *path, struct fuse_file_info *fileInfo); /**< *releasedir* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int fsyncdir(const char *path, int datasync, struct fuse_file_info *fileInfo); /**< *fsyncdir* FUSE callback. Not implemented yet. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */
        int init(struct fuse_conn_info *conn); /**< *init* FUSE callback. @see http://fuse.sourceforge.net/doxygen/structfuse__operations.html */

        virtual bool needsForceClusterProxy(const std::string &path); ///< Checks if user is able to use 'user' or 'group' permissions to access the file given by path.
        virtual bool runTask(TaskID taskId, const std::string &arg0, const std::string &arg1, const std::string &arg3); ///< Task runner derived from ISchedulable. @see ISchedulable::runTask

protected:
        /**
         * The SHCache struct is responsible for mapping file ID to an instance
         * of @c helpers::IStorageHelper in a thread-safe manner.
         */
        struct SHCache
        {
            using key = uint64_t;
            using value = std::shared_ptr<helpers::IStorageHelper>;

        public:
            /**
             * @param id The file id.
             * @return @c helpers::IStorageHelper instance mapped for the id.
             */
            value get(const key id);

            /**
             * @param id A file id.
             * @param sh A @c helpers::IStorageHelper instance to bind to the id.
             */
            void  set(const key id, value sh);

            /**
             * Removes a cached @c helpers::IStorageHelper instance and returns
             * it.
             * @copydoc SHCache::get(id)
             */
            value take(const key id);

        private:
            std::unordered_map<key, value> m_shCache;
            boost::shared_mutex m_shCacheMutex;
        };


        std::string m_root; ///< Filesystem root directory
        uid_t       m_uid;  ///< Filesystem owner's effective uid
        gid_t       m_gid;  ///< Filesystem owner's effective gid
        uid_t       m_ruid;  ///< Filesystem root real uid
        gid_t       m_rgid;  ///< Filesystem root real gid
        uint64_t    m_fh;

        std::shared_ptr<FslogicProxy> m_fslogic;             ///< FslogicProxy instance
        std::shared_ptr<MetaCache> m_metaCache;              ///< MetaCache instance
        std::shared_ptr<LocalStorageManager> m_sManager;     ///< LocalStorageManager instance
        std::shared_ptr<helpers::StorageHelperFactory> m_shFactory;   ///< Storage Helpers Factory instance
        std::shared_ptr<events::EventCommunicator> m_eventCommunicator;

        std::map<std::string, std::pair<std::string, time_t> > m_linkCache;         ///< Simple links cache
        boost::upgrade_mutex m_linkCacheMutex;
        SHCache m_shCache; ///< Storage Helpers' cache.

private:
        const std::shared_ptr<Context> m_context;
};

} // namespace client
} // namespace one

#endif // ONECLIENT_FS_IMPL_H
