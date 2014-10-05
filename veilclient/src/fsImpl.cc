/**
 * @file fsImpl.cc
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "fsImpl.h"

#include "communication_protocol.pb.h"
#include "communication/communicator.h"
#include "config.h"
#include "context.h"
#include "events/event.h"
#include "events/eventCommunicator.h"
#include "fslogicProxy.h"
#include "fuse_messages.pb.h"
#include "helpers/storageHelperFactory.h"
#include "jobScheduler.h"
#include "localStorageManager.h"
#include "logging.h"
#include "metaCache.h"
#include "options.h"
#include "pushListener.h"
#include "storageMapper.h"
#include "oneErrors.h"
#include "oneException.h"

#include <grp.h>
#include <pwd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread/shared_lock_guard.hpp>
#include <google/protobuf/descriptor.h>

#include <algorithm>
#include <cstring>
#include <functional>


/// Runs FUN on NAME storage helper with constructed with ARGS. Return value is avaiable in 'int sh_return'.
#define CUSTOM_SH_RUN(PTR, FUN) if(!PTR) { LOG(ERROR) << "Invalid storage helper's pointer!"; return -EIO; } \
                                int sh_return = PTR->FUN; \
                                if(sh_return < 0) LOG(INFO) << "Storage helper returned error: " << sh_return;
#define SH_RUN(NAME, ARGS, FUN) auto ptr = m_shFactory->getStorageHelper(NAME, ARGS); \
                                if(!ptr) { LOG(ERROR) << "storage helper '" << NAME << "' not found"; return -EIO; } \
                                CUSTOM_SH_RUN(ptr, FUN)

/// If given oneError does not produce POSIX 0 return code, interrupt execution by returning POSIX error code.
#define RETURN_IF_ERROR(X)  { \
                                int err = translateError(X); \
                                if(err != 0) { LOG(INFO) << "Returning error: " << err; return err; } \
                            }

/// Fetch locationInfo and storageInfo for given file.
/// On success - lInfo and sInfo variables will be set.
/// On error - POSIX error code will be returned, interrupting code execution.
#define GET_LOCATION_INFO(PATH, FORCE_PROXY) locationInfo lInfo; \
                                storageInfo sInfo; \
                                try \
                                { \
                                    pair<locationInfo, storageInfo> tmpLoc = m_context->getStorageMapper()->getLocationInfo(string(PATH), true, FORCE_PROXY); \
                                    lInfo = tmpLoc.first; \
                                    sInfo = tmpLoc.second; \
                                } \
                                catch(OneException e) \
                                { \
                                    LOG(WARNING) << "cannot get file mapping for file: " << string(PATH) << " (error: " << e.what() << ")"; \
                                    return translateError(e.oneError()); \
                                }

using namespace std;
using namespace one::clproto::fuse_messages;

namespace
{
/// Get parent path (as string)
inline std::string parent(const boost::filesystem::path &p)
{
    return p.branch_path().string();
}
}

namespace one {
namespace client {

FsImpl::FsImpl(string path, std::shared_ptr<Context> context,
               std::shared_ptr<FslogicProxy> fslogic,  std::shared_ptr<MetaCache> metaCache,
               std::shared_ptr<LocalStorageManager> sManager,
               std::shared_ptr<helpers::StorageHelperFactory> sh_factory,
               std::shared_ptr<events::EventCommunicator> eventCommunicator) :
    m_fh(0),
    m_fslogic(fslogic),
    m_metaCache(metaCache),
    m_sManager(sManager),
    m_shFactory(sh_factory),
    m_eventCommunicator(eventCommunicator),
    m_context{std::move(context)}
{
    if(path.size() > 1 && path[path.size()-1] == '/')
        path = path.substr(0, path.size()-1);
    LOG(INFO) << "setting VFS root dir as: " << string(path);
    m_root = path;

    // Construct new PushListener
    auto pushListener = std::make_shared<PushListener>(m_context);
    m_context->setPushListener(pushListener);

    // Update FUSE_ID in current connection pool
    m_context->getCommunicator()->setFuseId(m_context->getConfig()->getFuseID());
    m_context->getCommunicator()->setupPushChannels(std::bind(&PushListener::onMessage, pushListener, std::placeholders::_1));

    // Initialize cluster handshake in order to receive FuseID
    if(m_context->getConfig()->getFuseID() == "")
        m_context->getConfig()->negotiateFuseID();

    if(m_fslogic) {
        if(m_context->getScheduler() && m_context->getConfig()) {
            int alive = m_context->getOptions()->get_alive_meta_connections_count();
            for(int i = 0; i < alive; ++i) {
                Job pingTask = Job(time(NULL) + i, m_fslogic, ISchedulable::TASK_PING_CLUSTER, std::to_string(m_context->getOptions()->get_alive_meta_connections_count()));
                m_context->getScheduler(ISchedulable::TASK_PING_CLUSTER)->addTask(pingTask);
            }

        } else
            LOG(WARNING) << "Connection keep-alive subsystem cannot be started.";
    }

    if(!m_context->getOptions()->has_fuse_group_id() && !m_context->getConfig()->isEnvSet(string(FUSE_OPT_PREFIX) + string("GROUP_ID"))) {
        if(m_sManager) {
            vector<boost::filesystem::path> mountPoints = m_sManager->getMountPoints();
            vector< pair<int, string> > clientStorageInfo = m_sManager->getClientStorageInfo(mountPoints);
            if(!clientStorageInfo.empty()) {
                m_sManager->sendClientStorageInfo(clientStorageInfo);
            }
        }
    }

    m_uid = geteuid();
    m_gid = getegid();
    // Real IDs should be set real owner's ID of "/" directory by first getattr call
    m_ruid = -1;
    m_rgid = -1;

    if(m_eventCommunicator){
        eventCommunicator->setFslogic(m_fslogic);
        eventCommunicator->setMetaCache(m_metaCache);

        m_eventCommunicator->addStatAfterWritesRule(m_context->getOptions()->get_write_bytes_before_stat());
    }

    m_context->getPushListener()->subscribe(std::bind(&events::EventCommunicator::pushMessagesHandler, m_eventCommunicator.get(), std::placeholders::_1));
    m_context->getScheduler(ISchedulable::TASK_GET_EVENT_PRODUCER_CONFIG)->addTask(Job(time(NULL), m_eventCommunicator, ISchedulable::TASK_GET_EVENT_PRODUCER_CONFIG));
    m_context->getScheduler(ISchedulable::TASK_IS_WRITE_ENABLED)->addTask(Job(time(NULL), m_eventCommunicator, ISchedulable::TASK_IS_WRITE_ENABLED));
}

FsImpl::~FsImpl()
{
}

int FsImpl::access(const char *path, int mask)
{
    LOG(INFO) << "FUSE: access(path: " << string(path) << ", mask: " << mask << ")";

    // Always allow accessing file
    // This method should be not called in first place. If it is, use 'default_permissions' FUSE flag.
    // Even without this flag, letting this method to return always (int)0 is just OK.
    return 0;
}

int FsImpl::getattr(const char *path, struct stat *statbuf, bool fuse_ctx)
{
    if(fuse_ctx)
        LOG(INFO) << "FUSE: getattr(path: " << string(path) << ", statbuf)";

    FileAttr attr;

    statbuf->st_blocks = 0;
    statbuf->st_nlink = 1;
    statbuf->st_uid = -1;
    statbuf->st_gid = -1;
    statbuf->st_size = 0;
    statbuf->st_atime = 0;
    statbuf->st_mtime = 0;
    statbuf->st_ctime = 0;

    m_context->getStorageMapper()->resetHelperOverride(path);

    if(!m_metaCache->getAttr(string(path), statbuf))
    {
        // We do not have storage mapping so we have to comunicate with cluster anyway
        LOG(INFO) << "storage mapping not exists in cache for file: " << string(path);

        if(!m_fslogic->getFileAttr(string(path), attr))
            return -EIO;

        if(attr.answer() != VOK)
        {
            LOG(WARNING) << "Cluster answer: " << attr.answer();
            return translateError(attr.answer());
        }

        if(attr.type() == "REG" && fuse_ctx) // We'll need storage mapping for regular file
        {
            Job getLocTask = Job(time(NULL), m_context->getStorageMapper(), ISchedulable::TASK_ASYNC_GET_FILE_LOCATION, string(path));
            m_context->getScheduler()->addTask(getLocTask);
        }

        // At this point we have attributes from cluster

        statbuf->st_mode = attr.mode(); // File type still has to be set, fslogic gives only permissions in mode field
        statbuf->st_nlink = attr.links();

        statbuf->st_atime = attr.atime();
        statbuf->st_mtime = attr.mtime();
        statbuf->st_ctime = attr.ctime();

        uid_t uid = attr.uid();
        gid_t gid = attr.gid();

        if(string(path) == "/") { // FsImpl root should always belong to FUSE owner
            m_ruid = uid;
            m_rgid = gid;
        }

        struct passwd *ownerInfo = getpwnam(attr.uname().c_str()); // Static buffer, do NOT free !
        struct group *groupInfo = getgrnam(attr.gname().c_str());  // Static buffer, do NOT free !

        statbuf->st_uid   = (ownerInfo ? ownerInfo->pw_uid : uid);
        statbuf->st_gid   = (groupInfo ? groupInfo->gr_gid : gid);

        if(attr.type() == "DIR")
        {
            statbuf->st_mode |= S_IFDIR;

            // Prefetch "ls" result
            if(fuse_ctx && m_context->getOptions()->get_enable_dir_prefetch()  && m_context->getOptions()->get_enable_attr_cache()) {
                Job readDirTask = Job(time(NULL), shared_from_this(), ISchedulable::TASK_ASYNC_READDIR, string(path), "0");
                m_context->getScheduler()->addTask(readDirTask);
            }
        }
        else if(attr.type() == "LNK")
        {
            statbuf->st_mode |= S_IFLNK;

            // Check cache for validity
        boost::unique_lock<boost::upgrade_mutex> lock{m_linkCacheMutex};
            map<string, pair<string, time_t> >::iterator it = m_linkCache.find(string(path));
            if(it != m_linkCache.end() && statbuf->st_mtime > (*it).second.second)
            {
                m_linkCache.erase(it);
            }
        }
        else
        {
            statbuf->st_mode |= S_IFREG;
            statbuf->st_size = attr.size();
        }

        m_metaCache->addAttr(string(path), *statbuf);
    }

    return 0;
}

int FsImpl::readlink(const char *path, char *link, size_t size)
{
    LOG(INFO) << "FUSE: readlink(path: " << string(path) << ")";
    string target;

    boost::upgrade_lock<boost::upgrade_mutex> lock{m_linkCacheMutex};
    map<string, pair<string, time_t> >::const_iterator it = m_linkCache.find(string(path));
    if(it != m_linkCache.end()) {
        target = (*it).second.first;
    } else {
        pair<string, string> resp = m_fslogic->getLink(string(path));
        target = resp.second;
        RETURN_IF_ERROR(resp.first);

        boost::upgrade_to_unique_lock<boost::upgrade_mutex> writeLock{lock};
        m_linkCache[string(path)] = pair<string, time_t>(target, time(NULL));
    }

    if(target.size() == 0) {
        link[0] = 0;
        return 0;
    }

    if(target[0] == '/')
        target = m_root + target;

    int path_size = min(size - 1, target.size()); // truncate path if needed
    memcpy(link, target.c_str(), path_size);
    link[path_size] = 0;

    return 0;
}

int FsImpl::mknod(const char *path, mode_t mode, dev_t dev)
{
    LOG(INFO) << "FUSE: mknod(path: " << string(path) << ", mode: " << mode << ", ...)";
    if(!(mode & S_IFREG))
    {
        LOG(WARNING) << "cannot create non-regular file"; // TODO: or maybe it could be?
        return -EFAULT;
    }

    m_metaCache->clearAttr(string(path));

    FileLocation location;
    if(!m_fslogic->getNewFileLocation(string(path), mode & ALLPERMS, location, needsForceClusterProxy(parent(path))))
    {
        LOG(WARNING) << "cannot fetch new file location mapping";
        return -EIO;
    }

    if(location.answer() != VOK)
    {
        LOG(WARNING) << "cannot create node due to cluster error: " << location.answer();
        return translateError(location.answer());
    }

    m_context->getStorageMapper()->addLocation(string(path), location);
    GET_LOCATION_INFO(path, false);

    SH_RUN(sInfo.storageHelperName, sInfo.storageHelperArgs, sh_mknod(lInfo.fileId.c_str(), mode, dev));

    // if file existed before we consider it as a success and we want to apply same actions (chown and sending an acknowledgement)
    if(sh_return == -EEXIST)
        sh_return = 0;

    if(sh_return != 0)
        (void) m_fslogic->deleteFile(string(path));
    else { // File created, now we shall take care of its owner.
        std::vector<std::string> tokens;
        std::string sPath = string(path).substr(1);
        boost::split(tokens, sPath, boost::is_any_of("/"));

        if(tokens.size() > 2 && tokens[0] == "groups") // We are creating file in groups directory
        {
            string groupName = tokens[1];
            struct group *groupInfo = getgrnam(groupName.c_str());  // Static buffer, do NOT free !
            gid_t gid = (groupInfo ? groupInfo->gr_gid : -1);

            // We need to change group owner of this file
            SH_RUN(sInfo.storageHelperName, sInfo.storageHelperArgs, sh_chown(lInfo.fileId.c_str(), -1, gid));
            if(sh_return != 0)
                LOG(ERROR) << "Cannot change group owner of file " << sPath << " to: " << groupName;
        }

        m_context->getScheduler()->addTask(Job(time(NULL) + 5, shared_from_this(), TASK_CLEAR_ATTR, parent(path).c_str())); // Clear cache of parent (possible change of modify time)

        RETURN_IF_ERROR(m_fslogic->sendFileCreatedAck(string(path)));
    }
    return sh_return;
}

int FsImpl::mkdir(const char *path, mode_t mode)
{
    LOG(INFO) << "FUSE: mkdir(path: " << string(path) << ", mode: " << mode << ")";
    m_metaCache->clearAttr(string(path));
    // Clear parent's cache
    m_metaCache->clearAttr(parent(path).c_str());

    RETURN_IF_ERROR(m_fslogic->createDir(string(path), mode & ALLPERMS));
    m_context->getScheduler()->addTask(Job(time(NULL) + 5, shared_from_this(), TASK_CLEAR_ATTR, parent(path).c_str())); // Clear cache of parent (possible change of modify time)

    std::shared_ptr<events::Event> mkdirEvent = events::Event::createMkdirEvent(path);
    m_eventCommunicator->processEvent(mkdirEvent);

    return 0;
}

int FsImpl::unlink(const char *path)
{
    LOG(INFO) << "FUSE: unlink(path: " << string(path) << ")";
    struct stat statbuf;
    FileAttr attr;
    int isLink = 0;

    int attrStatus = getattr(path, &statbuf, false);
    isLink = S_ISLNK(statbuf.st_mode);

    m_metaCache->clearAttr(string(path)); // Clear cache

    if(!isLink)
    {
        m_context->getStorageMapper()->clearMappings(path);
        GET_LOCATION_INFO(path, needsForceClusterProxy(parent(path)) || attrStatus || !m_metaCache->canUseDefaultPermissions(statbuf)); //Get file location from cluster
        RETURN_IF_ERROR(m_fslogic->deleteFile(string(path)));

        SH_RUN(sInfo.storageHelperName, sInfo.storageHelperArgs, sh_unlink(lInfo.fileId.c_str()));
        if(sh_return < 0)
            return sh_return;
    } else
    {
        RETURN_IF_ERROR(m_fslogic->deleteFile(string(path)));
    }

    m_context->getScheduler()->addTask(Job(time(NULL) + 5, shared_from_this(), TASK_CLEAR_ATTR, parent(path).c_str())); // Clear cache of parent (possible change of modify time)

    std::shared_ptr<events::Event> rmEvent = events::Event::createRmEvent(path);
    m_eventCommunicator->processEvent(rmEvent);

    return 0;
}

int FsImpl::rmdir(const char *path)
{
    LOG(INFO) << "FUSE: rmdir(path: " << string(path) << ")";
    m_metaCache->clearAttr(string(path));
    // Clear parent's cache
    m_metaCache->clearAttr(parent(path).c_str());

    RETURN_IF_ERROR(m_fslogic->deleteFile(string(path)));
    m_context->getScheduler()->addTask(Job(time(NULL) + 5, shared_from_this(), TASK_CLEAR_ATTR, parent(path).c_str())); // Clear cache of parent (possible change of modify time)

    return 0;
}

int FsImpl::symlink(const char *to, const char *from)
{
    LOG(INFO) << "FUSE: symlink(path: " << string(from) << ", link: "<< string(to)  <<")";
    string toStr = string(to);
    if(toStr.size() >= m_root.size() && mismatch(m_root.begin(), m_root.end(), toStr.begin()).first == m_root.end()) {
        toStr = toStr.substr(m_root.size());
        if(toStr.size() == 0)
            toStr = "/";
        else if(toStr[0] != '/')
            toStr = string(to);
    }

    LOG(INFO) << "Creating link " << string(from) << "pointing to: " << toStr;

    RETURN_IF_ERROR(m_fslogic->createLink(string(from), toStr));
    return 0;
}

int FsImpl::rename(const char *path, const char *newpath)
{
    LOG(INFO) << "FUSE: rename(path: " << string(path) << ", newpath: "<< string(newpath)  <<")";

    RETURN_IF_ERROR(m_fslogic->renameFile(string(path), string(newpath)));
    m_context->getScheduler()->addTask(Job(time(NULL) + 5, shared_from_this(), TASK_CLEAR_ATTR, parent(path).c_str())); // Clear cache of parent (possible change of modify time)
    m_context->getScheduler()->addTask(Job(time(NULL) + 5, shared_from_this(), TASK_CLEAR_ATTR, parent(newpath))); // Clear cache of parent (possible change of modify time)

    m_metaCache->clearAttr(string(path));
    return 0;
}

int FsImpl::link(const char *path, const char *newpath)
{
    LOG(INFO) << "FUSE: link(path: " << string(path) << ", newpath: "<< string(newpath)  <<")";
    return -ENOTSUP;
}

int FsImpl::chmod(const char *path, mode_t mode)
{
    LOG(INFO) << "FUSE: chmod(path: " << string(path) << ", mode: "<< mode << ")";
    RETURN_IF_ERROR(m_fslogic->changeFilePerms(string(path), mode & ALLPERMS)); // ALLPERMS = 07777

    m_metaCache->clearAttr(string(path));

    // Chceck is its not regular file
    if(!S_ISREG(mode))
        return 0;

    // If it is, we have to call storage haleper's chmod
    GET_LOCATION_INFO(path, needsForceClusterProxy(path));

    SH_RUN(sInfo.storageHelperName, sInfo.storageHelperArgs, sh_chmod(lInfo.fileId.c_str(), mode));
    return sh_return;
}

int FsImpl::chown(const char *path, uid_t uid, gid_t gid)
{
    LOG(INFO) << "FUSE: chown(path: " << string(path) << ", uid: "<< uid << ", gid: " << gid <<")";

    struct passwd *ownerInfo = getpwuid(uid); // Static buffer, do NOT free !
    struct group *groupInfo = getgrgid(gid); // Static buffer, do NOT free !

    string uname = "", gname = "";
    if(ownerInfo)
        uname = ownerInfo->pw_name;
    if(groupInfo)
        gname = groupInfo->gr_name;

    m_metaCache->clearAttr(string(path));

    if((uid_t)-1 != uid)
        RETURN_IF_ERROR(m_fslogic->changeFileOwner(string(path), uid, uname));

    if((gid_t)-1 != gid)
        RETURN_IF_ERROR(m_fslogic->changeFileGroup(string(path), gid, gname));

    return 0;
}

int FsImpl::truncate(const char *path, off_t newSize)
{
    LOG(INFO) << "FUSE: truncate(path: " << string(path) << ", newSize: "<< newSize <<")";

    GET_LOCATION_INFO(path, needsForceClusterProxy(path));

    SH_RUN(sInfo.storageHelperName, sInfo.storageHelperArgs, sh_truncate(lInfo.fileId.c_str(), newSize));

    if(sh_return == 0) {
        (void) m_metaCache->updateSize(string(path), newSize);

        Job postTruncateTask = Job(time(NULL), shared_from_this(), TASK_POST_TRUNCATE_ACTIONS, path, utils::toString(newSize));
        m_context->getScheduler()->addTask(postTruncateTask);
    }

    return sh_return;
}

int FsImpl::utime(const char *path, struct utimbuf *ubuf)
{
    LOG(INFO) << "FUSE: utime(path: " << string(path) << ", ...)";

    // Update access times in meta cache right away
    (void) m_metaCache->updateTimes(string(path), ubuf->actime, ubuf->modtime);

    m_context->getScheduler()->addTask(Job(time(NULL), shared_from_this(), TASK_ASYNC_UPDATE_TIMES, string(path), utils::toString(ubuf->actime), utils::toString(ubuf->modtime)));

    return 0;
}

int FsImpl::open(const char *path, struct fuse_file_info *fileInfo)
{
    LOG(INFO) << "FUSE: open(path: " << string(path) << ", ...)";
    fileInfo->direct_io = 1;
    fileInfo->fh = ++m_fh;
    mode_t accMode = fileInfo->flags & O_ACCMODE;

    if(m_context->getOptions()->get_enable_permission_checking()){
        string openMode = UNSPECIFIED_MODE;
        if(accMode == O_RDWR)
            openMode = RDWR_MODE;
        else if(accMode== O_RDONLY)
            openMode = READ_MODE;
        else if(accMode == O_WRONLY)
            openMode = WRITE_MODE;
        std::string status;
        if(VOK != (status =  m_context->getStorageMapper()->findLocation(string(path), openMode)))
            return translateError(status);
    }

    GET_LOCATION_INFO(path, needsForceClusterProxy(path));

    m_context->getStorageMapper()->openFile(string(path));

    SH_RUN(sInfo.storageHelperName, sInfo.storageHelperArgs, sh_open(lInfo.fileId.c_str(), fileInfo));

    if(sh_return == 0) {
        m_shCache.set(fileInfo->fh, ptr);

        time_t atime = 0, mtime = 0;

        if((accMode == O_WRONLY) || (fileInfo->flags & O_APPEND) || (accMode == O_RDWR))
            mtime = time(NULL);
#ifdef __APPLE__
        if( ( (accMode == O_RDONLY) || (accMode == O_RDWR) ) )
#else
        if( ( (accMode == O_RDONLY) || (accMode == O_RDWR) ) && !(fileInfo->flags & O_NOATIME) )
#endif
            atime = time(NULL);

        if(atime || mtime)
        {
            // Update access times in meta cache right away
            (void) m_metaCache->updateTimes(string(path), atime, mtime);

            m_context->getScheduler()->addTask(Job(time(NULL), shared_from_this(), TASK_ASYNC_UPDATE_TIMES, string(path), utils::toString(atime), utils::toString(mtime)));
        }
    }

    return sh_return;
}

int FsImpl::read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo)
{
    //LOG(INFO) << "FUSE: read(path: " << string(path) << ", size: " << size << ", offset: " << offset << ", ...)";
    GET_LOCATION_INFO(path, false);

    auto sh = m_shCache.get(fileInfo->fh);
    CUSTOM_SH_RUN(sh, sh_read(lInfo.fileId.c_str(), buf, size, offset, fileInfo));

    std::shared_ptr<events::Event> writeEvent = events::Event::createReadEvent(path, sh_return);
    m_eventCommunicator->processEvent(writeEvent);

    return sh_return;
}

int FsImpl::write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fileInfo)
{
    //LOG(INFO) << "FUSE: write(path: " << string(path) << ", size: " << size << ", offset: " << offset << ", ...)";

    if(!m_eventCommunicator->isWriteEnabled()){
        LOG(WARNING) << "Attempt to write when write disabled.";
        return -EDQUOT;
    }

    GET_LOCATION_INFO(path, false);

    auto sh = m_shCache.get(fileInfo->fh);
    CUSTOM_SH_RUN(sh, sh_write(lInfo.fileId.c_str(), buf, size, offset, fileInfo));

    if(sh_return > 0) { // Update file size in cache
        struct stat buf;
        if(!m_metaCache->getAttr(string(path), &buf))
            buf.st_size = 0;
        if(offset + sh_return > buf.st_size) {
            m_metaCache->updateSize(string(path), offset + sh_return);
        }

        std::shared_ptr<events::Event> writeEvent = events::Event::createWriteEvent(path, size);
        m_eventCommunicator->processEvent(writeEvent);
    }

    return sh_return;
}

// not yet implemented
int FsImpl::statfs(const char *path, struct statvfs *statInfo)
{
    LOG(INFO) << "FUSE: statfs(path: " << string(path) << ", ...)";

    pair<string, struct statvfs> resp = m_fslogic->getStatFS();
    RETURN_IF_ERROR(resp.first);

    memcpy(statInfo, &resp.second, sizeof(struct statvfs));
    return 0;
}

// not yet implemented
int FsImpl::flush(const char *path, struct fuse_file_info *fileInfo)
{
    LOG(INFO) << "FUSE: flush(path: " << string(path) << ", ...)";
    GET_LOCATION_INFO(path, false);

    auto sh = m_shCache.get(fileInfo->fh);
    CUSTOM_SH_RUN(sh, sh_flush(lInfo.fileId.c_str(), fileInfo));

    m_context->getScheduler()->addTask(Job(time(NULL) + 3, shared_from_this(), TASK_CLEAR_ATTR, string(path)));

    return sh_return;
}

int FsImpl::release(const char *path, struct fuse_file_info *fileInfo)
{
    LOG(INFO) << "FUSE: release(path: " << string(path) << ", ...)";

    GET_LOCATION_INFO(path, false);

    /// Remove Storage Helper's pointer from cache
    auto sh = m_shCache.take(fileInfo->fh);
    CUSTOM_SH_RUN(sh, sh_release(lInfo.fileId.c_str(), fileInfo));

    m_context->getStorageMapper()->releaseFile(string(path));

    return sh_return;
}

// not yet implemented
int FsImpl::fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    LOG(INFO) << "FUSE: fsync(path: " << string(path) << ", datasync: " << datasync << ")";
    /* Just a stub.  This method is optional and can safely be left
       unimplemented */

    (void) path;
    (void) datasync;
    (void) fi;
    return 0;
}

int FsImpl::opendir(const char *path, struct fuse_file_info *fileInfo)
{
    LOG(INFO) << "FUSE: opendir(path: " << string(path) << ", ...)";

    m_context->getScheduler()->addTask(Job(time(NULL), shared_from_this(), TASK_ASYNC_UPDATE_TIMES, string(path), utils::toString(time(NULL))));

    return 0;
}

int FsImpl::readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fileInfo)
{
    LOG(INFO) << "FUSE: readdir(path: " << string(path) << ", ..., offset: " << offset << ", ...)";
    vector<string> children;

    if(offset == 0) {
        children.push_back(".");
        children.push_back("..");
    }

    if(!m_fslogic->getFileChildren(path, DIR_BATCH_SIZE, offset >= 2 ? offset - 2 : 0, children))
    {
        return -EIO;
    }

    for(std::vector<string>::iterator it = children.begin(); it < children.end(); ++it)
    {
        if(m_context->getOptions()->get_enable_parallel_getattr() && m_context->getOptions()->get_enable_attr_cache()) {
            Job readDirTask = Job(time(NULL), shared_from_this(), ISchedulable::TASK_ASYNC_GETATTR, (boost::filesystem::path(path) / (*it)).normalize().string());
            m_context->getScheduler()->addTask(readDirTask);
        }

        if(filler(buf, it->c_str(), NULL, ++offset))
        {
            LOG(WARNING) << "filler buffer overflow";
            break;
        }
    }


    return 0;
}

int FsImpl::releasedir(const char *path, struct fuse_file_info *fileInfo)
{
    LOG(INFO) << "FUSE: releasedir(path: " << string(path) << ", ...)";
    return 0;
}

int FsImpl::fsyncdir(const char *path, int datasync, struct fuse_file_info *fileInfo)
{
    LOG(INFO) << "FUSE: fsyncdir(path: " << string(path) << ", datasync: " << datasync << ", ...)";
    return 0;
}

int FsImpl::setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
    return -EIO;
}

int FsImpl::getxattr(const char *path, const char *name, char *value, size_t size)
{
    return -EIO;
}

int FsImpl::listxattr(const char *path, char *list, size_t size)
{
    return -EIO;
}

int FsImpl::removexattr(const char *path, const char *name)
{
    return -EIO;
}

int FsImpl::init(struct fuse_conn_info *conn) {
    LOG(INFO) << "FUSE: init(...)";
    return 0;
}


bool FsImpl::needsForceClusterProxy(const std::string &path)
{
    struct stat attrs;
    auto attrsStatus = getattr(path.c_str(), &attrs, false);
    auto filePermissions = attrs->st_mode && (S_IRWXU || S_IRWXG || S_IRWXO);
    return attrsStatus || (filePermissions == 0) || !m_metaCache->canUseDefaultPermissions(attrs);
}

bool FsImpl::runTask(TaskID taskId, const string &arg0, const string &arg1, const string &arg2)
{
    struct stat attr;
    vector<string> children;
    time_t currentTime;
    std::shared_ptr<events::Event> truncateEvent;

    switch(taskId)
    {
    case TASK_ASYNC_READDIR: // arg0 = path, arg1 = offset
        if(!m_context->getOptions()->get_enable_attr_cache())
            return true;

        if(!m_fslogic->getFileChildren(arg0, DIR_BATCH_SIZE, utils::fromString<unsigned int>(arg1), children)) {
            return false;
        }

        for(vector<string>::iterator it = children.begin(); it < children.end(); ++it) {
            Job readDirTask = Job(time(NULL), shared_from_this(), ISchedulable::TASK_ASYNC_GETATTR, (boost::filesystem::path(arg0) / (*it)).normalize().string());
            m_context->getScheduler()->addTask(readDirTask);
        }

        if(children.size() > 0) {
            Job readDirTask = Job(time(NULL), shared_from_this(), ISchedulable::TASK_ASYNC_READDIR, arg0, utils::toString(utils::fromString<unsigned int>(arg1) + children.size()));
            m_context->getScheduler()->addTask(readDirTask);
        }

        return true;

    case TASK_CLEAR_ATTR:
        m_metaCache->clearAttr(arg0);
        return true;

    case TASK_ASYNC_GETATTR:
        if(m_context->getOptions()->get_enable_attr_cache())
            getattr(arg0.c_str(), &attr, false);
        return true;

    case TASK_ASYNC_UPDATE_TIMES: // arg0 = path, arg1 = atime, arg2 = mtime
        if(m_fslogic->updateTimes(arg0, utils::fromString<time_t>(arg1), utils::fromString<time_t>(arg2)) == VOK)
            m_metaCache->updateTimes(arg0, utils::fromString<time_t>(arg1), utils::fromString<time_t>(arg2));
        return true;

    case TASK_POST_TRUNCATE_ACTIONS: // arg0 = path, arg1 = newSize
        // we need to statAndUpdatetimes before processing event because we want event to be run with new size value on cluster
        currentTime = time(NULL);
        m_fslogic->updateTimes(arg0, 0, currentTime, currentTime);

        m_metaCache->clearAttr(arg0);
        if(m_context->getOptions()->get_enable_attr_cache())
            getattr(arg0.c_str(), &attr, false);

        truncateEvent = events::Event::createTruncateEvent(arg0, utils::fromString<off_t>(arg1));
        m_eventCommunicator->processEvent(truncateEvent);
        return true;

    default:
        return false;
    }
}

FsImpl::SHCache::value FsImpl::SHCache::get(const key id)
{
    boost::shared_lock_guard<boost::shared_mutex> guard{m_shCacheMutex};
    return m_shCache[id];
}

void FsImpl::SHCache::set(const key id, value sh)
{
    boost::lock_guard<boost::shared_mutex> guard{m_shCacheMutex};
    m_shCache[id] = std::move(sh);
}

FsImpl::SHCache::value FsImpl::SHCache::take(const key id)
{
    boost::lock_guard<boost::shared_mutex> guard{m_shCacheMutex};
    const auto it = m_shCache.find(id);
    auto sh = std::move(it->second);
    m_shCache.erase(it);
    return sh;
}

} // namespace client
} // namespace one
