/**
 * @file localStorageManager.cc
 * @author Krzysztof Trzepla
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#include "localStorageManager.h"

#include "communication_protocol.pb.h"
#include "communication/communicator.h"
#include "communication/exception.h"
#include "config.h"
#include "context.h"
#include "fuse_messages.pb.h"
#include "logging.h"
#include "fsImpl.h"

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/tokenizer.hpp>
#include <google/protobuf/descriptor.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <random>

using boost::filesystem::path;
using namespace one::clproto::fuse_messages;
using namespace one::clproto::communication_protocol;

namespace one {
namespace client {

LocalStorageManager::LocalStorageManager(std::shared_ptr<Context> context)
    : m_context{std::move(context)}
    , m_messageBuilder{MessageBuilder{context}}
{
}

LocalStorageManager::~LocalStorageManager()
{
}

#ifdef __APPLE__

std::vector<path> LocalStorageManager::getMountPoints()
{
    std::vector<path> mountPoints;

    const int fs_num = getfsstat(NULL, 0, MNT_NOWAIT);
    if(fs_num < 0)
    {
        LOG(ERROR) << "Can not count mounted filesystems.";
        return mountPoints;
    }

    std::vector<struct statfs> stats(fs_num);

    const int stat_num = getfsstat(stats.data(), sizeof(struct statfs) * fs_num, MNT_NOWAIT);
    if(stat_num < 0)
    {
        LOG(ERROR) << "Can not get fsstat.";
        return mountPoints;
    }

    for(const auto &stat : stats)
    {
        std::string type(stat.f_fstypename);
        if(type.compare(0, 4, "fuse") != 0)
        {
            mountPoints.push_back(path(stat.f_mntonname).normalize());
        }
    }

    return mountPoints;
}

#else

std::vector<path> LocalStorageManager::getMountPoints()
{
    std::vector<path> mountPoints;

    FILE *file = setmntent(MOUNTS_INFO_FILE_PATH, "r");
    if(file == NULL)
    {
        LOG(ERROR) << "Can not parse /proc/mounts file.";
        return mountPoints;
    }

    struct mntent *ent;
    while ((ent = getmntent(file)) != NULL)
    {
        std::string type(ent->mnt_type);
        if(type.compare(0, 4, "fuse") != 0)
        {
            mountPoints.push_back(path(ent->mnt_dir).normalize());
        }
    }

    endmntent(file);

    return mountPoints;
}

#endif

std::vector< std::pair<int, std::string> > LocalStorageManager::parseStorageInfo(const path &mountPoint)
{
    std::vector< std::pair<int, std::string> > storageInfo;
    path storageInfoPath = (mountPoint / STORAGE_INFO_FILENAME).normalize();
    boost::system::error_code ec;

    if(boost::filesystem::exists(storageInfoPath, ec) && boost::filesystem::is_regular_file(storageInfoPath, ec))
    {
        boost::filesystem::ifstream storageInfoFile(storageInfoPath);
        std::string line;

        while(std::getline(storageInfoFile, line))
        {
            std::vector<std::string> tokens;
            boost::char_separator<char> sep(" ,{}");
            boost::tokenizer< boost::char_separator<char> > tokenizer(line, sep);

            for(const auto &token: tokenizer) tokens.push_back(token);

            // each line in file should by of form {storage id, relative path to storage against mount point}
            if(tokens.size() == 2)
            {
                try
                {
                    int storageId = boost::lexical_cast<int>(tokens[0]);
                    path absoluteStoragePath = mountPoint;

                    boost::algorithm::trim_left_if(tokens[1], boost::algorithm::is_any_of("./"));
                    if(!tokens[1].empty())
                    {
                        absoluteStoragePath /= tokens[1];
                    }
                    storageInfo.push_back(std::make_pair(storageId, absoluteStoragePath.string()));
                }
                catch(const boost::bad_lexical_cast &ex)
                {
                    LOG(ERROR) << "Wrong format of storage id in file: " << storageInfoPath << ", error: " << ex.what();
                }
            }
        }
    }

    return storageInfo;
}

std::vector< std::pair<int, std::string> > LocalStorageManager::getClientStorageInfo(const std::vector<path> &mountPoints)
{
    std::vector< std::pair<int, std::string> > clientStorageInfo;
    auto m_config = m_context->getConfig();

    for(const auto &mountPoint: mountPoints)
    {

        // Skip client mount point (just in case)
        if(mountPoint == m_config->getMountPoint()) continue;

        std::vector< std::pair<int, std::string> > storageInfo = parseStorageInfo(mountPoint);

        for(const auto &info : storageInfo)
        {
            const int storageId = info.first;
            const std::string absolutePath = info.second;
            std::string relativePath;
            std::string text;

            boost::optional< std::pair<std::string, std::string> > creationResult = createStorageTestFile(storageId);

            if(creationResult)
            {
                relativePath = creationResult->first;
                text = creationResult->second;
            }
            else
            {
                LOG(WARNING) << "Cannot create storage test file for storage with id: " << storageId;
                continue;
            }
            if(!hasClientStorageReadPermission(absolutePath, relativePath, text))
            {
                LOG(WARNING) << "Client does not have read permission for storage with id: " << storageId;
                continue;
            }
            if(!hasClientStorageWritePermission(storageId, absolutePath, relativePath))
            {
                LOG(WARNING) << "Client does not have write permission for storage with id: " << storageId;
                continue;
            }

            LOG(INFO) << "Storage with id: " << storageId << " is directly accessible to the client via: " << absolutePath;
            clientStorageInfo.push_back(std::make_pair(storageId, absolutePath));
        }
    }
    return clientStorageInfo;
}

bool LocalStorageManager::sendClientStorageInfo(const std::vector< std::pair<int, std::string> > &clientStorageInfo)
{
    ClusterMsg cMsg;
    ClientStorageInfo reqMsg;
    ClientStorageInfo::StorageInfo *storageInfo;
    Atom resMsg;

    auto communicator = m_context->getCommunicator();

    try
    {
        // Build ClientStorageInfo message
        for(const auto &info : clientStorageInfo)
        {
            storageInfo = reqMsg.add_storage_info();
            storageInfo->set_storage_id(info.first);
            storageInfo->set_absolute_path(info.second);
            LOG(INFO) << "Sending client storage info: {" << info.first << ", " << info.second << "}";
        }

        // Send ClientStorageInfo message
        auto fuseMsg = m_messageBuilder.createFuseMessage(reqMsg);
        auto ans = communicator->communicate<>(communication::ServerModule::FSLOGIC, fuseMsg, 2);

        // Check answer
        if(ans->answer_status() == VOK && resMsg.ParseFromString(ans->worker_answer()))
        {
            return resMsg.value() == "ok";
        }
        else if(ans->answer_status() == NO_USER_FOUND_ERROR)
        {
            LOG(ERROR) << "Cannot find user in database.";
        }
        else
        {
            LOG(ERROR) << "Cannot send client storage info.";
        }
    }
    catch(communication::Exception &e)
    {
        LOG(ERROR) << "Cannot select connection for storage test file creation: " << e.what();
    }

    return false;
}

boost::optional< std::pair<std::string, std::string> > LocalStorageManager::createStorageTestFile(const int storageId)
{
    ClusterMsg cMsg;
    CreateStorageTestFileRequest reqMsg;
    CreateStorageTestFileResponse resMsg;
    boost::optional< std::pair<std::string, std::string> > result;

    auto communicator = m_context->getCommunicator();

    try
    {
        reqMsg.set_storage_id(storageId);

        auto fuseMsg = m_messageBuilder.createFuseMessage(reqMsg);
        auto ans = communicator->communicate<CreateStorageTestFileResponse>(communication::ServerModule::FSLOGIC, fuseMsg, 2);

        if(ans->answer_status() == VOK && resMsg.ParseFromString(ans->worker_answer()))
        {
            result.reset({resMsg.relative_path(), resMsg.text()});
        }
        else if(ans->answer_status() == NO_USER_FOUND_ERROR)
        {
            LOG(ERROR) << "Cannot find user in database.";
        }
        else
        {
            LOG(ERROR) << "Cannot create test file for storage with id: " << storageId;
        }
    }
    catch(communication::Exception &e)
    {
        LOG(ERROR) << "Cannot select connection for storage test file creation: " << e.what();
    }

    return result;
}

bool LocalStorageManager::hasClientStorageReadPermission(const std::string &absolutePath, const std::string &relativePath, const std::string &expectedText)
{
    int fd = open((absolutePath + "/" + relativePath).c_str(), O_RDONLY | O_FSYNC);
    if(fd == -1)
    {
        return false;
    }
    fsync(fd);
    std::string actualText(expectedText.size(), 0);
    if(read(fd, &actualText[0], expectedText.size()) != (int) expectedText.size())
    {
        close(fd);
        return false;
    }
    close(fd);
    return expectedText == actualText;
}

bool LocalStorageManager::hasClientStorageWritePermission(const int storageId, const std::string &absolutePath, const std::string &relativePath)
{
    int fd = open((absolutePath + "/" + relativePath).c_str(), O_WRONLY | O_FSYNC);
    if(fd == -1)
    {
        return false;
    }

    const int length = 20;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<char> dis('0', 'z');
    std::string text;
    std::generate_n(std::back_inserter(text), length, [&]{ return dis(gen); });

    if(write(fd, text.c_str(), length) != length)
    {
        close(fd);
        return false;
    }
    close(fd);

    ClusterMsg cMsg;
    StorageTestFileModifiedRequest reqMsg;
    StorageTestFileModifiedResponse resMsg;
    Answer ans;

    auto communicator = m_context->getCommunicator();

    try
    {
        reqMsg.set_storage_id(storageId);
        reqMsg.set_relative_path(relativePath);
        reqMsg.set_text(text);

        auto fuseMsg = m_messageBuilder.createFuseMessage(reqMsg);
        auto ans = communicator->communicate<StorageTestFileModifiedResponse>(communication::ServerModule::FSLOGIC, fuseMsg, 2);

        if(ans->answer_status() == VOK && resMsg.ParseFromString(ans->worker_answer()))
        {
            return resMsg.answer();
        }
        else if(ans->answer_status() == NO_USER_FOUND_ERROR)
        {
            LOG(ERROR) << "Cannot find user in database.";
        }
        else
        {
            LOG(ERROR) << "Cannot check client write permission for storage with id: " << storageId;
        }
    }
    catch(communication::Exception &e)
    {
        LOG(ERROR) << "Cannot select connection for storage test file creation: " << e.what();
    }

    return false;
}

} // namespace client
} // namespace one
