/**
 * @file storageMapper_mock.h
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef STORAGE_MAPPER_MOCK_H
#define STORAGE_MAPPER_MOCK_H


#include "storageMapper.h"

#include <gmock/gmock.h>

class MockStorageMapper: public one::client::StorageMapper
{
public:
    MockStorageMapper(std::weak_ptr<one::client::Context> context,
                      std::shared_ptr<one::client::FslogicProxy> fslogicProxy)
        : StorageMapper(std::move(context), fslogicProxy)
    {
    }

    MOCK_METHOD1(releaseFile, void(const std::string&));
    MOCK_METHOD3(getLocationInfo, std::pair<one::client::locationInfo, one::client::storageInfo>(const std::string&, bool, bool));
    MOCK_METHOD2(addLocation, void(const std::string&, const one::clproto::fuse_messages::FileLocation&));
    MOCK_METHOD3(findLocation, std::string(const std::string&, const std::string&, bool));
    MOCK_METHOD2(helperOverride, void(const std::string&, const one::client::storageInfo&));
    MOCK_METHOD1(resetHelperOverride, void(const std::string&));
};


#endif // STORAGE_MAPPER_MOCK_H
