/**
 * @file testCommon.h
 * @author Rafal Slota
 * @author Konrad Zemek
 * @copyright (C) 2013-2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef TEST_COMMON_H
#define TEST_COMMON_H


#include <boost/system/error_code.hpp>
#include <gmock/gmock.h>

#include <memory>

namespace one
{
    class Scheduler;

    namespace testing
    {
        class FsImplMount;
    }
    namespace client
    {
        class Config;
        class Context;
        class FsImpl;
        class FslogicProxy;
        class Options;
        class StorageMapper;
    }
}

struct MockOptions;
class MockJobScheduler;
class MockCommunicator;
class MockStorageMapper;
class MockFslogicProxy;

class CommonTest: public ::testing::Test
{
public:
    std::shared_ptr<one::client::Context> context;
    std::shared_ptr<one::client::Config> config;
    std::shared_ptr<MockOptions> options;
    std::shared_ptr<MockJobScheduler> jobScheduler;
    std::shared_ptr<MockCommunicator> communicator;
    std::shared_ptr<MockStorageMapper> storageMapper;
    std::shared_ptr<MockFslogicProxy> fslogic;
    std::shared_ptr<one::Scheduler> scheduler;

protected:
    virtual void SetUp() override;
};

class CommonIntegrationTest: public ::testing::Test
{
public:
    boost::system::error_code ec;
    std::shared_ptr<one::client::Context> context;
    std::shared_ptr<one::client::FsImpl> onedata;
    std::shared_ptr<one::client::FslogicProxy> fslogic;
    std::shared_ptr<one::client::Config> config;
    std::shared_ptr<one::client::Options> options;
    std::unique_ptr<one::testing::FsImplMount> onedataMount;
    std::shared_ptr<one::client::StorageMapper> storageMapper;

protected:
    CommonIntegrationTest(std::unique_ptr<one::testing::FsImplMount> onedataMount);

    virtual void SetUp() override;
};


#endif // TEST_COMMON_H
