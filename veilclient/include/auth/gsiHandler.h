/**
 * @file gsiHandler.hh
 * @author Rafal Slota
 * @copyright (C) 2013 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef ONECLIENT_GSI_HANDLER_H
#define ONECLIENT_GSI_HANDLER_H


#include "communication/certificateData.h"

#include <openssl/ossl_typ.h>

#include <memory>
#include <mutex>
#include <string>

namespace boost { namespace filesystem { class path; } }

namespace one
{

inline std::string CONFIRM_CERTIFICATE_PROMPT(const std::string &USERNAME)
{
    return "Warning ! You are trying to connect using unconfirmed certificate "
           "as: '" + USERNAME + "'. Is it your account? (y/n): ";
}

namespace client
{

class Context;

namespace auth
{

class GSIHandler
{
public:
    GSIHandler(std::weak_ptr<Context> context, const bool debug = false);

    void validateProxyConfig();
    void validateProxyCert();
    std::string getClusterHostname(const std::string &baseDomain);
    std::shared_ptr<communication::CertificateData> getCertData();

private:
    std::pair<std::string, std::string> findUserCertAndKey(const boost::filesystem::path &dir);
    std::pair<std::string, std::string> findUserCertAndKey();
    std::pair<std::string, std::string> getUserCertAndKey();
    const std::vector<std::pair<std::string, std::string>> &getCertSearchPath();

    const std::weak_ptr<Context> m_context;
    const bool m_debug;
    std::mutex m_certCallbackMutex;
    std::string m_userDN;
    bool m_proxyInitialized = false;
    BUF_MEM *m_keyBuff = nullptr;
    BUF_MEM *m_chainBuff = nullptr;
    std::string m_userCertPath;
    std::string m_userKeyPath;
};

} // namespace auth
} // namespace client
} // namespace one


#endif // ONECLIENT_GSI_HANDLER_H
