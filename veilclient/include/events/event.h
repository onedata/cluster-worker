/**
 * Class Event is responsible for storing data related to event.
 * @file event.h
 * @author Michal Sitko
 * @copyright (C) 2014 ACK CYFRONET AGH
 * @copyright This software is released under the MIT license cited in 'LICENSE.txt'
 */

#ifndef ONECLIENT_EVENT_H
#define ONECLIENT_EVENT_H


#include <map>
#include <memory>
#include <string>

namespace one
{

namespace clproto{ namespace fuse_messages { class EventMessage; } }

namespace client
{
namespace events
{

using NumericProperty = long long;

/**
 * Class Event is key-value container for events.
 * It can store numerical and string values.
 * TODO: consider making it immutable.
 */
class Event
{
public:
    Event() = default;
    Event(const Event & anotherEvent);
    virtual ~Event() = default;

    virtual std::shared_ptr< ::one::clproto::fuse_messages::EventMessage> createProtoMessage(); ///< Creates protocol buffer message representing Event.

    /* Access methods for m_numericProperties */
    NumericProperty getNumericProperty(const std::string & key, const NumericProperty defaultValue) const; ///< Returns numericProperty for key. If cannot be found defaultValue is returned instead.
    void setNumericProperty(const std::string & key, NumericProperty value);
    int getNumericPropertiesSize() const;

    /* Access methods for m_stringProperties */
    std::string getStringProperty(const std::string & key, const std::string & defaultValue) const; ///< Returns stringProperty for key. If cannot be found defaultValue is returned instead.
    void setStringProperty(const std::string & key, const std::string & value);
    int getStringPropertiesSize() const;

    /* Factory methods */
    static std::shared_ptr<Event> createMkdirEvent(const std::string & filePath);
    static std::shared_ptr<Event> createWriteEvent(const std::string & filePath, NumericProperty bytes);
    static std::shared_ptr<Event> createReadEvent(const std::string & filePath, NumericProperty bytes);
    static std::shared_ptr<Event> createRmEvent(const std::string & filePath);
    static std::shared_ptr<Event> createTruncateEvent(const std::string & filePath, off_t newSize);

private:
    std::map<std::string, NumericProperty> m_numericProperties; ///< Stores numeric properties
    std::map<std::string, std::string> m_stringProperties; ///< Stores string properties
};

} // namespace events
} // namespace client
} // namespace one

#endif // ONECLIENT_EVENT_H
