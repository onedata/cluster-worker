/**
 * @file rt_exception.h
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_EXCEPTION_H
#define RT_EXCEPTION_H

#include <string>
#include <stdexcept>

namespace one {
namespace provider {

/**
 * The rt_exception class.
 * rt_exception object represents RTransfer container exception
 */
class rt_exception : public std::runtime_error {
public:
    /**
     * @copydoc std::runtime_error
     */
    rt_exception(const std::string &message)
        : std::runtime_error(message){};
};

} // namespace provider
} // namespace one

#endif // RT_EXCEPTION_H