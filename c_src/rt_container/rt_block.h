/**
 * @file rt_block.h
 * @author Krzysztof Trzepla
 * @copyright (C): 2014 ACK CYFRONET AGH
 * This software is released under the MIT license
 * cited in 'LICENSE.txt'.
 */

#ifndef RT_BLOCK_H
#define RT_BLOCK_H

#include "nifpp.h"
#include "rt_term.h"

#include <list>
#include <string>

namespace one {
namespace provider {

/**
 * The rt_block class.
 * rt_block object represents single block pushed on RTransfer container
 */
class rt_block {
public:
    /**
     * rt_block constructor.
     * Constructs RTransfer block.
     * @param file_id ID of file this block is a part of
     * @param offset block offset
     * @param size block size
     * @param priority block priority
     * @param terms list of Erlang terms associated with block
     * @param provider_ref reference to provider that poses block
     * @param counter defines how many times block was pushed on the
     * rt_container
     */
    rt_block(std::string file_id = "", rt_term provider_ref = rt_term(),
             ErlNifUInt64 offset = 0, ErlNifUInt64 size = 0,
             ErlNifUInt64 priority = 0,
             std::list<rt_term> terms = std::list<rt_term>(),
             ErlNifUInt64 counter = 1)
        : file_id_{std::move(file_id)}
        , provider_ref_{std::move(provider_ref)}
        , offset_{offset}
        , size_{size}
        , priority_{priority}
        , terms_{std::move(terms)}
        , counter_{counter}
    {
    }

    /// Getter for block's file ID
    const std::string &file_id() const { return file_id_; }

    /// Getter for provider ID
    const rt_term &provider_ref() const { return provider_ref_; }

    /// Getter for block's offset
    ErlNifUInt64 offset() const { return offset_; }

    /// Getter for block's size
    ErlNifUInt64 size() const { return size_; }

    /// Getter for block's end
    ErlNifUInt64 end() const { return offset_ + size_ - 1; }

    /// Getter for block's priority
    ErlNifUInt64 priority() const { return priority_; }

    /// Getter for block's terms
    const std::list<rt_term> &terms() const { return terms_; }

    /// Getter for block's addition counter
    ErlNifUInt64 counter() const { return counter_; }

    /**
     * Appends list of terms to block
     * @param list of terms to be appended to the list of block's terms
     */
    void appendTerms(const std::list<rt_term> &terms);

    /**
     * Checks whether this block can be merge with other block. That is
     * both belong to the same file and are successive.
     * @param block to be merged
     * @return true if blocks can be merged
     */
    bool is_mergeable(const rt_block &block);

    /**
     * Checks whether this block can be merge with other block. That is
     * both belong to the same file, are successive and summary size is
     * less than maximal RTransfer block size.
     * @param block to be merged
     * @param block_size maximal RTransfer block size
     * @return true if blocks can be merged
     */
    bool is_mergeable(const rt_block &block, ErlNifUInt64 block_size);

    /**
     * Compares this block with other block. Order of comparison criteria:
     * 1) priority - block with higher priority comes first
     * 2) counter - block with higher addition count comes first
     * 3) file_id - block with lexicographically less file ID comes first
     * 4) offset - block with smaller offset comes first
     * 5) size - block with smaller size comes first
     * @param block to be compared with
     * @return true if this block comes before the other, otherwise false
     */
    bool operator<(const rt_block &block) const;

    /**
     * Modifies this block by merging other block
     * @param block to be merged
     * @return merged block
     */
    rt_block &operator+=(const rt_block &block);

private:
    std::string file_id_;
    rt_term provider_ref_;
    ErlNifUInt64 offset_;
    ErlNifUInt64 size_;
    ErlNifUInt64 priority_;
    std::list<rt_term> terms_;
    ErlNifUInt64 counter_;
};

/**
 * Compares two RTransfer blocks.
 * Blocks are equal if corresponding block fields are equal.
 * @param block to be compared
 * @param block to be compared
 * @return true if blocks are equal
 */
bool operator==(const rt_block &lhs, const rt_block &rhs);

} // namespace provider
} // namespace one

#endif // RT_BLOCK_H