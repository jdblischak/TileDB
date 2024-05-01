/**
 * @file   alt_alt_var_length_view.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file contains the definition of the alt_var_length_view class, which
 * splits a given view into subranges of variable length, as delimited by
 * adjacent pairs of values in an index range.
 *
 * The difference between `alt_var_length_view` and `var_length_view` is that
 * `alt_var_length_view` maintains a materialized range of subranges, whereas
 * `var_length_view` creates subrange views on the fly as proxy objects.  As a
 * result
 * * An `alt_var_length_view` does not need to refer to the offsets array after
 * it is constructed
 * * An `alt_var_length_view` can be sorted
 *
 *
 * Usage example:
 * ```
 * std::vector<int> x {1, 2, 3, 4, 5, 6, 7, 8, 9}; // Data range
 * std::vector<size_t> indices {0, 4, 7, 9};       // Index range
 * alt_var_length_view v(x, indices);
 * CHECK(std::ranges::equal(v[0], std::vector<int>{1, 2, 3, 4}));
 * CHECK(std::ranges::equal(v[1], std::vector<int>{5, 6, 7}));
 * CHECK(std::ranges::equal(v[2], std::vector<int>{8, 9}));
 * ```
 */

#ifndef TILEDB_ALT_VAR_LENGTH_VIEW_H
#define TILEDB_ALT_VAR_LENGTH_VIEW_H

#include <ranges>

#include "iterator_facade.h"

/**
 * A view that splits a view into subranges of variable length, as delimited by
 * a range of indices. The resulting view is a range of subranges, each of which
 * is a view into the original data range. The iterator over the
 * alt_var_length_view is a random_access_iterator that dereferences to a
 * subrange of the data range.
 *
 * @tparam R Type of the data range, assumed to be a random access range.
 * @tparam I Type of the index range, assumed to be a random access range.
 *
 * @todo R could be a view rather than a range.
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range I>
class alt_var_length_view : public std::ranges::view_base {
  // Forward reference of the iterator over the range of variable length data
  template <class Value>
  struct private_iterator;

  /** The type of the iterator over the var length data range */
  using data_iterator_type = std::ranges::iterator_t<R>;

  /** The type that can index into the var length data range */
  using data_index_type = std::iter_difference_t<data_iterator_type>;

  /** The type dereferenced by the iterator is a subrange */
  using var_length_type = std::ranges::subrange<data_iterator_type>;

  /** The type of the iterator over the var length view */
  using var_length_iterator = private_iterator<var_length_type>;

  /** The type of the const iterator over the var length view */
  using var_length_const_iterator = private_iterator<var_length_type const>;

 public:
  /*****************************************************************************
   * Constructors
   * @note: Since this view has ownership of the subranges, we don't allow
   * copying, but do allow moves.
   ****************************************************************************/

  /** Default constructor */
  alt_var_length_view() = default;

  /** Copy constructor is deleted */
  alt_var_length_view(const alt_var_length_view&) = delete;

  /** Move constructor */
  alt_var_length_view(alt_var_length_view&&) = default;

  /** Copy assignment is deleted */
  alt_var_length_view& operator=(const alt_var_length_view&) = delete;

  /** Move assignment */
  alt_var_length_view& operator=(alt_var_length_view&&) = default;

  /** Primary constructor. All offsets are contained in the input (notably, the
   * index to the end of the data range). */
  alt_var_length_view(R& data, const I& index) {
    auto data_begin(std::ranges::begin(data));

    subranges_.reserve(std::ranges::size(index) - 1);
    for (size_t i = 0; i < std::ranges::size(index) - 1; ++i) {
      subranges_.emplace_back(data_begin + index[i], data_begin + index[i + 1]);
    }
  }

  /** Constructor. The offsets do not contain the final index value (which would
   * be the end of the data range), so the final index is passed in as a
   * separate argument.
   */
  alt_var_length_view(R& data, const I& index, data_index_type end_index) {
    auto data_begin(std::ranges::begin(data));

    subranges_.reserve(std::ranges::size(index) - 1);

    for (size_t i = 0; i < std::ranges::size(index) - 1; ++i) {
      subranges_.emplace_back(data_begin + index[i], data_begin + index[i + 1]);
    }
    subranges_.emplace_back(data_begin + index.back(), data_begin + end_index);
  }

  /** Return iterator to the beginning of the var length view */
  auto begin() {
    return subranges_.begin();
  }

  /** Return iterator to the end of the var length view */
  auto end() {
    return subranges_.end();
  }

  /** Return const iterator to the beginning of the var length view */
  auto begin() const {
    return subranges_.cbegin();
  }

  /** Return const iterator to the end of the var length view */
  auto end() const {
    return subranges_.cend();
  }

  /** Return const iterator to the beginning of the var length view */
  auto cbegin() const {
    return subranges_.cbegin();
  }

  /** Return const iterator to the end of the var length view */
  auto cend() const {
    return subranges_.cend();
  }

  /** Return the number of subranges in the var length view */
  auto size() const {
    return std::ranges::size(subranges_);
  }

 private:
  std::vector<var_length_type> subranges_;
};

#endif  // TILEDB_ALT_VAR_LENGTH_VIEW_H