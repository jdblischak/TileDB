/**
 * @file tiledb/sm/dimension_label/dimension_label_range_query.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Class for reading the index range from a label range on a dimension label.
 */

#ifndef TILEDB_DIMENSION_LABEL_RANGE_QUERY_H
#define TILEDB_DIMENSION_LABEL_RANGE_QUERY_H

#include <string>
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/query/query.h"

namespace tiledb::sm {

class Array;
class DimensionLabel;
class Query;
class RangeSetAndSuperset;

/** Return a Status_RangeQueryError error class Status with a given
 * message. Note: currently set to return Query error. **/
inline Status Status_RangeQueryError(const std::string& msg) {
  return {"[TileDB::Query] Error", msg};
};

/**
 * This class should be considered deprecated at creation. This class
 * should be replaced with a range strategy that can handle multiple ranges
 * inside a single query.
 *
 * The range query assumes that the index values for the dimension label index
 * are consecutive values that are increasing or decreasing and that there is
 * no gaps in the label.
 *
 */
class DimensionLabelRangeQuery {
 public:
  DimensionLabelRangeQuery() = delete;

  DimensionLabelRangeQuery(
      DimensionLabel* dimension_label,
      StorageManager* storage_manager,
      const RangeSetAndSuperset& label_ranges);

  /** Disable copy and move. */
  DISABLE_COPY_AND_COPY_ASSIGN(DimensionLabelRangeQuery);
  DISABLE_MOVE_AND_MOVE_ASSIGN(DimensionLabelRangeQuery);

  /** Cancel the query. */
  void cancel();

  /**
   * Finalize the query and update the computed index range if it contains an
   * extra value.
   */
  void finalize();

  /** Returns the index range computed by the range query. */
  inline const Range& index_range() const {
    return computed_index_range_;
  }

  /** Returns the status of the query. */
  inline QueryStatus status() const {
    return status_;
  }

  /** Submits the query. */
  void submit();

 private:
  LabelOrder order_;
  Range input_label_range_;
  Range computed_label_range_;
  Range computed_index_range_;
  Query lower_bound_query_;
  uint64_t lower_bound_index_buffer_size_;
  uint64_t lower_bound_label_buffer_size_;
  Query upper_bound_query_;
  uint64_t upper_bound_index_buffer_size_;
  uint64_t upper_bound_label_buffer_size_;
  std::function<bool(const Range& range1, const Range& range2)>
      has_extra_range_element_;
  std::function<void(Range& range)> fix_index_range_;
  QueryStatus status_{QueryStatus::UNINITIALIZED};
};

}  // namespace tiledb::sm

#endif
