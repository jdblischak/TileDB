/**
 * @file dimension_label_queries.h
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
 * TODO: Add description
 */

#ifndef TILEDB_DIMENSION_LABELS_QUERIES_H
#define TILEDB_DIMENSION_LABELS_QUERIES_H

#include "tiledb/common/common.h"

using namespace tiledb::common;

namespace tiledb::sm {

class DimensionLabel;
class Query;
class StorageManager;

/**
 * Return a Status_DimensionQueryError error class Status with a given
 * message.
 *
 * Note: Returns error as a QueryError.
 ***/
inline Status Status_DimensionQueryError(const std::string& msg) {
  return {"[TileDB::Query] Error", msg};
}

class DimensionLabelQueries {
 public:
  /** Default constructor is not C.41 compliant. */
  DimensionLabelQueries() = delete;

  /** Constructor. */
  DimensionLabelQueries(StorageManager* storage_manager);

  void add_dimension_labels(
      const Subarray& subarray, const std::vector<QueryBuffer>& label_buffers);

  void process_range_queries();
  void process_data_queries();
  void cancel();
  void finalize();

 private:
  /** TODO */
  StorageManager* storage_manager_;

  /** TODO */
  std::vector<unique_ptr<DimensionLabelRangeQuery>> ordered_range_queries_;

  /** TODO */
  std::vector<unique_ptr<Query>> unordered_range_queries_;

  /** TODO */
  std::vector<unique_ptr<Query>> data_queries_;

  /** TODO */
  void add_ordered_query(
      DimensionLabel* dimension_label,
      StorageManager* storage_manager,
      const RangeSetAndSuperset& label_ranges,
      const RangeSetAndSuperset& index_ranges,
      const QueryBuffer& label_data_buffer);

  /** TODO */
  void add_unordered_query(
      DimensionLabel* dimension_label,
      StorageManager* storage_manager,
      const RangeSetAndSuperset& label_ranges,
      const RangeSetAndSuperset& index_ranges,
      const QueryBuffer& label_data_buffer);
};

}  // namespace tiledb::sm

#endif
