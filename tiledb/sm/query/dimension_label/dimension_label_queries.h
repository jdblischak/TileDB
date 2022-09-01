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

#include <unordered_map>
#include "tiledb/common/common.h"
#include "tiledb/sm/query/dimension_label/dimension_label_data_query.h"
#include "tiledb/sm/query/dimension_label/dimension_label_range_query.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Array;
class DimensionLabel;
class DimensionLabelDataQuery;
class DimensionLabelRangeQuery;
class DimensionLabelReference;
class Query;
class QueryBuffer;
class StorageManager;
class Subarray;

enum class QueryType : uint8_t;

/**
 * Return a Status_DimensionQueryError error class Status with a given
 * message.
 *
 * Note: Returns error as a QueryError.
 ***/
inline Status Status_DimensionLabelQueryError(const std::string& msg) {
  return {"[TileDB::Query] Error", msg};
}

class DimensionLabelQueries {
 public:
  /**
   * Size type for the number of dimensions of an array and for dimension
   * indices.
   *
   * Note: This should be the same as `Domain::dimension_size_type`. We're
   * not including `domain.h`, otherwise we'd use that definition here.
   */
  using dimension_size_type = unsigned int;

  /** TODO */
  using range_size_type = uint64_t;

  /** Default constructor is not C.41 compliant. */
  DimensionLabelQueries() = delete;

  /** Constructor. */
  DimensionLabelQueries(
      StorageManager* storage_manager,
      Array* array,
      const Subarray& subarray,
      const std::unordered_map<std::string, QueryBuffer>& label_buffers,
      const std::unordered_map<std::string, QueryBuffer>& array_buffers,
      optional<std::string> fragment_name);

  /** Disable copy and move. */
  DISABLE_COPY_AND_COPY_ASSIGN(DimensionLabelQueries);
  DISABLE_MOVE_AND_MOVE_ASSIGN(DimensionLabelQueries);

  void cancel();
  void finalize();

  inline bool has_label_ranges(dimension_size_type dim_idx) const {
    return range_queries_[dim_idx] != nullptr;
  }

  /**
   * TODO: finish docs.
   *
   * @return [is_point_range, start, count]
   */
  inline tuple<bool, const void*, range_size_type> index_ranges(
      dimension_size_type dim_idx) const {
    return range_queries_[dim_idx]->index_ranges();
  }

  /** TODO: docs */
  void process_data_queries();

  /** TODO: docs */
  void process_range_queries();

  /** TODO: docs */
  inline QueryStatus status_range_query(dimension_size_type dim_idx) const {
    if (range_queries_[dim_idx]) {
      return range_queries_[dim_idx]->status();
    }
    throw StatusException(Status_DimensionLabelQueryError(
        "No range query set on dimension " + std::to_string(dim_idx)));
  }

 private:
  /** TODO: docs */
  std::unordered_map<std::string, tdb_unique_ptr<DimensionLabel>>
      dimension_labels_;

  /** TODO: docs */
  std::unordered_map<std::string, tdb_unique_ptr<DimensionLabelRangeQuery>>
      range_queries_map_;

  /** TODO: docs */
  std::vector<DimensionLabelRangeQuery*> range_queries_;

  /** TODO: docs */
  std::unordered_map<std::string, tdb_unique_ptr<DimensionLabelDataQuery>>
      data_queries_;

  /** TODO: docs */
  void add_data_queries_for_read(
      StorageManager* storage_manager,
      Array* array,
      const Subarray& subarray,
      const std::unordered_map<std::string, QueryBuffer>& label_buffers);

  /** TODO: docs */
  void add_data_queries_for_write(
      StorageManager* storage_manager,
      Array* array,
      const Subarray& subarray,
      const std::unordered_map<std::string, QueryBuffer>& label_buffers,
      const std::unordered_map<std::string, QueryBuffer>& array_buffers,
      const optional<std::string> fragment_name);

  /** TODO: docs */
  void add_range_queries(
      StorageManager* storage_manager,
      Array* array,
      const Subarray& subarray,
      const std::unordered_map<std::string, QueryBuffer>& label_buffers,
      const std::unordered_map<std::string, QueryBuffer>& array_buffers);

  /** TODO: docs */
  DimensionLabel* open_dimension_label(
      StorageManager* storage_manager,
      Array* array,
      const DimensionLabelReference& dim_label_ref,
      const QueryType& query_type,
      const bool open_indexed_array,
      const bool open_labelled_array);
};

}  // namespace tiledb::sm

#endif
