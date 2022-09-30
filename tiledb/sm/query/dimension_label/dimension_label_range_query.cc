/**
 * @file dimension_label_range_query.cc
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
 * TODO: Docs
 */

#include "tiledb/sm/query/dimension_label/dimension_label_range_query.h"

namespace tiledb::sm {

/**
 * Creates a buffer to hold index range data.
 *
 * @param Datatype of the index data to create.
 * @param num_values The number of contained data points.
 * @returns A pointer to an index data object.
 */
tdb_unique_ptr<IndexData> create_index_data(
    const Datatype type, const uint64_t num_values) {
  switch (type) {
    case Datatype::INT8:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<int8_t>, num_values));
    case Datatype::UINT8:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<uint8_t>, num_values));
    case Datatype::INT16:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<int16_t>, num_values));
    case Datatype::UINT16:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<uint16_t>, num_values));
    case Datatype::INT32:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<int32_t>, num_values));
    case Datatype::UINT32:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<uint32_t>, num_values));
    case Datatype::INT64:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<int64_t>, num_values));
    case Datatype::UINT64:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<uint64_t>, num_values));
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return tdb_unique_ptr<IndexData>(
          tdb_new(TypedIndexData<int64_t>, num_values));
    default:
      stdx::unreachable();
  }
}

OrderedRangeQuery::OrderedRangeQuery(
    StorageManager* storage_manager,
    stats::Stats* stats,
    DimensionLabel* dimension_label,
    const std::vector<Range>& label_ranges)
    : DimensionLabelQuery{storage_manager,
                          stats,
                          dimension_label,
                          true,
                          false,
                          nullopt}
    , index_data_{create_index_data(
          dimension_label->index_dimension()->type(), label_ranges.size())} {
  throw_if_not_ok(indexed_array_query_->set_layout(Layout::ROW_MAJOR));
  indexed_array_query_->set_data_buffer(
      dimension_label->index_dimension->index_dimension()->name(),
      dimension_label->index_dimension->name(),
      index_data_->data(),
      index_data_->data_size(),
      true);
  // TODO: Finishes constructing the query.
}

tuple<bool, const void*, uint64_t> OrderedRangeQuery::computed_ranges() const {
  if (indexed_array_query_->status() != QueryStatus::COMPLETED) {
    return Status_DimensionLabelQueryError(
        "Cannot return computed ranges. Query has not completed.");
  }
  return {
    false, index_data_->data(), index_data_->num_values()
  }
}

}  // namespace tiledb::sm
