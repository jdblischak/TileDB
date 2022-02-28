/**
 * @file tiledb/sm/dimension_label/dimension_label_query.cc
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
 */

#include "tiledb/sm/dimension_label/unordered_labels_write_query.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/storage_manager/storage_manager.h"

namespace tiledb::sm {

UnorderedLabelsWriteQuery::UnorderedLabelsWriteQuery(
    shared_ptr<DimensionLabel> dimension_label,
    StorageManager* storage_manager,
    const RangeSetAndSuperset& label_ranges,
    const RangeSetAndSuperset& index_ranges,
    const QueryBuffer& label_data_buffer,
    const QueryBuffer& index_data_buffer)
    : labelled_array_query_{tdb_unique_ptr<Query>(
          tdb_new(Query, storage_manager, dimension_label->labelled_array()))}
    , indexed_array_query_{tdb_unique_ptr<Query>(
          tdb_new(Query, storage_manager, dimension_label->indexed_array()))} {
  // Check query type is as expected.
  if (dimension_label->query_type() != QueryType::WRITE)
    throw std::invalid_argument(
        "Failed to create dimension label query. Cannot write to a dimension "
        "label opened with query type " +
        query_type_str(dimension_label->query_type()) + ".");
  // Check write is for the entire dimension label.
  if (!label_ranges.is_empty() || index_ranges.is_explicitly_initialized())
    throw StatusException(Status_DimensionLabelQueryError(
        "Failed to create dimension label query. Currently dimension labels "
        "only support writing the full array."));
  if (label_ranges.is_empty() && index_ranges.is_empty())
    throw StatusException(Status_DimensionLabelQueryError("No query set."));
  // Verify only one dimension label is set.
  if (!dimension_label->labelled_array()->is_empty() ||
      !dimension_label->indexed_array()->is_empty())
    throw Status_DimensionLabelQueryError(
        "Cannot write to dimension label. Currently ordered dimension labels "
        "can only be written to once.");
  // TODO: set fragment URI to be the same for both writes
  // Set-up labelled array query (sparse array)
  throw_if_not_ok(labelled_array_query_->set_layout(Layout::UNORDERED));
  throw_if_not_ok(labelled_array_query_->set_data_buffer(
      dimension_label->label_dimension()->name(),
      label_data_buffer.buffer_,
      label_data_buffer.buffer_size_,
      true));
  throw_if_not_ok(labelled_array_query_->set_data_buffer(
      dimension_label->index_attribute()->name(),
      index_data_buffer.buffer_,
      index_data_buffer.buffer_size_,
      true));
  // Set-up indexed array query (dense array)
  throw_if_not_ok(indexed_array_query_->set_layout(Layout::UNORDERED));
  throw_if_not_ok(indexed_array_query_->set_data_buffer(
      dimension_label->label_attribute()->name(),
      label_data_buffer.buffer_,
      label_data_buffer.buffer_size_,
      true));
  throw_if_not_ok(indexed_array_query_->set_data_buffer(
      dimension_label->index_dimension()->name(),
      index_data_buffer.buffer_,
      index_data_buffer.buffer_size_,
      true));
}

void UnorderedLabelsWriteQuery::cancel() {
  throw_if_not_ok(indexed_array_query_->cancel());
  throw_if_not_ok(labelled_array_query_->cancel());
}

void UnorderedLabelsWriteQuery::finalize() {
  throw_if_not_ok(indexed_array_query_->finalize());
  throw_if_not_ok(labelled_array_query_->finalize());
}

QueryStatus UnorderedLabelsWriteQuery::status() const {
  // Check both queries exist, and return appropriate status otherwise.
  if (!labelled_array_query_ && !indexed_array_query_)
    return QueryStatus::UNINITIALIZED;
  if (!labelled_array_query_ && indexed_array_query_)
    // Bad state - both arrays should be initialized
    throw std::runtime_error(
        "Dimension label query failed to fully initialize.");
  if (!indexed_array_query_ && labelled_array_query_) {
    // Bad state - both arrays should be initialized
    return QueryStatus::FAILED;
  }
  auto labelled_status = labelled_array_query_->status();
  auto indexed_status = indexed_array_query_->status();
  // Return status if both queries have the same status.
  if (labelled_status == indexed_status)
    return labelled_status;
  // Resolve mixed status
  //  - If only one query is uninitialized, then in a bad state.
  //  - If either failed, then the query is failed.
  //  - If neither failed and either is incomplete, then the query is
  //  incomplete.
  //  - If neither are failed, incomplete, or uninitialized, then one is
  //  complete and one is in progress: the overall status is in progress.
  if (labelled_status == QueryStatus::UNINITIALIZED ||
      indexed_status == QueryStatus::UNINITIALIZED)
    throw std::runtime_error(
        "Dimension label query failed to fully initialize.");
  if (labelled_status == QueryStatus::FAILED ||
      indexed_status == QueryStatus::FAILED)
    return QueryStatus::FAILED;
  if (labelled_status == QueryStatus::INCOMPLETE ||
      indexed_status == QueryStatus::INCOMPLETE)
    return QueryStatus::INCOMPLETE;
  return QueryStatus::INPROGRESS;
}

void UnorderedLabelsWriteQuery::submit() {
  if (labelled_array_query_)
    throw_if_not_ok(labelled_array_query_->submit());
  if (indexed_array_query_)
    throw_if_not_ok(indexed_array_query_->submit());
}

}  // namespace tiledb::sm
