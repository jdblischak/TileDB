/**
 * @file dimension_label_data_query.cc
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
 */

#include "tiledb/sm/query/dimension_label/dimension_label_data_query.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/subarray/subarray.h"

using namespace tiledb::common;

namespace tiledb::sm {

inline Status Status_DimensionLabelQueryError(const std::string& msg) {
  return {"[TileDB::Query] Error", msg};
}

DimensionLabelDataQuery::DimensionLabelDataQuery(
    StorageManager* storage_manager,
    DimensionLabel* dimension_label,
    bool add_indexed_query,
    bool add_labelled_query,
    optional<std::string> fragment_name)
    : indexed_array_query{
          add_indexed_query ?
              tdb_unique_ptr<Query>(tdb_new(
                  Query, storage_manager, dimension_label->indexed_array(), fragment_name)) :
              nullptr} 
    , labelled_array_query{
          add_labelled_query ?
              tdb_unique_ptr<Query>(tdb_new(
                  Query, storage_manager, dimension_label->labelled_array(), fragment_name)) :
              nullptr} {
}

void DimensionLabelDataQuery::cancel() {
  if (indexed_array_query) {
    throw_if_not_ok(indexed_array_query->cancel());
  }
  if (labelled_array_query) {
    throw_if_not_ok(labelled_array_query->cancel());
  }
}

void DimensionLabelDataQuery::finalize() {
  if (indexed_array_query) {
    throw_if_not_ok(indexed_array_query->finalize());
  }
  if (labelled_array_query) {
    throw_if_not_ok(labelled_array_query->finalize());
  }
}

void DimensionLabelDataQuery::process() {
  if (indexed_array_query) {
    throw_if_not_ok(indexed_array_query->init());
    throw_if_not_ok(indexed_array_query->process());
  }
  if (labelled_array_query) {
    throw_if_not_ok(labelled_array_query->init());
    throw_if_not_ok(labelled_array_query->process());
  }
}

DimensionLabelReadDataQuery::DimensionLabelReadDataQuery(
    StorageManager* storage_manager,
    DimensionLabel* dimension_label,
    const Subarray& parent_subarray,
    const QueryBuffer& label_buffer,
    const uint32_t dim_idx)
    : DimensionLabelDataQuery{storage_manager, dimension_label, true, false} {
  // Set the layout (ordered, 1D).
  throw_if_not_ok(indexed_array_query->set_layout(Layout::ROW_MAJOR));

  // Set the subarray.
  Subarray subarray{*indexed_array_query->subarray()};
  throw_if_not_ok(
      subarray.set_ranges_for_dim(0, parent_subarray.ranges_for_dim(dim_idx)));
  throw_if_not_ok(indexed_array_query->set_subarray(subarray));

  // Set the label data buffer.
  indexed_array_query->set_buffer(
      dimension_label->label_attribute()->name(), label_buffer);
}

OrderedWriteDataQuery::OrderedWriteDataQuery(
    StorageManager* storage_manager,
    DimensionLabel* dimension_label,
    const Subarray& parent_subarray,
    const QueryBuffer& index_buffer,
    const QueryBuffer& label_buffer,
    const uint32_t dim_idx,
    optional<std::string> fragment_name)
    : DimensionLabelDataQuery{
          storage_manager, dimension_label, true, true, fragment_name} {
  // Verify only one dimension label is set.
  if (!dimension_label->labelled_array()->is_empty() ||
      !dimension_label->indexed_array()->is_empty()) {
    throw Status_DimensionLabelQueryError(
        "Cannot write to dimension label. Currently ordered dimension "
        "labels can only be written to once.");
  }
  if (!parent_subarray.is_default(dim_idx)) {
    throw StatusException(Status_DimensionLabelQueryError(
        "Failed to create dimension label query. Currently dimension "
        "labels only support writing the full array."));
  }
  // TODO: Check sort
  // TODO: Check full array

  // Set-up labelled array query (sparse array)
  throw_if_not_ok(labelled_array_query->set_layout(Layout::UNORDERED));
  labelled_array_query->set_buffer(
      dimension_label->label_attribute()->name(), label_buffer);
  labelled_array_query->set_buffer(
      dimension_label->index_attribute()->name(), index_buffer);

  // Set-up indexed array query (dense array)
  throw_if_not_ok(indexed_array_query->set_layout(Layout::ROW_MAJOR));
  indexed_array_query->set_buffer(
      dimension_label->label_attribute()->name(), label_buffer);
}

UnorderedWriteDataQuery::UnorderedWriteDataQuery(
    StorageManager* storage_manager,
    DimensionLabel* dimension_label,
    const QueryBuffer& index_buffer,
    const QueryBuffer& label_buffer,
    optional<std::string> fragment_name)
    : DimensionLabelDataQuery{
          storage_manager, dimension_label, true, true, fragment_name} {
  // Set-up labelled array (sparse array)
  throw_if_not_ok(labelled_array_query->set_layout(Layout::UNORDERED));
  labelled_array_query->set_buffer(
      dimension_label->label_dimension()->name(), label_buffer);
  labelled_array_query->set_buffer(
      dimension_label->index_attribute()->name(), index_buffer);

  // Set-up indexed array query (sparse array)
  throw_if_not_ok(indexed_array_query->set_layout(Layout::UNORDERED));
  indexed_array_query->set_buffer(
      dimension_label->label_attribute()->name(), label_buffer);
  indexed_array_query->set_buffer(
      dimension_label->index_dimension()->name(), index_buffer);
}

}  // namespace tiledb::sm
