/**
 * @file dimension_label_queries.cc
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

#include "tiledb/sm/query/dimension_label/dimension_label_queries.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/array_schema/dimension_label_schema.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/label_order.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/query/dimension_label/dimension_label_data_query.h"
#include "tiledb/sm/query/dimension_label/dimension_label_range_query.h"
#include "tiledb/sm/query/query.h"

using namespace tiledb::common;

namespace tiledb::sm {
/*
DimensionLabelQueries::DimensionLabelQueries(
    StorageManager* storage_manager,
    shared_ptr<Array> array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_data_buffers,
    const std::unordered_map<std::string, QueryBuffer>& label_offsets_buffers,
    const std::unordered_map<std::string, QueryBuffer>& array_data_buffers,
    optional<std::string>)
    // optional<std::string> fragment_name)
    : storage_manager_(storage_manager)
    , array_(array) {
  switch (array_->get_query_type()) {
    case (QueryType::READ):
      add_range_queries(
          subarray,
          label_data_buffers,
          label_offsets_buffers,
          array_data_buffers);
      add_data_queries_for_read(
          subarray, label_data_buffers, label_offsets_buffers);
      break;
    case (QueryType::WRITE): {
      add_range_queries(
          subarray,
          label_data_buffers,
          label_offsets_buffers,
          array_data_buffers);
      add_range_queries_for_write(
          subarray,
          label_data_buffers,
          label_offsets_buffers,
          array_data_buffers);
      break;
    }

    default:
      throw StatusException(Status_DimensionLabelQueryError(
          "Failed toadd dimension label queries. Query type " +
          query_type_str(array_->get_query_type()) +
          " is not supported for dimension labels."));
  }
}
*/

void DimensionLabelQueries::cancel() {
  for (auto& [label_name, query] : range_queries_) {
    query->cancel();
  }
  for (auto& [label_name, query] : data_queries_) {
    query->cancel();
  }
}

void DimensionLabelQueries::finalize() {
  for (auto& [label_name, query] : range_queries_) {
    query->finalize();
  }
  for (auto& [label_name, query] : data_queries_) {
    query->finalize();
  }
}

void DimensionLabelQueries::process_data_queries() {
  // TODO: Parallel?
  for (auto& [label_name, query] : data_queries_) {
    query->process();
  }
}

void DimensionLabelQueries::process_range_queries() {
  // TODO: Parallel?
  for (auto& [label_name, query] : range_queries_) {
    query->process();
  }
}

void DimensionLabelQueries::add_data_queries_for_read(
    StorageManager* storage_manager,
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_data_buffers,
    const std::unordered_map<std::string, QueryBuffer>& label_offsets_buffers) {
  for (const auto& [label_name, data_buffer] : label_data_buffers) {
    // Skip adding a new query if this dimension label was already used to
    // add a range query above.
    if (range_queries_.find(label_name) != range_queries_.end()) {
      continue;
    }

    // Get the dimension label reference from the array.
    const auto& dim_label_ref =
        array->array_schema_latest().dimension_label_reference(label_name);
    // TODO: Ensure label order type is valid.

    // Open the indexed array.
    auto* dim_label = open_dimension_label(
        storage_manager, array, dim_label_ref, QueryType::READ, true, false);

    // Create the data query.
    data_queries_[label_name] = tdb_unique_ptr<DimensionLabelDataQuery>(tdb_new(
        DimensionLabelDataQuery, storage_manager, dim_label, true, false));
    auto& data_query = data_queries_[label_name]->indexed_array_query;

    // Set the layout (ordered, 1D).
    throw_if_not_ok(data_query->set_layout(Layout::ROW_MAJOR));

    // Set the subarray.
    Subarray data_query_subarray{*data_query->subarray()};
    throw_if_not_ok(data_query_subarray.set_ranges_for_dim(
        0, subarray.ranges_for_dim(dim_label_ref.dimension_id())));
    throw_if_not_ok(data_query->set_subarray(data_query_subarray));

    // Set the label data buffer.
    throw_if_not_ok(data_query->set_data_buffer(
        dim_label->label_attribute()->name(),
        data_buffer.buffer_,
        data_buffer.buffer_size_,
        false));

    // Set the label offsets buffer if it exists.
    const auto label_offsets_buffer_pair =
        label_offsets_buffers.find(label_name);
    if (label_offsets_buffer_pair != label_offsets_buffers.end()) {
      throw_if_not_ok(data_query->set_data_buffer(
          dim_label->label_attribute()->name(),
          label_offsets_buffer_pair->second.buffer_,
          label_offsets_buffer_pair->second.buffer_size_,
          false));
    }
  }
}

void DimensionLabelQueries::add_data_queries_for_write(
    StorageManager* storage_manager,
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_data_buffers,
    const std::unordered_map<std::string, QueryBuffer>& label_offsets_buffers,
    const std::unordered_map<std::string, QueryBuffer>& array_data_buffers,
    const optional<std::string> fragment_name) {
  for (const auto& [label_name, label_data_buffer] : label_data_buffers) {
    // Skip adding a new query if this dimension label was already used to
    // add a range query above.
    if (range_queries_.find(label_name) != range_queries_.end()) {
      continue;
    }

    // Get the dimension label reference from the array.
    const auto& dim_label_ref =
        array->array_schema_latest().dimension_label_reference(label_name);

    // Open the indexed array.
    auto* dim_label = open_dimension_label(
        storage_manager, array, dim_label_ref, QueryType::READ, true, false);
    // Create the data query.
    data_queries_[label_name] = tdb_unique_ptr<DimensionLabelDataQuery>(tdb_new(
        DimensionLabelDataQuery,
        storage_manager,
        dim_label,
        true,
        false,
        fragment_name));
    auto& labelled_array_query = data_queries_[label_name]->indexed_array_query;
    auto& indexed_array_query = data_queries_[label_name]->labelled_array_query;

    // Get the index_data_buffer from the array buffers.
    const auto& dim_name = array->array_schema_latest()
                               .dimension_ptr(dim_label_ref.dimension_id())
                               ->name();
    const auto& index_data_buffer_pair = array_data_buffers.find(dim_name);
    if (index_data_buffer_pair == array_data_buffers.end()) {
      throw StatusException(Status_DimensionLabelQueryError(
          "Cannot read range data from unordered label '" + label_name +
          "'; Missing a data buffer for dimension '" + dim_name + "'."));
    }
    const auto& index_data_buffer = index_data_buffer_pair->second;

    // TODO add label offsets buffer
    const auto& label_offsets_buffer_pair =
        label_offsets_buffers.find(label_name);
    if (label_offsets_buffer_pair != label_offsets_buffers.end()) {
      throw std::runtime_error("TODO");
    }

    switch (dim_label_ref.label_order()) {
      case (LabelOrder::INCREASING_LABELS):
      case (LabelOrder::DECREASING_LABELS):
        // Verify only one dimension label is set.
        if (!dim_label->labelled_array()->is_empty() ||
            !dim_label->indexed_array()->is_empty()) {
          throw Status_DimensionLabelQueryError(
              "Cannot write to dimension label. Currently ordered dimension "
              "labels can only be written to once.");
        }
        if (!subarray.is_default(dim_label_ref.dimension_id())) {
          throw StatusException(Status_DimensionLabelQueryError(
              "Failed to create dimension label query. Currently dimension "
              "labels "
              "only support writing the full array."));
        }
        // TODO: Check sort
        // TODO: CHeck full array
        // TODO: Check
        {  // Set-up labelled array query (sparse array)
          throw_if_not_ok(labelled_array_query->set_layout(Layout::UNORDERED));
          throw_if_not_ok(labelled_array_query->set_data_buffer(
              dim_label->label_dimension()->name(),
              label_data_buffer.buffer_,
              label_data_buffer.buffer_size_,
              true));
          throw_if_not_ok(labelled_array_query->set_data_buffer(
              dim_label->index_attribute()->name(),
              index_data_buffer.buffer_,
              index_data_buffer.buffer_size_,
              true));
          // Set-up indexed array query (dense array)
          throw_if_not_ok(indexed_array_query->set_layout(Layout::ROW_MAJOR));
          throw_if_not_ok(indexed_array_query->set_data_buffer(
              dim_label->label_attribute()->name(),
              label_data_buffer.buffer_,
              label_data_buffer.buffer_size_,
              true));
        }
        break;
      case (LabelOrder::UNORDERED_LABELS): {
        throw_if_not_ok(labelled_array_query->set_layout(Layout::UNORDERED));
        throw_if_not_ok(labelled_array_query->set_data_buffer(
            dim_label->label_dimension()->name(),
            label_data_buffer.buffer_,
            label_data_buffer.buffer_size_,
            true));
        throw_if_not_ok(labelled_array_query->set_data_buffer(
            dim_label->index_attribute()->name(),
            index_data_buffer.buffer_,
            index_data_buffer.buffer_size_,
            true));
        // Set-up indexed array query (dense array)
        throw_if_not_ok(indexed_array_query->set_layout(Layout::UNORDERED));
        throw_if_not_ok(indexed_array_query->set_data_buffer(
            dim_label->label_attribute()->name(),
            label_data_buffer.buffer_,
            label_data_buffer.buffer_size_,
            true));
        throw_if_not_ok(indexed_array_query->set_data_buffer(
            dim_label->index_dimension()->name(),
            index_data_buffer.buffer_,
            index_data_buffer.buffer_size_,
            true));

      } break;
      default:
        // Invalid label order type.
        throw StatusException(Status_DimensionLabelQueryError(
            "Cannot initialize dimension label '" + label_name +
            "'; Dimension label order " +
            label_order_str(dim_label_ref.label_order()) + " not supported."));
    }
  }
}

void DimensionLabelQueries::add_range_queries(
    StorageManager* storage_manager,
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_data_buffers,
    const std::unordered_map<std::string, QueryBuffer>& label_offsets_buffers,
    const std::unordered_map<std::string, QueryBuffer>& array_data_buffers) {
  // Add queries for dimension labels set on the subarray.
  for (ArraySchema::dimension_size_type dim_idx{0};
       dim_idx < subarray.dim_num();
       ++dim_idx) {
    // Continue to the next dimension if this dimension does not have any
    // label ranges.
    if (!subarray.has_label_ranges(dim_idx)) {
      continue;
    }

    // Get the dimension label reference from the array.
    const auto& label_name = subarray.get_label_name(dim_idx);
    const auto& dim_label_ref =
        array->array_schema_latest().dimension_label_reference(label_name);

    // Find the label data buffer.
    const auto label_buffer_pair = label_data_buffers.find(label_name);
    bool has_label_buffer{label_buffer_pair != label_data_buffers.end()};

    // Check this array and dimension label type can handle label queries.
    if (has_label_buffer && array->array_schema_latest().dense()) {
      throw StatusException(Status_DimensionLabelQueryError(
          "Failed to intialize dimension label queries. Reading dimension "
          "label data from a sparse array has not been implemented."));
    }

    // TODO: Implement reading for the unordered dimension label.
    switch (dim_label_ref.label_order()) {
      case (LabelOrder::INCREASING_LABELS):
      case (LabelOrder::DECREASING_LABELS): {
        const auto& label_offsets_buffer_pair =
            label_offsets_buffers.find(label_name);
        if (label_offsets_buffer_pair != label_offsets_buffers.end()) {
          throw StatusException(Status_DimensionLabelQueryError(
              "Reading ranges from ordered dimension labels with variable "
              "length label data is not implemented."));
        }

        // Open the dimension label.
        auto* dim_label = open_dimension_label(
            storage_manager,
            array,
            dim_label_ref,
            QueryType::READ,
            false,
            true);

        // Add the query.
        range_queries_[dim_label_ref.name()] =
            tdb_unique_ptr<DimensionLabelRangeQuery>(tdb_new(
                DimensionLabelRangeQuery,
                dim_label,
                storage_manager,
                subarray.ranges_for_label(label_name)));
        break;
      }
      case (LabelOrder::UNORDERED_LABELS): {
        const auto& dim_name = array->array_schema_latest()
                                   .dimension_ptr(dim_label_ref.dimension_id())
                                   ->name();
        const auto& index_data_buffer_pair = array_data_buffers.find(dim_name);
        if (index_data_buffer_pair == array_data_buffers.end()) {
          throw StatusException(Status_DimensionLabelQueryError(
              "Cannot read range data from unordered label '" + label_name +
              "'; Missing a data buffer for dimension '" + dim_name + "'."));
        }

        throw std::runtime_error("TODO");
        break;
      }
      default:
        throw StatusException(Status_DimensionLabelQueryError(
            "Cannot initialize dimension label '" + label_name +
            "'; Dimension label order " +
            label_order_str(dim_label_ref.label_order()) + " not supported."));
    }
  }
}

DimensionLabel* DimensionLabelQueries::open_dimension_label(
    StorageManager* storage_manager,
    Array* array,
    const DimensionLabelReference& dim_label_ref,
    const QueryType& query_type,
    const bool open_indexed_array,
    const bool open_labelled_array) {
  const auto& uri = dim_label_ref.uri();
  bool is_relative = true;  // TODO: Fix to move this to array
  auto dim_label_uri =
      is_relative ? array->array_uri().join_path(uri.to_string()) : uri;

  // Create dimension label.
  dimension_labels_[dim_label_ref.name()] = tdb_unique_ptr<DimensionLabel>(
      tdb_new(DimensionLabel, dim_label_uri, storage_manager));
  auto* dim_label = dimension_labels_[dim_label_ref.name()].get();

  // Currently there is no way to open just one of these arrays. This is a
  // placeholder for a single array open is implemented.
  if (open_indexed_array || open_labelled_array) {
    // Open the dimension label.
    // TODO: Fix how the encryption is handled.
    // TODO: Fix the timestamps for writes.
    dim_label->open(
        query_type,
        array->timestamp_start(),
        array->timestamp_end(),
        EncryptionType::NO_ENCRYPTION,
        nullptr,
        0);

    // Check the dimension label schema matches the definition provided in
    // the dimension label reference.
    // TODO: Move to array schema.
    const auto& label_schema = dim_label->schema();
    if (!label_schema.is_compatible_label(
            array->array_schema_latest().dimension_ptr(
                dim_label_ref.dimension_id()))) {
      throw StatusException(
          Status_DimensionLabelQueryError("TODO: Add error msg."));
    }
    if (label_schema.label_order() != dim_label_ref.label_order()) {
      throw StatusException(
          Status_DimensionLabelQueryError("TODO: Add error msg."));
    }
    if (label_schema.label_dimension()->type() != dim_label_ref.label_type()) {
      throw StatusException(
          Status_DimensionLabelQueryError("TODO: Add error msg."));
    }
    if (!(label_schema.label_dimension()->domain() ==
          dim_label_ref.label_domain())) {
      throw StatusException(
          Status_DimensionLabelQueryError("TODO: Add error msg."));
    }
    if (label_schema.label_dimension()->cell_val_num() !=
        dim_label_ref.label_cell_val_num()) {
      throw StatusException(
          Status_DimensionLabelQueryError("TODO: Add error msg."));
    }
  }

  return dim_label;
}

}  // namespace tiledb::sm
