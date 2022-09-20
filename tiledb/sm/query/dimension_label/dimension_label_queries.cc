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

#include <algorithm>

using namespace tiledb::common;

namespace tiledb::sm {

DimensionLabelQueries::DimensionLabelQueries(
    StorageManager* storage_manager,
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_buffers,
    const std::unordered_map<std::string, QueryBuffer>& array_buffers,
    optional<std::string> fragment_name)
    : range_query_status_{QueryStatus::UNINITIALIZED} {
  switch (array->get_query_type()) {
    case (QueryType::READ):
      add_range_queries(
          storage_manager, array, subarray, label_buffers, array_buffers);
      add_data_queries_for_read(
          storage_manager, array, subarray, label_buffers);
      break;
    case (QueryType::WRITE):
      add_range_queries(
          storage_manager, array, subarray, label_buffers, array_buffers);
      add_data_queries_for_write(
          storage_manager,
          array,
          subarray,
          label_buffers,
          array_buffers,
          fragment_name);
      break;
    case (QueryType::DELETE):
    case (QueryType::UPDATE):
    case (QueryType::MODIFY_EXCLUSIVE):
      if (!label_buffers.empty() || subarray.has_label_ranges()) {
        throw StatusException(Status_DimensionLabelQueryError(
            "Failed to add dimension label queries. Query type " +
            query_type_str(array->get_query_type()) +
            " is not supported for dimension labels."));
      }
      break;
    default:
      throw StatusException(Status_DimensionLabelQueryError(
          "Failed to add dimension label queries. Unknown query type " +
          query_type_str(array->get_query_type()) + "."));
  }
  range_query_status_ =
      range_queries_.empty() ? QueryStatus::COMPLETED : QueryStatus::INPROGRESS;
}

void DimensionLabelQueries::cancel() {
  for (auto& [label_name, query] : range_queries_map_) {
    query->cancel();
  }
  for (auto& [label_name, query] : data_queries_) {
    query->cancel();
  }
}

void DimensionLabelQueries::finalize() {
  for (auto& [label_name, query] : range_queries_map_) {
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

void DimensionLabelQueries::process_range_queries(Subarray& subarray) {
  // TODO: Parallel?
  for (auto& [label_name, query] : range_queries_map_) {
    query->process();
  }

  // TODO: Do something smart for throwing errors and setting ranges.
  // Update the subarray.
  for (dimension_size_type dim_idx{0}; dim_idx < subarray.dim_num();
       ++dim_idx) {
    const auto& range_query = range_queries_[dim_idx];

    // Continue to next dimension index if no range query.
    if (!range_query) {
      continue;
    }

    if (range_query->status() != QueryStatus::COMPLETED) {
      range_query_status_ = QueryStatus::FAILED;
      // TODO: Allow incomplete and push up failed reason for acutal failed
      // queries.
      return;
    }

    // Get pointer to ranges from range query.
    auto [is_point_ranges, range_start, count] = range_query->index_ranges();

    // Continue if there is no ranges.
    // TODO: Special logic for empty range?
    if (count == 0) {
      continue;
    }

    Status st = Status::Ok();
    // Add ranges to subarray.
    if (is_point_ranges) {
      st = subarray.add_point_ranges(dim_idx, range_start, count);
    } else {
      // TODO: Before merge rebase to use new method for bulk adding ranges
      if (count != 1) {
        auto range_size = 2 * subarray.array()
                                  ->array_schema_latest()
                                  .dimension_ptr(dim_idx)
                                  ->coord_size();
        for (uint64_t range_idx{0}; range_idx < count; ++range_idx) {
          const auto* start =
              static_cast<const uint8_t*>(range_start) + range_idx * range_size;
          const auto* end = static_cast<const uint8_t*>(range_start) +
                            (range_idx + 1) * range_size;
          st = subarray.add_range(dim_idx, start, end, nullptr);
          if (!st.ok()) {
            break;
          }
        }
      }
    }
    if (!st.ok()) {
      range_query_status_ = QueryStatus::FAILED;
      throw StatusException(st);
    }

    // TODO: Store completion of individual ranges and skip over them once
    // subarray is successfully update. If subarray is unsucessfully updated,
    // remove ranges.
  }
  range_query_status_ = QueryStatus::COMPLETED;
}

void DimensionLabelQueries::add_data_queries_for_read(
    StorageManager* storage_manager,
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_buffers) {
  for (const auto& [label_name, label_buffer] : label_buffers) {
    // Skip adding a new query if this dimension label was already used to
    // add a range query above.
    if (range_queries_map_.find(label_name) != range_queries_map_.end()) {
      continue;
    }

    // Get the dimension label reference from the array.
    const auto& dim_label_ref =
        array->array_schema_latest().dimension_label_reference(label_name);
    // TODO: Ensure label order type is valid with
    // ensure_label_order_is_valid.

    // Open the indexed array.
    auto* dim_label = open_dimension_label(
        storage_manager, array, dim_label_ref, QueryType::READ, true, false);

    // Create the data query.
    data_queries_[label_name] = tdb_unique_ptr<DimensionLabelDataQuery>(tdb_new(
        DimensionLabelReadDataQuery,
        storage_manager,
        dim_label,
        subarray,
        label_buffer,
        dim_label_ref.dimension_id()));
  }
}

void DimensionLabelQueries::add_data_queries_for_write(
    StorageManager* storage_manager,
    Array* array,
    const Subarray& subarray,
    const std::unordered_map<std::string, QueryBuffer>& label_buffers,
    const std::unordered_map<std::string, QueryBuffer>& array_buffers,
    const optional<std::string> fragment_name) {
  for (const auto& [label_name, label_buffer] : label_buffers) {
    // Skip adding a new query if this dimension label was already used to
    // add a range query above.
    if (range_queries_map_.find(label_name) != range_queries_map_.end()) {
      continue;
    }

    // Get the dimension label reference from the array.
    const auto& dim_label_ref =
        array->array_schema_latest().dimension_label_reference(label_name);

    // Open both arrays in the dimension label.
    auto* dim_label = open_dimension_label(
        storage_manager, array, dim_label_ref, QueryType::WRITE, true, true);

    // Get the index_buffer from the array buffers.
    const auto& dim_name = array->array_schema_latest()
                               .dimension_ptr(dim_label_ref.dimension_id())
                               ->name();
    const auto& index_buffer_pair = array_buffers.find(dim_name);

    switch (dim_label_ref.label_order()) {
      case (LabelOrder::INCREASING_LABELS):
      case (LabelOrder::DECREASING_LABELS):
        if (index_buffer_pair == array_buffers.end()) {
          data_queries_[label_name] =
              tdb_unique_ptr<DimensionLabelDataQuery>(tdb_new(
                  OrderedWriteDataQuery,
                  storage_manager,
                  dim_label,
                  subarray,
                  label_buffer,
                  dim_label_ref.dimension_id(),
                  fragment_name));
        } else {
          data_queries_[label_name] =
              tdb_unique_ptr<DimensionLabelDataQuery>(tdb_new(
                  OrderedWriteDataQuery,
                  storage_manager,
                  dim_label,
                  index_buffer_pair->second,
                  label_buffer,
                  fragment_name));
        }
        break;
      case (LabelOrder::UNORDERED_LABELS):
        if (index_buffer_pair == array_buffers.end()) {
          throw StatusException(Status_DimensionLabelQueryError(
              "Cannot read range data from unordered label '" + label_name +
              "'; Missing a data buffer for dimension '" + dim_name + "'."));
        }
        data_queries_[label_name] =
            tdb_unique_ptr<DimensionLabelDataQuery>(tdb_new(
                UnorderedWriteDataQuery,
                storage_manager,
                dim_label,
                index_buffer_pair->second,
                label_buffer,
                fragment_name));
        break;
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
    const std::unordered_map<std::string, QueryBuffer>& label_buffers,
    const std::unordered_map<std::string, QueryBuffer>& array_buffers) {
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
    const auto label_buffer_pair = label_buffers.find(label_name);
    bool has_label_buffer{label_buffer_pair != label_buffers.end()};

    // Check this array and dimension label type can handle label queries.
    if (has_label_buffer && !array->array_schema_latest().dense()) {
      throw StatusException(Status_DimensionLabelQueryError(
          "Failed to intialize dimension label queries. Reading dimension "
          "label data from a sparse array has not been implemented."));
    }

    // TODO: Implement reading for the unordered dimension label.
    switch (dim_label_ref.label_order()) {
      case (LabelOrder::INCREASING_LABELS):
      case (LabelOrder::DECREASING_LABELS): {
        // Open the dimension label.
        auto* dim_label = open_dimension_label(
            storage_manager,
            array,
            dim_label_ref,
            QueryType::READ,
            false,
            true);

        // Add the query.
        range_queries_map_[dim_label_ref.name()] =
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
        const auto& index_buffer_pair = array_buffers.find(dim_name);
        if (index_buffer_pair == array_buffers.end()) {
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
    range_queries_[dim_idx] = range_queries_map_[dim_label_ref.name()].get();
  }
}

bool DimensionLabelQueries::completed() const {
  return std::all_of(
             range_queries_.cbegin(),
             range_queries_.cend(),
             [](const auto& query) { return query->completed(); }) &&
         std::all_of(
             data_queries_.cbegin(),
             data_queries_.cend(),
             [](const auto& query) { return query.second->completed(); });
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
