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

#include "tiledb/sm/dimension_label/ordered_labels_read_query.h"
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

OrderedLabelsReadQuery::OrderedLabelsReadQuery(
    shared_ptr<DimensionLabel> dimension_label,
    StorageManager* storage_manager,
    const RangeSetAndSuperset& label_ranges,
    const RangeSetAndSuperset& index_ranges,
    const QueryBuffer& label_data_buffer)
    : dimension_label_{dimension_label}
    , storage_manager_{storage_manager}
    , stats_{storage_manager_->stats()->create_child("DimensionLabelQuery")}
    , logger_(
          storage_manager->logger()->clone("DimensionLabelQuery", ++logger_id_))
    , data_query_{nullptr}
    , label_ranges_{label_ranges}
    , index_ranges_{index_ranges}
    , label_buffer_{label_data_buffer} {
  if (dimension_label_->query_type() != QueryType::READ)
    throw std::invalid_argument(
        "Failed to create dimension label query. Cannot read from dimension "
        "label opened with query type " +
        query_type_str(dimension_label_->query_type()) + ".");
  if (!label_ranges_.is_empty() && !index_ranges_.is_empty())
    throw StatusException(Status_DimensionLabelQueryError(
        "Failed to create dimension label query. Cannot add both index and "
        "label ranges to dimension label query."));
  if (label_data_buffer.buffer_) {
    if (!label_ranges_.is_empty()) {
      data_query_ = tdb_unique_ptr<Query>(
          tdb_new(Query, storage_manager_, dimension_label_->labelled_array()));
      throw_if_not_ok(data_query_->set_layout(Layout::ROW_MAJOR));
      Subarray subarray{dimension_label_->labelled_array().get(),
                        Layout::ROW_MAJOR,
                        stats_,
                        logger_};
      throw_if_not_ok(subarray.set_ranges_for_dim(0, label_ranges_.ranges()));
      throw_if_not_ok(data_query_->set_subarray(subarray));
      throw_if_not_ok(data_query_->set_data_buffer(
          dimension_label_->label_attribute()->name(),
          label_buffer_.buffer_,
          label_buffer_.buffer_size_,
          false));
    } else if (!index_ranges_.is_empty()) {
      data_query_ = tdb_unique_ptr<Query>(
          tdb_new(Query, storage_manager_, dimension_label_->indexed_array()));
      throw_if_not_ok(data_query_->set_layout(Layout::ROW_MAJOR));
      Subarray subarray{dimension_label_->indexed_array().get(),
                        Layout::ROW_MAJOR,
                        stats_,
                        logger_};
      throw_if_not_ok(subarray.set_ranges_for_dim(0, index_ranges_.ranges()));
      throw_if_not_ok(data_query_->set_subarray(subarray));
      throw_if_not_ok(data_query_->set_data_buffer(
          dimension_label_->label_attribute()->name(),
          label_buffer_.buffer_,
          label_buffer_.buffer_size_,
          false));
    }
  }
}

void OrderedLabelsReadQuery::cancel() {
  if (data_query_) {
    throw_if_not_ok(data_query_->cancel());
  }
}

void OrderedLabelsReadQuery::finalize() {
  if (data_query_) {
    throw_if_not_ok(data_query_->finalize());
  }
}

QueryStatus OrderedLabelsReadQuery::status() const {
  // Data query is blocked by range query. If no range query or range query is
  // completed, return the data query status. Otherwise, return the range query
  // status.
  return data_query_ ? data_query_->status() : QueryStatus::COMPLETED;
}

void OrderedLabelsReadQuery::submit() {
  if (data_query_) {
    throw_if_not_ok(data_query_->submit());
  }
}

}  // namespace tiledb::sm
