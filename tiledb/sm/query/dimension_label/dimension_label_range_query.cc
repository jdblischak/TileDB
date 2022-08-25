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
 */

#include "tiledb/sm/query/dimension_label/dimension_label_range_query.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/storage_manager/storage_manager.h"
#include "tiledb/sm/subarray/range_subset.h"

namespace tiledb::sm {

std::function<bool(const Range&, const Range&)> label_upper_bound_greater_than(
    Datatype type) {
  switch (type) {
    case Datatype::INT8:
      return upper_bound_greater_than<int8_t>;
    case Datatype::UINT8:
      return upper_bound_greater_than<uint8_t>;
    case Datatype::INT16:
      return upper_bound_greater_than<int16_t>;
    case Datatype::UINT16:
      return upper_bound_greater_than<uint16_t>;
    case Datatype::INT32:
      return upper_bound_greater_than<int32_t>;
    case Datatype::UINT32:
      return upper_bound_greater_than<uint32_t>;
    case Datatype::INT64:
      return upper_bound_greater_than<int64_t>;
    case Datatype::UINT64:
      return upper_bound_greater_than<uint64_t>;
    case Datatype::FLOAT32:
      return upper_bound_greater_than<float>;
    case Datatype::FLOAT64:
      return upper_bound_greater_than<double>;
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
      return upper_bound_greater_than<int64_t>;
    default:
      throw std::invalid_argument(
          "Index datatype '" + datatype_str(type) +
          "' not supported for range queries.");
  }
}

std::function<void(Range& range)> index_range_fixer(
    LabelOrder order, Datatype type) {
  if (order != LabelOrder::INCREASING_LABELS &&
      order != LabelOrder::DECREASING_LABELS)
    throw std::invalid_argument(
        "Support for reading ranges is only implemented for increasing and "
        "decreasing labels.");
  switch (type) {
    case Datatype::INT8:
      return order == LabelOrder::INCREASING_LABELS ?
                 decrease_upper_bound<int8_t> :
                 increase_upper_bound<int8_t>;
    case Datatype::UINT8:
      return order == LabelOrder::INCREASING_LABELS ?
                 decrease_upper_bound<uint8_t> :
                 increase_upper_bound<uint8_t>;
    case Datatype::INT16:
      return order == LabelOrder::INCREASING_LABELS ?
                 decrease_upper_bound<int16_t> :
                 increase_upper_bound<int16_t>;
    case Datatype::UINT16:
      return order == LabelOrder::INCREASING_LABELS ?
                 decrease_upper_bound<uint16_t> :
                 increase_upper_bound<uint16_t>;
    case Datatype::INT32:
      return order == LabelOrder::INCREASING_LABELS ?
                 decrease_upper_bound<int32_t> :
                 increase_upper_bound<int32_t>;
    case Datatype::UINT32:
      return order == LabelOrder::INCREASING_LABELS ?
                 decrease_upper_bound<uint32_t> :
                 increase_upper_bound<uint32_t>;
    case Datatype::INT64:
      return order == LabelOrder::INCREASING_LABELS ?
                 decrease_upper_bound<int64_t> :
                 increase_upper_bound<int64_t>;
    case Datatype::UINT64:
      return order == LabelOrder::INCREASING_LABELS ?
                 decrease_upper_bound<uint64_t> :
                 increase_upper_bound<uint64_t>;
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
      return order == LabelOrder::INCREASING_LABELS ?
                 decrease_upper_bound<int64_t> :
                 increase_upper_bound<int64_t>;
    default:
      throw std::invalid_argument(
          "Index datatype '" + datatype_str(type) +
          "' not supported for range queries.");
  }
}

DimensionLabelRangeQuery::DimensionLabelRangeQuery(
    DimensionLabel* dimension_label,
    StorageManager* storage_manager,
    const RangeSetAndSuperset& label_ranges)
    : order_{dimension_label->label_order()}
    , input_label_range_{}
    , computed_index_range_{dimension_label->index_dimension()->domain()}
    , lower_bound_query_{storage_manager, dimension_label->labelled_array()}
    , lower_bound_index_buffer_size_{datatype_size(
          dimension_label->index_attribute()->type())}
    , lower_bound_label_buffer_size_{datatype_size(
          dimension_label->label_dimension()->type())}
    , upper_bound_query_{storage_manager, dimension_label->labelled_array()}
    , upper_bound_index_buffer_size_{datatype_size(
          dimension_label->index_attribute()->type())}
    , upper_bound_label_buffer_size_{datatype_size(
          dimension_label->label_dimension()->type())}
    , has_extra_range_element_{label_upper_bound_greater_than(
          dimension_label->label_dimension()->type())}
    , fix_index_range_{index_range_fixer(
          order_, dimension_label->index_attribute()->type())} {
  // TODO: ensure label data is a supported type
  // TODO: ensure index data is a supported type

  // Check there is exactly one range in the label ranges.
  if (label_ranges.num_ranges() == 0)
    throw Status_RangeQueryError(
        "Cannot initialize range query; no query to set.");
  if (label_ranges.num_ranges() > 1) {
    throw Status_RangeQueryError(
        "Cannot initialize range query; Setting more than one label range is "
        "currently unsupported.");
  }
  // Set the input label range and initialize the compute label range.
  input_label_range_ = label_ranges[0];
  computed_label_range_ = label_ranges[0];

  // Initialize some useful values.
  const auto label_dim = dimension_label->label_dimension();
  const auto label_domain = label_dim->domain();
  const auto label_name = label_dim->name();
  const auto index_name = dimension_label->index_attribute()->name();

  // Create query for the lower bound.
  throw_if_not_ok(lower_bound_query_.set_layout(Layout::ROW_MAJOR));
  throw_if_not_ok(lower_bound_query_.add_range(
      0, input_label_range_.start_fixed(), label_domain.end_fixed(), nullptr));
  throw_if_not_ok(lower_bound_query_.set_data_buffer(
      label_name,
      const_cast<void*>(computed_label_range_.start_fixed()),
      &lower_bound_label_buffer_size_));

  // Create query for the upper bound
  throw_if_not_ok(upper_bound_query_.set_layout(Layout::ROW_MAJOR));
  throw_if_not_ok(upper_bound_query_.add_range(
      0, input_label_range_.end_fixed(), label_domain.end_fixed(), nullptr));
  throw_if_not_ok(upper_bound_query_.set_data_buffer(
      label_name,
      const_cast<void*>(computed_label_range_.end_fixed()),
      &upper_bound_label_buffer_size_));

  // Set data buffer for computed index range.
  switch (order_) {
    case (LabelOrder::INCREASING_LABELS):
      throw_if_not_ok(lower_bound_query_.set_data_buffer(
          index_name,
          const_cast<void*>(computed_index_range_.start_fixed()),
          &lower_bound_label_buffer_size_));
      throw_if_not_ok(upper_bound_query_.set_data_buffer(
          index_name,
          const_cast<void*>(computed_index_range_.end_fixed()),
          &upper_bound_index_buffer_size_));
      break;
    case (LabelOrder::DECREASING_LABELS):
      throw_if_not_ok(upper_bound_query_.set_data_buffer(
          index_name,
          const_cast<void*>(computed_index_range_.start_fixed()),
          &upper_bound_index_buffer_size_));
      throw_if_not_ok(lower_bound_query_.set_data_buffer(
          index_name,
          const_cast<void*>(computed_index_range_.end_fixed()),
          &lower_bound_index_buffer_size_));
      break;
    default:
      throw std::invalid_argument(
          "Support for reading ranges is only implemented for increasing and "
          "decreasing labels.");
  }

  // Initialize both queries.
  throw_if_not_ok(lower_bound_query_.init());
  throw_if_not_ok(upper_bound_query_.init());
}

void DimensionLabelRangeQuery::cancel() {
  throw_if_not_ok(lower_bound_query_.cancel());
  throw_if_not_ok(upper_bound_query_.cancel());
}

void DimensionLabelRangeQuery::finalize() {
  throw_if_not_ok(lower_bound_query_.finalize());
  throw_if_not_ok(upper_bound_query_.finalize());
}

void DimensionLabelRangeQuery::submit() {
  auto status = lower_bound_query_.process();
  if (!status.ok()) {
    upper_bound_query_.cancel();
    throw StatusException(status);
  }
  status = upper_bound_query_.process();
  if (!status.ok()) {
    lower_bound_query_.cancel();
    throw StatusException(status);
  }
  if (!lower_bound_query_.has_results() || !upper_bound_query_.has_results()) {
    status_ = QueryStatus::FAILED;
    lower_bound_query_.finalize();
    upper_bound_query_.finalize();
    throw StatusException(
        Status_RangeQueryError("Failed to read index range from label."));
  }
  // This will compare the upper bound of the label query and fix the computed
  // index range the values do no match.
  //
  // For increasing labels, if the computed label upper bound is greater than
  // the input range, we need to decrease the range to the previous value.
  //
  // For decreasing labels, if the computed label upper bound is greater than
  // the input range, we need to increase the range to the next value.
  if (has_extra_range_element_(computed_label_range_, input_label_range_))
    fix_index_range_(computed_index_range_);
  status_ = QueryStatus::COMPLETED;
}

}  // namespace tiledb::sm
