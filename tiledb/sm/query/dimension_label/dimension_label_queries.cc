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

#include "dimension_label_queries.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/query/query.h"

using namespace tiledb::common;

namespace tiledb::sm {

DimensionLabelQueries::DimensionLabelQueries(
    StorageManager* storage_manager,
    shared_ptr<Array> array,
    optional<std::string> fragment_name = nullopt)
    : storage_manager_(storage_manager)
    , array_(array)
    , fragment_name_(fragment_name) {
}

DimensionLabelQueries::add_dimension_labels(
    const Subarray& subarray, const std::vector<QueryBuffer>& label_buffers) {
  for (
  for (const auto& query_buffer : label_buffers) {
  }
}

DimensionLabelQueries::add_ordered_query(
    DimensionLabel* dimension_label,
    StorageManager* storage_manager,
    const RangeSetAndSuperset& label_ranges,
    const RangeSetAndSuperset& index_ranges,
    const QueryBuffer& label_data_buffer) {
}

DimensionLabelQueries::add_unordered_query(
    DimensionLabel* dimension_label,
    StorageManage* storage_manager,
    const RangeSetAndSuperset& label_ranges,
    const RangeSetAndSuperset& index_ranges,
    const QueryBuffer& label_data_buffer) {
}
