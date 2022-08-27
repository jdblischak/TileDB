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

using namespace tiledb::common;

namespace tiledb::sm {

DimensionLabelDataQuery::DimensionLabelDataQuery(
    StorageManager* storage_manager,
    DimensionLabel* dimension_label,
    bool add_indexed_query,
    bool add_labelled_query)
    : indexed_array_query{
          add_indexed_query ?
              tdb_unique_ptr<Query>(tdb_new(
                  Query, storage_manager, dimension_label->indexed_array())) :
              nullptr} 
    , labelled_array_query{
          add_labelled_query ?
              tdb_unique_ptr<Query>(tdb_new(
                  Query, storage_manager, dimension_label->labelled_array())) :
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

void DimensionLabelDataQuery::submit() {
  if (indexed_array_query) {
    throw_if_not_ok(indexed_array_query->submit());
  }
  if (labelled_array_query) {
    throw_if_not_ok(labelled_array_query->submit());
  }
}

}  // namespace tiledb::sm
