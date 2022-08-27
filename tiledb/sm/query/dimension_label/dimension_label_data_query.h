/**
 * @file dimension_label_data_query.h
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

#ifndef TILEDB_DIMENSION_LABEL_DATA_QUERY_H
#define TILEDB_DIMENSION_LABEL_DATA_QUERY_H

#include "tiledb/common/common.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::common;

namespace tiledb::sm {

class DimensionLabel;
class Query;
class StorageManager;

struct DimensionLabelDataQuery {
 public:
  /** Default constructor. */
  DimensionLabelDataQuery() = default;

  /**
   * General constructor.
   *
   * TODO finish docs
   */
  DimensionLabelDataQuery(
      StorageManager* storage_manager,
      DimensionLabel* dimension_label,
      bool add_indexed_query,
      bool add_labelled_query);

  /** TODO */
  void cancel();

  /** TODO */
  void finalize();

  /** TODO */
  void submit();

  /** TODO */
  tdb_unique_ptr<Query> indexed_array_query{nullptr};

  /** TODO */
  tdb_unique_ptr<Query> labelled_array_query{nullptr};
};

}  // namespace tiledb::sm

#endif
