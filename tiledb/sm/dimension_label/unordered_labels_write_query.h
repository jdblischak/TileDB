/**
 * @file tiledb/sm/dimension_label/ordered_label_write_query.h
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
 * Class for writing to an ordered dimension label.
 */

#ifndef TILEDB_UNORDERED_LABELS_WRITE_QUERY_H
#define TILEDB_UNORDERED_LABELS_WRITE_QUERY_H

#include <string>
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/dimension_label/dimension_label_query.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/query/query.h"

namespace tiledb::sm {

class Array;
class DimensionLabel;
class Query;

class UnorderedLabelsWriteQuery : public DimensionLabelQuery {
 public:
  UnorderedLabelsWriteQuery() = delete;

  UnorderedLabelsWriteQuery(
      shared_ptr<DimensionLabel> dimension_label,
      StorageManager* storage_manager,
      const RangeSetAndSuperset& label_ranges,
      const RangeSetAndSuperset& index_ranges,
      const QueryBuffer& label_buffer,
      const QueryBuffer& index_data_buffer);

  /** Disable copy and move. */
  DISABLE_COPY_AND_COPY_ASSIGN(UnorderedLabelsWriteQuery);
  DISABLE_MOVE_AND_MOVE_ASSIGN(UnorderedLabelsWriteQuery);

  void cancel() override;

  void finalize() override;

  QueryStatus status() const override;

  void submit() override;

 private:
  tdb_unique_ptr<Query> labelled_array_query_;

  tdb_unique_ptr<Query> indexed_array_query_;
};

}  // namespace tiledb::sm

#endif
