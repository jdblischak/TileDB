/**
 * @file test-capi-dimension_labels.cc
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
 * Tests the DimensionLabel API
 */

#include "test/src/experimental_helpers.h"
#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/c_api/experimental/tiledb_dimension_label.h"
#include "tiledb/sm/c_api/experimental/tiledb_struct_def.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/encryption_type.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <test/support/tdb_catch.h>
#include <iostream>
#include <string>
#include <unordered_map>

using namespace tiledb::sm;
using namespace tiledb::test;

/**
 * Create a small sparse array with a dimension label.
 *
 */
template <tiledb_label_order_t label_order>
class SparseArrayExample1 : public DimensionLabelFixture {
 public:
  SparseArrayExample1()
      : index_domain_{0, 3}
      , label_domain_{-1, 1} {
    // Create an array schema
    uint64_t x_tile_extent{4};
    auto array_schema = create_array_schema(
        ctx,
        TILEDB_SPARSE,
        {"x"},
        {TILEDB_UINT64},
        {&index_domain_[0]},
        {&x_tile_extent},
        {"a"},
        {TILEDB_FLOAT64},
        {1},
        {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        4096,
        false);

    // Add dimension label.
    double label_tile_extent = 2.0;
    add_dimension_label(
        ctx,
        array_schema,
        "x",
        0,
        label_order,
        TILEDB_FLOAT64,
        &label_domain_[0],
        &label_tile_extent,
        &x_tile_extent);
    // Create array
    array_name = create_temporary_array("array_with_label_1", array_schema);
    tiledb_array_schema_free(&array_schema);
  }

  void write_array_with_label(
      std::vector<uint64_t>& input_index_data,
      std::vector<double>& input_attr_data,
      std::vector<double>& input_label_data) {
    // Open array for writing.
    tiledb_array_t* array;
    require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_WRITE));

    // Define sizes for setting buffers.
    uint64_t index_data_size{input_index_data.size() * sizeof(uint64_t)};
    uint64_t attr_data_size{input_attr_data.size() * sizeof(double)};
    uint64_t label_data_size{input_label_data.size() * sizeof(double)};

    // Create write query.
    tiledb_query_t* query;
    require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));
    require_tiledb_ok(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED));
    if (index_data_size != 0) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "x", input_index_data.data(), &index_data_size));
    }
    if (attr_data_size != 0) {
      require_tiledb_ok(tiledb_query_set_data_buffer(
          ctx, query, "a", input_attr_data.data(), &attr_data_size));
    }
    if (label_data_size != 0) {
      require_tiledb_ok(tiledb_query_set_label_data_buffer(
          ctx, query, "x", input_label_data.data(), &label_data_size));
    }

    // Submit write query.
    require_tiledb_ok(tiledb_query_submit(ctx, query));
    tiledb_query_status_t query_status;
    require_tiledb_ok(tiledb_query_get_status(ctx, query, &query_status));
    REQUIRE(query_status == TILEDB_COMPLETED);

    // Clean-up.
    tiledb_query_free(&query);
    tiledb_array_free(&array);
  }

  std::vector<double> read_indexed_array() {
    return DimensionLabelFixture::read_indexed_array<double>(
        URI(array_name).join_path("__labels/l0"),
        4,
        &index_domain_[0],
        &index_domain_[1]);
  }

  std::tuple<std::vector<uint64_t>, std::vector<double>> read_labelled_array() {
    return DimensionLabelFixture::read_labelled_array<uint64_t, double>(
        URI(array_name).join_path("__labels/l0"),
        4,
        &label_domain_[0],
        &label_domain_[1]);
  }

 protected:
  /** Name of the example array. */
  std::string array_name;

 private:
  /** Valid range for the index. */
  uint64_t index_domain_[2];

  /** Valid range for the label. */
  double label_domain_[2];
};

TEST_CASE_METHOD(
    SparseArrayExample1<TILEDB_INCREASING_LABELS>,
    "Write increasing dimension label and array for sparse 1D array",
    "[capi][query][DimensionLabel]") {
  // Define input data and write.
  std::vector<uint64_t> input_index_data{0, 1, 2, 3};
  std::vector<double> input_label_data{-1.0, 0.0, 0.5, 1.0};
  std::vector<double> input_attr_data{0.5, 1.0, 1.5, 2.0};
  write_array_with_label(input_index_data, input_attr_data, input_label_data);

  // Read back and check indexed array.
  {
    auto label_data = read_indexed_array();
    // Check results.
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(label_data[ii] == input_label_data[ii]);
    }
  }

  // Read back and check labelled array.
  {
    auto [index_data, label_data] = read_labelled_array();
    // Check results.
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(label_data[ii] == input_label_data[ii]);
    }
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(index_data[ii] == input_index_data[ii]);
    }
  }
}

TEST_CASE_METHOD(
    SparseArrayExample1<TILEDB_INCREASING_LABELS>,
    "Write increasing dimension label only for sparse 1D array",
    "[capi][query][DimensionLabel]") {
  // Define input data and write.
  std::vector<uint64_t> input_index_data{0, 1, 2, 3};
  std::vector<double> input_label_data{-1.0, 0.0, 0.5, 1.0};
  std::vector<double> input_attr_data{0.5, 1.0, 1.5, 2.0};
  write_array_with_label(input_index_data, input_attr_data, input_label_data);

  // Read back and check indexed array.
  {
    auto label_data = read_indexed_array();
    // Check results.
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(label_data[ii] == input_label_data[ii]);
    }
  }

  // Read back and check labelled array.
  {
    auto [index_data, label_data] = read_labelled_array();
    // Check results.
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(label_data[ii] == input_label_data[ii]);
    }
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(index_data[ii] == input_index_data[ii]);
    }
  }
}
