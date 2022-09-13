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

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "Write ordered dimension label data",
    "[capi][query][DimensionLabel]") {
  // Create an array schema
  uint64_t x_domain[2]{0, 3};
  uint64_t x_tile_extent{4};
  auto array_schema = create_array_schema(
      ctx,
      TILEDB_SPARSE,
      {"x"},
      {TILEDB_UINT64},
      {&x_domain[0]},
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
  double label_domain[] = {-1.0, 1.0};
  double label_tile_extent = 2.0;
  {
    tiledb_dimension_label_schema_t* dim_label_schema;
    REQUIRE_TILEDB_OK(tiledb_dimension_label_schema_alloc(
        ctx,
        TILEDB_INCREASING_LABELS,
        TILEDB_UINT64,
        x_domain,
        &x_tile_extent,
        TILEDB_FLOAT64,
        &label_domain[0],
        &label_tile_extent,
        &dim_label_schema));

    REQUIRE_TILEDB_OK(tiledb_array_schema_add_dimension_label(
        ctx, array_schema, 0, "x", dim_label_schema));
    tiledb_dimension_label_schema_free(&dim_label_schema);

    // Check array schema and number of dimension labels.
    REQUIRE_TILEDB_OK(tiledb_array_schema_check(ctx, array_schema));
    auto dim_label_num = array_schema->array_schema_->dim_label_num();
    REQUIRE(dim_label_num == 1);
  }

  // Create array
  auto array_name = create_temporary_array("array_with_label_1", array_schema);
  tiledb_array_schema_free(&array_schema);

  // Input data.
  std::vector<uint64_t> input_index_data{0, 1, 2, 3};
  uint64_t index_data_size{input_index_data.size() * sizeof(uint64_t)};
  std::vector<double> input_label_data{-1.0, 0.0, 0.5, 1.0};
  uint64_t label_data_size{input_label_data.size() * sizeof(double)};
  std::vector<double> input_attr_data{0.5, 1.0, 1.5, 2.0};
  uint64_t attr_data_size{input_attr_data.size() * sizeof(double)};

  {
    // Open array for writing.
    tiledb_array_t* array;
    REQUIRE_TILEDB_OK(tiledb_array_alloc(ctx, array_name.c_str(), &array));
    REQUIRE_TILEDB_OK(tiledb_array_open(ctx, array, TILEDB_WRITE));

    // Create write query.
    tiledb_query_t* query;
    REQUIRE_TILEDB_OK(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));
    REQUIRE_TILEDB_OK(tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED));
    REQUIRE_TILEDB_OK(tiledb_query_set_label_data_buffer(
        ctx, query, "x", input_label_data.data(), &label_data_size));
    REQUIRE_TILEDB_OK(tiledb_query_set_data_buffer(
        ctx, query, "x", input_index_data.data(), &index_data_size));
    REQUIRE_TILEDB_OK(tiledb_query_set_data_buffer(
        ctx, query, "a", input_attr_data.data(), &attr_data_size));

    // Submit write query.
    REQUIRE_TILEDB_OK(tiledb_query_submit(ctx, query));
    tiledb_query_status_t query_status;
    REQUIRE_TILEDB_OK(tiledb_query_get_status(ctx, query, &query_status));
    REQUIRE(query_status == TILEDB_COMPLETED);

    // Clean-up.
    tiledb_query_free(&query);
    tiledb_array_free(&array);
  }

  // Read indexed array
  {
    // Define output data.
    std::vector<double> label_data(4, 0.0);
    label_data_size = label_data.size() * sizeof(double);

    // Open array.
    tiledb_array_t* array;
    REQUIRE_TILEDB_OK(tiledb_array_alloc(
        ctx, "tiledb_test/array_with_label_1/__labels/l0/indexed", &array));
    REQUIRE_TILEDB_OK(tiledb_array_open(ctx, array, TILEDB_READ));

    // Create subarray.
    tiledb_subarray_t* subarray;
    REQUIRE_TILEDB_OK(tiledb_subarray_alloc(ctx, array, &subarray));
    REQUIRE_TILEDB_OK(tiledb_subarray_add_range(
        ctx, subarray, 0, &x_domain[0], &x_domain[1], nullptr));

    // Create query.
    tiledb_query_t* query;
    REQUIRE_TILEDB_OK(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
    REQUIRE_TILEDB_OK(tiledb_query_set_subarray_t(ctx, query, subarray));
    REQUIRE_TILEDB_OK(tiledb_query_set_data_buffer(
        ctx, query, "label", label_data.data(), &label_data_size));

    // Submit query.
    REQUIRE_TILEDB_OK(tiledb_query_submit(ctx, query));
    tiledb_query_status_t query_status;
    REQUIRE_TILEDB_OK(tiledb_query_get_status(ctx, query, &query_status));
    REQUIRE(query_status == TILEDB_COMPLETED);

    // Clean-up.
    tiledb_query_free(&query);
    tiledb_subarray_free(&subarray);
    tiledb_array_free(&array);

    // Check results.
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(label_data[ii] == input_label_data[ii]);
    }
  }

  // Read labelled array
  {
    // Define output data.
    std::vector<double> label_data(4, 0.0);
    label_data_size = label_data.size() * sizeof(double);
    std::vector<uint64_t> index_data(4, 0.0);
    index_data_size = index_data.size() * sizeof(uint64_t);
    // Open array.
    tiledb_array_t* array;
    REQUIRE_TILEDB_OK(tiledb_array_alloc(
        ctx, "tiledb_test/array_with_label_1/__labels/l0/labelled", &array));
    REQUIRE_TILEDB_OK(tiledb_array_open(ctx, array, TILEDB_READ));

    // Create subarray.
    tiledb_subarray_t* subarray;
    REQUIRE_TILEDB_OK(tiledb_subarray_alloc(ctx, array, &subarray));
    REQUIRE_TILEDB_OK(tiledb_subarray_add_range(
        ctx, subarray, 0, &label_domain[0], &label_domain[1], nullptr));

    // Create query.
    tiledb_query_t* query;
    REQUIRE_TILEDB_OK(tiledb_query_alloc(ctx, array, TILEDB_READ, &query));
    REQUIRE_TILEDB_OK(tiledb_query_set_subarray_t(ctx, query, subarray));
    REQUIRE_TILEDB_OK(tiledb_query_set_data_buffer(
        ctx, query, "label", label_data.data(), &label_data_size));
    REQUIRE_TILEDB_OK(tiledb_query_set_data_buffer(
        ctx, query, "index", index_data.data(), &index_data_size));

    // Submit query.
    REQUIRE_TILEDB_OK(tiledb_query_submit(ctx, query));
    tiledb_query_status_t query_status;
    REQUIRE_TILEDB_OK(tiledb_query_get_status(ctx, query, &query_status));
    REQUIRE(query_status == TILEDB_COMPLETED);

    // Clean-up.
    tiledb_query_free(&query);
    tiledb_subarray_free(&subarray);
    tiledb_array_free(&array);

    // Check results.
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(label_data[ii] == input_label_data[ii]);
    }
    for (uint64_t ii{0}; ii < 4; ++ii) {
      CHECK(index_data[ii] == input_index_data[ii]);
    }
  }
}
