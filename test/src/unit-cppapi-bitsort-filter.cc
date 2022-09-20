/**
 * @file   unit-cppapi-bitsort-filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests the C++ API for bitsort filter related functions.
 */

#include <iostream>
#include <random>
#include <vector>

#include "catch.hpp"
#include "tiledb/common/common.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

std::string bitsort_array_name = "cpp_unit_array";
int bitsort_dim_hi = 10;

template <typename T>
void bitsort_filter_api_test(Context& ctx) {
  Domain domain(ctx);
  auto d1 = Dimension::create<int>(ctx, "x", {{1, bitsort_dim_hi}}, 4);
  auto d2 = Dimension::create<int>(ctx, "y", {{1, bitsort_dim_hi}}, 4);
  auto d3 = Dimension::create<int>(ctx, "z", {{1, bitsort_dim_hi}}, 4);
  domain.add_dimensions(d1, d2, d3);

  Filter f(ctx, TILEDB_FILTER_BITSORT);

  FilterList filters(ctx);
  filters.add_filter(f);

  auto a = Attribute::create<T>(ctx, "a");
  a.set_filter_list(filters);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a);
  Array::create(bitsort_array_name, schema);

  // Setting up the random number generator for the bitsort filter testing.
  std::mt19937_64 gen(0xADA65ED6);
  std::uniform_int_distribution<int64_t> dis(
      std::numeric_limits<T>::min(), std::numeric_limits<T>::max());

  std::cout << "Writing the following data, in this order...\n";

  std::vector<int> x_dims;
  std::vector<int> y_dims;
  std::vector<int> z_dims;
  std::vector<T> a_write;
  std::vector<T> expected_a;
  for (int x_tile = 0; x_tile < bitsort_dim_hi; x_tile += 4) {
    for (int y_tile = 0; y_tile < bitsort_dim_hi; y_tile += 4) {
      for (int z_tile = 0; z_tile < bitsort_dim_hi; z_tile += 4) {
        for (int x = x_tile; x < x_tile + 4; ++x) {
          if (x >= bitsort_dim_hi) {
            break;
          }
          for (int y = y_tile; y < y_tile + 4; ++y) {
            if (y >= bitsort_dim_hi) {
              break;
            }
            for (int z = z_tile; z < z_tile + 4; ++z) {
              if (z >= bitsort_dim_hi) {
                break;
              }

              x_dims.push_back(x + 1);
              y_dims.push_back(y + 1);
              z_dims.push_back(z + 1);

              T f = static_cast<T>(dis(gen));
              a_write.push_back(f);
              expected_a.push_back(f);
            }
          }
        }
      }
    }
  }

  tiledb_layout_t layout_type = TILEDB_UNORDERED; // TODO: change

  Array array_w(ctx, bitsort_array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(layout_type).set_data_buffer("a", a_write);

  query_w.set_data_buffer("x", x_dims).set_data_buffer("y", y_dims).set_data_buffer("z", z_dims);

  query_w.submit();
  query_w.finalize();
  array_w.close();

  // Open and read the entire array.
  std::vector<T> a_data_read(bitsort_dim_hi * bitsort_dim_hi * bitsort_dim_hi, 0);
  Array array_r(ctx, bitsort_array_name, TILEDB_READ);
  Query query_r(ctx, array_r);
  query_r.set_layout(TILEDB_UNORDERED).set_data_buffer("a", a_data_read);
  query_r.submit();

  // Check for results.
  size_t total_num_elements = static_cast<size_t>(bitsort_dim_hi * bitsort_dim_hi * bitsort_dim_hi);
  auto table = query_r.result_buffer_elements();
  REQUIRE(table.size() == 1);
  REQUIRE(table["a"].first == 0);
  REQUIRE(table["a"].second == total_num_elements);

  std::cout << "Reading the following data, in this order...\n";
  for (size_t i = 0; i < total_num_elements; ++i) {
    CHECK(a_data_read[i] == expected_a[i]);
  }

  query_r.finalize();
  array_r.close();
}

TEMPLATE_TEST_CASE(
    "C++ API: Bitsort Filter list on array",
    "[cppapi][filter][bitsort]",
    int8_t,
    int16_t,
    int32_t,
    int64_t) {

  // Setup.
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(bitsort_array_name))
    vfs.remove_dir(bitsort_array_name);

  bitsort_filter_api_test<TestType>(ctx);

  // Teardown.
  if (vfs.is_dir(bitsort_array_name))
    vfs.remove_dir(bitsort_array_name);
}
