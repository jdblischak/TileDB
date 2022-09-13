/**
 * @file test-dimension_labels-read-consistency.cc
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
 * Tests dimension labels only read fragments that exist in both labelled and
 * indexed array.
 */

#include <algorithm>

#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/array_schema/dimension_label_schema.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/label_order.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/query/dimension_label/dimension_label_range_query.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/storage_format/uri/generate_uri.h"
#include "tiledb/type/range/range.h"

using namespace tiledb::sm;
using namespace tiledb::test;

/**
 * Write the entire dimension label.
 *
 * @param label_data Label data for the entire label array.
 * @param enable_indexed_array_write If true, write to the indexed array.
 * @param enable_labelled_array_write If true, write to the lablled array.
 */
template <typename label_data_t, typename index_data_t>
void write_dimension_label(
    tiledb_ctx_t* ctx,
    const URI& uri,
    std::vector<label_data_t> label_data,
    std::vector<index_data_t> index_data) {
  // Open dimension label for writing.
  DimensionLabel dimension_label{uri, ctx->storage_manager()};
  dimension_label.open(
      QueryType::WRITE, EncryptionType::NO_ENCRYPTION, nullptr, 0);

  // Generate single fragment name for queries.
  auto timestamp = dimension_label.indexed_array()->timestamp_end_opened_at();
  auto timestamp2 = dimension_label.labelled_array()->timestamp_end_opened_at();
  REQUIRE(timestamp == timestamp2);
  auto fragment_name = tiledb::storage_format::generate_fragment_name(
      timestamp, constants::format_version);

  // Create label query buffer.
  uint64_t label_data_size{label_data.size() * sizeof(index_data_t)};
  tiledb::sm::QueryBuffer label_data_buffer{
      label_data.data(), nullptr, &label_data_size, nullptr};
  // Write indexed array.
  {
    Query query{
        ctx->storage_manager(), dimension_label.indexed_array(), fragment_name};
    REQUIRE_TILEDB_STATUS_OK(query.set_data_buffer(
        dimension_label.label_attribute()->name(),
        label_data_buffer.buffer_,
        label_data_buffer.buffer_size_,
        true));
    auto st = query.submit();
    INFO(st.to_string());
    REQUIRE_TILEDB_STATUS_OK(st);
    REQUIRE(query.status() == QueryStatus::COMPLETED);
  }

  // Write labelled array.
  {
    Query query{
        ctx->storage_manager(),
        dimension_label.labelled_array(),
        fragment_name};

    // Create index query buffer.
    uint64_t index_data_size{index_data.size() * sizeof(index_data_t)};
    tiledb::sm::QueryBuffer index_data_buffer{
        index_data.data(), nullptr, &index_data_size, nullptr};

    // Create the query.
    REQUIRE_TILEDB_STATUS_OK(query.set_layout(Layout::UNORDERED));
    REQUIRE_TILEDB_STATUS_OK(query.set_data_buffer(
        dimension_label.label_dimension()->name(),
        label_data_buffer.buffer_,
        label_data_buffer.buffer_size_,
        true));
    REQUIRE_TILEDB_STATUS_OK(query.set_data_buffer(
        dimension_label.index_attribute()->name(),
        index_data_buffer.buffer_,
        index_data_buffer.buffer_size_,
        true));
    REQUIRE_TILEDB_STATUS_OK(query.submit());
    REQUIRE(query.status() == QueryStatus::COMPLETED);
  }
}

/**
 * Read the requested range from the dimension label.
 *
 * @param ctx The context to open the dimension label with.
 * @param uri The URI of the dimension label.
 * @param start The starting label value for the read.
 * @param end The ending label value for the read.
 *
 * @returns The index range read from the dimension label.
 */
template <typename label_data_t>
Range read_range(
    tiledb_ctx_t* ctx, const URI& uri, label_data_t start, label_data_t end) {
  // Open the dimension label are read the data.
  DimensionLabel dimension_label{uri, ctx->storage_manager()};
  dimension_label.open(
      QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0);

  // Create vector with label range to query.
  label_data_t input_range[2]{start, end};
  std::vector<Range> label_ranges;
  label_ranges.emplace_back(&input_range[0], 2 * sizeof(uint64_t));

  // Create query and read resulting index range.
  DimensionLabelRangeQuery query{
      &dimension_label, ctx->storage_manager(), label_ranges};
  query.process();
  auto output_index_range = query.index_range();

  // Close the dimension label.
  dimension_label.close();

  // Return the result.
  return output_index_range;
}

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "Read range for ordered labels with fixed-type datatype",
    "[DimensionLabel][Query]") {
  // Create the dimension label. Use Catch2 GENERATE to create sections:
  //    Section 1. Increasing labels
  //    Section 2. Decreasing labels
  uint64_t index_domain[2]{1, 4};
  uint64_t label_domain[2]{0, 400};
  uint64_t index_tile_extent{4};
  uint64_t label_tile_extent{401};
  auto label_order =
      GENERATE(LabelOrder::INCREASING_LABELS, LabelOrder::DECREASING_LABELS);
  auto uri = URI(fullpath("fixed_label"));
  DimensionLabelSchema dim_label_schema{
      label_order,
      tiledb::sm::Datatype::UINT64,
      &index_domain[0],
      &index_tile_extent,
      tiledb::sm::Datatype::UINT64,
      &label_domain[0],
      &label_tile_extent};
  create_dimension_label(uri, *ctx->storage_manager(), dim_label_schema);

  // Write data to the dimension label.
  // Increasing Labels:
  //   Index:  1,  2,  3,  4
  //   Label: 10, 20, 30, 40
  // Decreasing Labels:
  //   Index:  1,  2,  3,  4
  //   Label: 40, 30, 20, 10
  std::vector<uint64_t> input_index_data{1, 2, 3, 4};
  std::vector<uint64_t> input_label_data;
  if (label_order == LabelOrder::INCREASING_LABELS) {
    input_label_data.insert(input_label_data.end(), {10, 20, 30, 40});
  } else {
    input_label_data.insert(input_label_data.end(), {40, 30, 20, 10});
  }
  write_dimension_label(ctx, uri, input_label_data, input_index_data);

  SECTION("exact range result") {
    // Read range.
    const auto& index_range = read_range<uint64_t>(ctx, uri, 20, 30);

    // Check result.
    REQUIRE(!index_range.empty());
    auto result_data = static_cast<const uint64_t*>(index_range.data());
    CHECK(result_data[0] == 2);
    CHECK(result_data[1] == 3);
  }

  SECTION("inexact range result") {
    // Read range.
    const auto& index_range = read_range<uint64_t>(ctx, uri, 12, 35);

    // Check result.
    REQUIRE(!index_range.empty());
    auto result_data = static_cast<const uint64_t*>(index_range.data());
    CHECK(result_data[0] == 2);
    CHECK(result_data[1] == 3);
  }

  SECTION("exact singleton result") {
    // Read range.
    const auto& index_range = read_range<uint64_t>(ctx, uri, 20, 20);

    // Check result.
    REQUIRE(!index_range.empty());
    auto result_data = static_cast<const uint64_t*>(index_range.data());
    uint64_t expected_result =
        label_order == LabelOrder::INCREASING_LABELS ? 2 : 3;
    CHECK(result_data[0] == expected_result);
    CHECK(result_data[1] == expected_result);
  }

  SECTION("inexact singleton result") {
    // Read range.
    const auto& index_range = read_range<uint64_t>(ctx, uri, 12, 25);

    // Check result.
    REQUIRE(!index_range.empty());
    auto result_data = static_cast<const uint64_t*>(index_range.data());
    uint64_t expected_result =
        label_order == LabelOrder::INCREASING_LABELS ? 2 : 3;
    CHECK(result_data[0] == expected_result);
    CHECK(result_data[1] == expected_result);
  }

  SECTION("full range input") {
    // Read range.
    const auto& index_range =
        read_range<uint64_t>(ctx, uri, label_domain[0], label_domain[1]);

    // Check result.
    REQUIRE(!index_range.empty());
    auto result_data = static_cast<const uint64_t*>(index_range.data());
    CHECK(result_data[0] == 1);
    CHECK(result_data[1] == 4);
  }

  SECTION("empty range result") {
    // Read range.
    const auto& index_range = read_range<uint64_t>(ctx, uri, 12, 18);

    // Check result.
    if (!index_range.empty()) {
      auto result_data = static_cast<const uint64_t*>(index_range.data());
      INFO("index_range[0] = " + std::to_string(result_data[0]));
      INFO("index_range[1] = " + std::to_string(result_data[1]));
      REQUIRE(index_range.empty());
    }
    REQUIRE(index_range.empty());
  }
}
