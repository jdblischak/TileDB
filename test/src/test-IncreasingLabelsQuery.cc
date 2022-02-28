/**
 * @file test-LabelledQuery.cc
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
 * Tests dimension label queries.
 */

#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#include "tiledb.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/dimension_label/dimension_label.h"
#include "tiledb/sm/dimension_label/ordered_labels_read_query.h"
#include "tiledb/sm/dimension_label/ordered_labels_write_query.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/label_order.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <catch.hpp>
#include <iostream>
#include <string>
#include <unordered_map>

using namespace tiledb::sm;
using namespace tiledb::test;

class DimensionLabelExample1 : public TemporaryDirectoryFixture {
 public:
  using index_data_t = uint64_t;
  using label_data_t = int64_t;

  DimensionLabelExample1()
      : uri_{URI(fullpath("l0"))}
      , index_data_(ncells_, 0)
      , label_data_(ncells_, 0) {
    int64_t index_domain[2]{1, 16};
    int64_t index_tile_extent{16};
    int64_t label_domain[2]{-16, 16};
    int64_t label_tile_extent{33};
    tiledb::sm::DimensionLabelSchema dim_label_schema{
        tiledb::sm::LabelOrder::INCREASING_LABELS,
        tiledb::sm::Datatype::UINT64,
        index_domain,
        &index_tile_extent,
        tiledb::sm::Datatype::INT64,
        label_domain,
        &label_tile_extent};
    create_dimension_label(uri_, *ctx->storage_manager(), dim_label_schema);
  }

  const URI& example1_uri() const {
    return uri_;
  }

  std::string indexed_array_path() const {
    return uri_.to_string() + "/indexed";
  }

  index_data_t index_value(uint64_t idx) {
    return index_data_[idx];
  }

  std::string labelled_array_path() const {
    return uri_.to_string() + "/labelled";
  }

  label_data_t label_value(uint64_t idx) {
    return label_data_[idx];
  }

  Range read_range(const std::vector<int64_t> label_range_data) {
    // Open the dimension label.
    auto dimension_label =
        make_shared<DimensionLabel>(HERE(), uri_, ctx->storage_manager());
    dimension_label->open(
        QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0);

    // Create label ranges  // Create ranges to be fed into dimension label
    RangeSetAndSuperset label_ranges{
        dimension_label->label_dimension()->type(),
        dimension_label->label_dimension()->domain(),
        false,
        true};
    Range range{label_range_data.data(), 2 * sizeof(int64_t)};
    auto&& [status, msg] = label_ranges.add_range(range, false);
    REQUIRE_TILEDB_STATUS_OK(status);

    // Create, submit, and finalize query.
    DimensionLabelRangeQuery query{
        dimension_label.get(), ctx->storage_manager(), label_ranges};
    query.submit();
    query.finalize();

    // Return query result.
    auto index_range = query.index_range();

    // Close and clean-up the array
    dimension_label->close();

    return index_range;
  }

  void write_sample_data() {
    for (uint32_t ii{0}; ii < 16; ++ii) {
      label_data_[ii] = static_cast<int64_t>(2 * ii) - 15;
      index_data_[ii] = (ii + 1);
    }
    // Data for the dimension label.
    uint64_t index_data_size{16 * sizeof(uint64_t)};
    uint64_t label_data_size{16 * sizeof(int64_t)};

    // Open the dimension label.
    auto dimension_label =
        make_shared<DimensionLabel>(HERE(), uri_, ctx->storage_manager());
    dimension_label->open(
        QueryType::WRITE, EncryptionType::NO_ENCRYPTION, nullptr, 0);

    // Create ranges to be fed into dimension label
    RangeSetAndSuperset label_range{
        dimension_label->label_dimension()->type(),
        dimension_label->label_dimension()->domain(),
        false,
        true};
    RangeSetAndSuperset index_range{
        dimension_label->label_dimension()->type(),
        dimension_label->label_dimension()->domain(),
        true,  // set input index range to full range
        true};

    // Create buffers
    tiledb::sm::QueryBuffer label_buffer{
        label_data_.data(), nullptr, &label_data_size, nullptr};
    tiledb::sm::QueryBuffer index_buffer{
        index_data_.data(), nullptr, &index_data_size, nullptr};

    // Create dimension label data query.
    OrderedLabelsWriteQuery query{dimension_label,
                                  ctx->storage_manager(),
                                  label_range,
                                  index_range,
                                  label_buffer,
                                  index_buffer};

    // Submit the query.
    query.submit();
    REQUIRE(query.status() == QueryStatus::COMPLETED);

    // Close and clean-up the array
    dimension_label->close();
  }

 private:
  static const uint64_t ncells_ = 16;
  URI uri_;
  std::vector<index_data_t> index_data_;
  std::vector<label_data_t> label_data_;
};

TEST_CASE_METHOD(
    DimensionLabelExample1,
    "Write to an increasing dimension label",
    "[Query][1d][DimensionLabel]") {
  // Write to the dimension label.
  write_sample_data();

  Status status{Status::Ok()};
  uint64_t index_data_size{16 * sizeof(uint64_t)};
  uint64_t label_data_size{16 * sizeof(int64_t)};

  // Check index data from the dimension label.
  {
    const std::string indexed_array_name = indexed_array_path();
    // Allocate and open the indexed array.
    tiledb_array_t* indexed_array;
    REQUIRE_TILEDB_OK(
        tiledb_array_alloc(ctx, indexed_array_name.c_str(), &indexed_array));
    REQUIRE_TILEDB_OK(tiledb_array_open(ctx, indexed_array, TILEDB_READ));
    // Read data from array.
    std::vector<int64_t> output_label_data(16, 0);
    tiledb::test::SubarrayRanges<uint64_t> ranges{{1, 16}};
    tiledb::test::QueryBuffers buffers;
    buffers["label"] = tiledb::test::QueryBuffer(
        {&output_label_data[0], label_data_size, nullptr, 0});
    read_array(ctx, indexed_array, ranges, TILEDB_ROW_MAJOR, buffers);
    // Check data is as expected.
    for (uint32_t ii{0}; ii < 16; ++ii) {
      CHECK(output_label_data[ii] == label_value(ii));
    }
    // Close array.
    REQUIRE_TILEDB_OK(tiledb_array_close(ctx, indexed_array));
    tiledb_array_free(&indexed_array);
  }
  {
    const std::string labelled_array_name = labelled_array_path();
    // Open array.
    tiledb_array_t* labelled_array;
    REQUIRE_TILEDB_OK(
        tiledb_array_alloc(ctx, labelled_array_name.c_str(), &labelled_array));
    REQUIRE_TILEDB_OK(tiledb_array_open(ctx, labelled_array, TILEDB_READ));
    // Read array.
    std::vector<int64_t> output_label_data(16, 0);
    std::vector<uint64_t> output_index_data(16, 0);
    tiledb::test::SubarrayRanges<int64_t> ranges{{-15, 15}};
    tiledb::test::QueryBuffers buffers;
    buffers["label"] = tiledb::test::QueryBuffer(
        {&output_label_data[0], label_data_size, nullptr, 0});
    buffers["index"] = tiledb::test::QueryBuffer(
        {&output_index_data[0], index_data_size, nullptr, 0});
    read_array(ctx, labelled_array, ranges, TILEDB_UNORDERED, buffers);
    // Check data is as expected.
    for (uint32_t ii{0}; ii < 16; ++ii) {
      CHECK(output_label_data[ii] == label_value(ii));
    }
    for (uint32_t ii{0}; ii < 16; ++ii) {
      CHECK(output_index_data[ii] == index_value(ii));
    }
    // Close array.
    REQUIRE_TILEDB_OK(tiledb_array_close(ctx, labelled_array));
    tiledb_array_free(&labelled_array);
  }
}

TEST_CASE_METHOD(
    DimensionLabelExample1,
    "Read an exact range from an increasing dimension label",
    "[Query][1d][DimensionLabel]") {
  write_sample_data();

  // Read the range from the range reader.
  std::vector<int64_t> label_range_data{-11, -3};
  auto index_range = read_range(label_range_data);

  // Check results.
  auto range_data = static_cast<const uint64_t*>(index_range.data());
  CHECK(range_data[0] == 3);
  CHECK(range_data[1] == 7);
}

TEST_CASE_METHOD(
    DimensionLabelExample1,
    "Read an inexact range from an increasing dimension label",
    "[Query][1d][DimensionLabel]") {
  write_sample_data();

  // Read the range from the range reader.
  std::vector<int64_t> label_range_data{-12, -2};
  auto index_range = read_range(label_range_data);

  // Check results.
  auto range_data = static_cast<const uint64_t*>(index_range.data());
  CHECK(range_data[0] == 3);
  CHECK(range_data[1] == 7);
}

TEST_CASE_METHOD(
    DimensionLabelExample1,
    "Read label data from an index range for an increasing dimension label",
    "[Query][1d][DimensionLabel]") {
  write_sample_data();

  const auto& dim_label_uri = example1_uri();

  // Open the dimension label.
  auto dimension_label = make_shared<DimensionLabel>(
      HERE(), dim_label_uri, ctx->storage_manager());
  dimension_label->open(
      QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0);

  // Create ranges to be fed into dimension label
  RangeSetAndSuperset label_ranges{dimension_label->label_dimension()->type(),
                                   dimension_label->label_dimension()->domain(),
                                   false,
                                   true};
  RangeSetAndSuperset index_ranges{dimension_label->label_dimension()->type(),
                                   dimension_label->label_dimension()->domain(),
                                   false,
                                   true};
  std::vector<uint64_t> input_data{9, 12};
  Range range{input_data.data(), 2 * sizeof(uint64_t)};
  auto&& [status, msg] = index_ranges.add_range(range, false);
  REQUIRE_TILEDB_STATUS_OK(status);

  // Create buffers for the dimension label query.
  std::vector<int64_t> output_label_data(16);
  uint64_t label_size{output_label_data.size() * sizeof(int64_t)};
  tiledb::sm::QueryBuffer label_data_buffer{
      &output_label_data[0], nullptr, &label_size, nullptr};

  // Create dimension label data query.
  OrderedLabelsReadQuery query{dimension_label,
                               ctx->storage_manager(),
                               label_ranges,
                               index_ranges,
                               label_data_buffer};

  // Submit label query and check for success.
  query.submit();

  // Submit main query and check for success.
  auto query_status = query.status();
  INFO("Query status label data: " + query_status_str(query_status));

  // Close and clean-up the array
  dimension_label->close();

  // Check results
  std::vector<int64_t> expected_label{1, 3, 5, 7};
  expected_label.resize(16, 0);
  for (size_t ii{0}; ii < 16; ++ii) {
    CHECK(output_label_data[ii] == expected_label[ii]);
  }
}

TEST_CASE_METHOD(
    DimensionLabelExample1,
    "Read label data from an label range for an increasing dimension label",
    "[Query][1d][DimensionLabel]") {
  write_sample_data();

  const auto& dim_label_uri = example1_uri();

  // Open the dimension label.
  auto dimension_label = make_shared<DimensionLabel>(
      HERE(), dim_label_uri, ctx->storage_manager());
  dimension_label->open(
      QueryType::READ, EncryptionType::NO_ENCRYPTION, nullptr, 0);

  // Create ranges to be fed into dimension label
  RangeSetAndSuperset label_ranges{dimension_label->label_dimension()->type(),
                                   dimension_label->label_dimension()->domain(),
                                   false,
                                   true};
  RangeSetAndSuperset index_ranges{dimension_label->label_dimension()->type(),
                                   dimension_label->label_dimension()->domain(),
                                   false,
                                   true};
  std::vector<int64_t> input_data{-12, -2};
  Range range{input_data.data(), 2 * sizeof(int64_t)};
  auto&& [status, msg] = label_ranges.add_range(range, false);
  REQUIRE_TILEDB_STATUS_OK(status);

  // Create buffers for the dimension label query.
  std::vector<int64_t> output_label_data(16);
  uint64_t label_size{output_label_data.size() * sizeof(int64_t)};
  tiledb::sm::QueryBuffer label_data_buffer{
      &output_label_data[0], nullptr, &label_size, nullptr};

  // Create dimension label data query.
  OrderedLabelsReadQuery query{dimension_label,
                               ctx->storage_manager(),
                               label_ranges,
                               index_ranges,
                               label_data_buffer};

  // Submit label query and check for success.
  query.submit();

  // Submit main query and check for success.
  auto query_status = query.status();
  INFO("Query status label data: " + query_status_str(query_status));

  // Close and clean-up the array
  dimension_label->close();

  // Check results
  std::vector<int64_t> expected_label{-11, -9, -7, -5, -3};
  expected_label.resize(16, 0);
  for (size_t ii{0}; ii < 16; ++ii) {
    CHECK(output_label_data[ii] == expected_label[ii]);
  }
}
