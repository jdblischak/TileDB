/**
 * @file unit-capi-smoke-test.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2024 TileDB, Inc.
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
 * Smoke test that performs basic operations on the matrix of posssible
 * array schemas.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/temporary_local_directory.h"
#include "tiledb/api/c_api/vfs/vfs_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/encryption_type.h"

#include <iostream>
#include <optional>
#include <vector>

using namespace std;
using namespace tiledb::sm;
using namespace tiledb::test;

const char* combination_op_str(tiledb_query_condition_combination_op_t op) {
  switch (op) {
    case TILEDB_AND:
      return "AND";
    case TILEDB_OR:
      return "OR";
    default:
      return nullptr;
  }
}

class SmokeTestFx {
 public:
  /**
   * Wraps data to build a dimension with the C-API.
   */
  struct test_dim_t {
    /** Value constructor. */
    test_dim_t(
        const string& name,
        const tiledb_datatype_t type,
        const void* const domain,
        const uint64_t tile_extent)
        : name_(name)
        , type_(type)
        , domain_(domain)
        , tile_extent_(tile_extent) {
    }

    /** Dimension name. */
    string name_;

    /** Dimension data type. */
    tiledb_datatype_t type_;

    /** Dimension domain range. */
    const void* domain_;

    /** Tile extent size. */
    uint64_t tile_extent_;
  };

  /**
   * Wraps data to build an attribute with the C-API.
   */
  struct test_attr_t {
    /** Value constructor. */
    test_attr_t(
        const string& name,
        const tiledb_datatype_t type,
        const uint32_t cell_val_num,
        const bool nullable)
        : name_(name)
        , type_(type)
        , cell_val_num_(cell_val_num)
        , nullable_(nullable) {
    }

    /** Attribute name. */
    string name_;

    /** Attribute data type. */
    tiledb_datatype_t type_;

    /** Values per cell. */
    uint32_t cell_val_num_;

    /** True if attribute is nullable.  */
    bool nullable_;
  };

  /**
   * Wraps data to build a query condition with the C-API.
   */
  struct test_query_condition_t {
    /** Value constructor. */
    test_query_condition_t(
        const string& name, const tiledb_query_condition_op_t op)
        : name_(name)
        , op_(op) {
    }

    /** Destructor. */
    virtual ~test_query_condition_t() = default;

    /** Returns the value to compare against. */
    virtual const void* value() const = 0;

    /** Returns the byte size of the value to compare against. */
    virtual uint64_t value_size() const = 0;

    /** Returns true if (`lhs` `op_` `value_`). */
    virtual bool cmp(const void* lhs) const = 0;

    /** The name of the attribute to compare against. */
    const string name_;

    /** The relational operator. */
    const tiledb_query_condition_op_t op_;
  };

  /** Returns a shared_ptr for the template-typed query condition. */
  template <typename T>
  shared_ptr<test_query_condition_t> make_condition(
      const string& name, const tiledb_query_condition_op_t op, const T value) {
    return shared_ptr<test_query_condition_t>(
        new test_query_condition_t_impl<T>(name, op, value));
  }

  /** The template-typed test_query_condition_t implementation. */
  template <typename T>
  struct test_query_condition_t_impl;

  struct test_query_buffer_t {
    /** Value constructor. */
    test_query_buffer_t(
        const string& name,
        void* const buffer,
        uint64_t* const buffer_size,
        void* const buffer_offset,
        uint64_t* const buffer_offset_size,
        uint8_t* const buffer_validity,
        uint64_t* const buffer_validity_size)
        : name_(name)
        , buffer_(buffer)
        , buffer_size_(buffer_size)
        , buffer_offset_(buffer_offset)
        , buffer_offset_size_(buffer_offset_size)
        , buffer_validity_(buffer_validity)
        , buffer_validity_size_(buffer_validity_size) {
    }

    string name_;
    void* buffer_;
    uint64_t* buffer_size_;
    void* buffer_offset_;
    uint64_t* buffer_offset_size_;
    uint8_t* buffer_validity_;
    uint64_t* buffer_validity_size_;
  };

  /** Constructor. */
  SmokeTestFx();

  /** Destructor. */
  ~SmokeTestFx();

  /**
   * Create, write and read attributes to an array.
   *
   * @param test_attrs The attributes to test.
   * @param test_query_conditions_vec The different query conditions for read.
   * @param test_dims The dimensions to test.
   * @param array_type The type of the array (dense/sparse).
   * @param cell_order The cell order of the array.
   * @param tile_order The tile order of the array.
   * @param write_order The write layout.
   * @param encryption_type The encryption type.
   */
  void smoke_test(
      const vector<test_attr_t>& test_attrs,
      const vector<vector<shared_ptr<test_query_condition_t>>>&
          query_conditions_vec,
      const vector<test_dim_t>& test_dims,
      tiledb_array_type_t array_type,
      tiledb_layout_t cell_order,
      tiledb_layout_t tile_order,
      tiledb_layout_t write_order,
      tiledb_encryption_type_t encryption_type);

 private:
  friend struct Instance;

  /** The C-API context object. */
  tiledb_ctx_t* ctx_;

  /** The C-API VFS object. */
  tiledb_vfs_t* vfs_;

  /** The unique local directory object. */
  TemporaryLocalDirectory temp_dir_;

  /** The encryption key. */
  const char encryption_key_[33] = "unittestunittestunittestunittest";

  /** The name of the array. */
  const string array_name_ = "smoke_test_array";

  /**
   * Compute the full array path given an array name.
   *
   * @param array_name The array name.
   * @return The full array path.
   */
  const string array_path(const string& array_name);

  /**
   * Creates a TileDB array.
   *
   * @param array_type The type of the array (dense/sparse).
   * @param test_dims The dimensions in the array.
   * @param test_attr The attribute in the array.
   * @param cell_order The cell order of the array.
   * @param tile_order The tile order of the array.
   * @param encryption_type The encryption type of the array.
   *
   */
  void create_array(
      tiledb_array_type_t array_type,
      const vector<test_dim_t>& test_dims,
      const vector<test_attr_t>& test_attrs,
      tiledb_layout_t cell_order,
      tiledb_layout_t tile_order,
      tiledb_encryption_type_t encryption_type);

  /**
   * Creates and executes a single write query.
   *
   * @param test_query_buffers The query buffers to write.
   * @param layout The write layout.
   * @param encryption_type The encryption type of the array.
   */
  void write(
      const vector<test_query_buffer_t>& test_query_buffers,
      tiledb_layout_t layout,
      tiledb_encryption_type_t encryption_type);

  /**
   * Creates and executes a single read query.
   *
   * @param test_query_conditions The attribute conditions to filter on.
   * @param test_query_buffers The query buffers to read.
   * @param subarray The subarray to read.
   * @param read_order The read layout.
   * @param encryption_type The encryption type of the array.
   */
  void read(
      const vector<shared_ptr<test_query_condition_t>>& test_query_conditions,
      const vector<test_query_buffer_t>& test_query_buffers,
      const void* subarray,
      tiledb_layout_t read_order,
      tiledb_encryption_type_t encryption_type,
      tiledb_query_condition_combination_op_t combination_op);
};

/** The string template-typed test_query_condition_t implementation. */
template <>
struct SmokeTestFx::test_query_condition_t_impl<const char*>
    : public SmokeTestFx::test_query_condition_t {
  test_query_condition_t_impl(
      const string& name,
      const tiledb_query_condition_op_t op,
      const char* value)
      : test_query_condition_t(name, op)
      , value_(value)
      , value_size_(strlen(value_)) {
  }

  const void* value() const {
    return value_;
  }

  uint64_t value_size() const {
    return value_size_;
  }

  bool cmp(const void* lhs) const {
    switch (op_) {
      case TILEDB_LT:
        return string(static_cast<const char*>(lhs), value_size_) <
               string(value_, value_size_);
      case TILEDB_LE:
        return string(static_cast<const char*>(lhs), value_size_) <=
               string(value_, value_size_);
      case TILEDB_GT:
        return string(static_cast<const char*>(lhs), value_size_) >
               string(value_, value_size_);
      case TILEDB_GE:
        return string(static_cast<const char*>(lhs), value_size_) >=
               string(value_, value_size_);
      case TILEDB_EQ:
        return string(static_cast<const char*>(lhs), value_size_) ==
               string(value_, value_size_);
      case TILEDB_NE:
        return string(static_cast<const char*>(lhs), value_size_) !=
               string(value_, value_size_);
      default:
        break;
    }

    REQUIRE(false);
    return false;
  }

  const char* const value_;
  const uint64_t value_size_;
};

/** The generic template-typed test_query_condition_t implementation. */
template <typename T>
struct SmokeTestFx::test_query_condition_t_impl
    : public SmokeTestFx::test_query_condition_t {
  test_query_condition_t_impl(
      const string& name, const tiledb_query_condition_op_t op, const T value)
      : test_query_condition_t(name, op)
      , value_(value) {
  }

  const void* value() const {
    return static_cast<const void*>(&value_);
  }

  uint64_t value_size() const {
    return static_cast<uint64_t>(sizeof(T));
  }

  bool cmp(const void* lhs) const {
    switch (op_) {
      case TILEDB_LT:
        return *static_cast<const T*>(lhs) < value_;
      case TILEDB_LE:
        return *static_cast<const T*>(lhs) <= value_;
      case TILEDB_GT:
        return *static_cast<const T*>(lhs) > value_;
      case TILEDB_GE:
        return *static_cast<const T*>(lhs) >= value_;
      case TILEDB_EQ:
        return *static_cast<const T*>(lhs) == value_;
      case TILEDB_NE:
        return *static_cast<const T*>(lhs) != value_;
      default:
        break;
    }

    REQUIRE(false);
    return false;
  }

  const T value_;
};

SmokeTestFx::SmokeTestFx() {
  // Create a config.
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  throw_if_setup_failed(tiledb_config_alloc(&config, &error));
  throw_if_setup_failed(error == nullptr);

  // Create the context.
  throw_if_setup_failed(tiledb_ctx_alloc(config, &ctx_));
  throw_if_setup_failed(ctx_ != nullptr);

  // Create the VFS.
  throw_if_setup_failed(tiledb_vfs_alloc(ctx_, config, &vfs_));
  throw_if_setup_failed(vfs_ != nullptr);
  tiledb_config_free(&config);
}

SmokeTestFx::~SmokeTestFx() {
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

const string SmokeTestFx::array_path(const string& array_name) {
  return temp_dir_.path() + array_name;
}

void SmokeTestFx::create_array(
    tiledb_array_type_t array_type,
    const vector<test_dim_t>& test_dims,
    const vector<test_attr_t>& test_attrs,
    tiledb_layout_t cell_order,
    tiledb_layout_t tile_order,
    tiledb_encryption_type_t encryption_type) {
  // Create the dimensions.
  vector<tiledb_dimension_t*> dims;
  dims.reserve(test_dims.size());
  for (const auto& test_dim : test_dims) {
    tiledb_dimension_t* dim;
    const int rc = tiledb_dimension_alloc(
        ctx_,
        test_dim.name_.c_str(),
        test_dim.type_,
        test_dim.domain_,
        &test_dim.tile_extent_,
        &dim);
    REQUIRE(rc == TILEDB_OK);
    dims.emplace_back(dim);
  }

  // Create the domain.
  tiledb_domain_t* domain;
  int rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  for (const auto& dim : dims) {
    rc = tiledb_domain_add_dimension(ctx_, domain, dim);
    REQUIRE(rc == TILEDB_OK);
  }

  // Create attributes
  vector<tiledb_attribute_t*> attrs;
  attrs.reserve(test_attrs.size());
  for (const auto& test_attr : test_attrs) {
    tiledb_attribute_t* attr;
    rc = tiledb_attribute_alloc(
        ctx_, test_attr.name_.c_str(), test_attr.type_, &attr);
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_attribute_set_cell_val_num(ctx_, attr, test_attr.cell_val_num_);
    REQUIRE(rc == TILEDB_OK);

    if (test_attr.nullable_) {
      rc = tiledb_attribute_set_nullable(ctx_, attr, 1);
      REQUIRE(rc == TILEDB_OK);
    }

    attrs.emplace_back(attr);
  }

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, array_type, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, cell_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, tile_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  for (const auto& attr : attrs) {
    rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr);
    REQUIRE(rc == TILEDB_OK);
  }
  if (array_type != TILEDB_DENSE) {
    rc = tiledb_array_schema_set_allows_dups(ctx_, array_schema, true);
  }

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create the array with or without encryption
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_ctx_free(&ctx_);
    tiledb_config_t* config;
    tiledb_error_t* error = nullptr;
    rc = tiledb_config_alloc(&config, &error);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(error == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    rc = tiledb_config_set(
        config, "sm.encryption_type", encryption_type_string.c_str(), &error);
    REQUIRE(error == nullptr);
    rc =
        tiledb_config_set(config, "sm.encryption_key", encryption_key_, &error);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(error == nullptr);
    REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
    tiledb_config_free(&config);
  }
  rc = tiledb_array_create(ctx_, array_path(array_name_).c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Free attributes.
  for (auto& attr : attrs) {
    tiledb_attribute_free(&attr);
  }

  // Free dimensions
  for (auto& dim : dims) {
    tiledb_dimension_free(&dim);
  }

  // Free the domain
  tiledb_domain_free(&domain);

  // Free the array schema
  tiledb_array_schema_free(&array_schema);
}

void SmokeTestFx::write(
    const vector<test_query_buffer_t>& test_query_buffers,
    tiledb_layout_t layout,
    tiledb_encryption_type_t encryption_type) {
  // Open the array for writing (with or without encryption).
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_path(array_name_).c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Create the write query.
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);

  // Set the query layout.
  rc = tiledb_query_set_layout(ctx_, query, layout);
  REQUIRE(rc == TILEDB_OK);

  // Set the query buffers.
  for (const auto& test_query_buffer : test_query_buffers) {
    if (test_query_buffer.buffer_validity_size_ == nullptr) {
      if (test_query_buffer.buffer_offset_ == nullptr) {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
      } else {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_offsets_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            static_cast<uint64_t*>(test_query_buffer.buffer_offset_),
            test_query_buffer.buffer_offset_size_);
        REQUIRE(rc == TILEDB_OK);
      }
    } else {
      if (test_query_buffer.buffer_offset_ == nullptr) {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_validity_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_validity_,
            test_query_buffer.buffer_validity_size_);
        REQUIRE(rc == TILEDB_OK);
      } else {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_offset_,
            test_query_buffer.buffer_offset_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_offsets_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            static_cast<uint64_t*>(test_query_buffer.buffer_),
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_validity_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_validity_,
            test_query_buffer.buffer_validity_size_);
        REQUIRE(rc == TILEDB_OK);
      }
    }
  }

  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SmokeTestFx::read(
    const vector<shared_ptr<test_query_condition_t>>& test_query_conditions,
    const vector<test_query_buffer_t>& test_query_buffers,
    const void* subarray,
    tiledb_layout_t read_order,
    tiledb_encryption_type_t encryption_type,
    tiledb_query_condition_combination_op_t combination_op) {
  // Open the array for reading (with or without encryption).
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_path(array_name_).c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Create the read query.
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);

  // Set the layout.
  rc = tiledb_query_set_layout(ctx_, query, read_order);
  REQUIRE(rc == TILEDB_OK);

  // Set the query buffers.
  for (size_t i = 0; i < test_query_buffers.size(); ++i) {
    const test_query_buffer_t& test_query_buffer = test_query_buffers[i];
    if (test_query_buffer.buffer_validity_size_ == nullptr) {
      if (test_query_buffer.buffer_offset_ == nullptr) {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
      } else {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_offsets_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            static_cast<uint64_t*>(test_query_buffer.buffer_offset_),
            test_query_buffer.buffer_offset_size_);
        REQUIRE(rc == TILEDB_OK);
      }
    } else {
      if (test_query_buffer.buffer_offset_ == nullptr) {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_,
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_validity_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_validity_,
            test_query_buffer.buffer_validity_size_);
        REQUIRE(rc == TILEDB_OK);
      } else {
        rc = tiledb_query_set_data_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_offset_,
            test_query_buffer.buffer_offset_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_offsets_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            static_cast<uint64_t*>(test_query_buffer.buffer_),
            test_query_buffer.buffer_size_);
        REQUIRE(rc == TILEDB_OK);
        rc = tiledb_query_set_validity_buffer(
            ctx_,
            query,
            test_query_buffer.name_.c_str(),
            test_query_buffer.buffer_validity_,
            test_query_buffer.buffer_validity_size_);
        REQUIRE(rc == TILEDB_OK);
      }
    }
  }

  // Set the subarray to read.
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  // Create the attribute condition objects.
  tiledb_query_condition_t* combined_query_condition = nullptr;
  for (size_t i = 0; i < test_query_conditions.size(); ++i) {
    tiledb_query_condition_t* query_condition;
    rc = tiledb_query_condition_alloc(ctx_, &query_condition);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_condition_init(
        ctx_,
        query_condition,
        test_query_conditions[i]->name_.c_str(),
        test_query_conditions[i]->value(),
        test_query_conditions[i]->value_size(),
        test_query_conditions[i]->op_);
    REQUIRE(rc == TILEDB_OK);

    if (i == 0) {
      combined_query_condition = query_condition;
    } else {
      tiledb_query_condition_t* tmp_query_condition;
      rc = tiledb_query_condition_combine(
          ctx_,
          combined_query_condition,
          query_condition,
          combination_op,
          &tmp_query_condition);
      REQUIRE(rc == TILEDB_OK);
      tiledb_query_condition_free(&combined_query_condition);
      tiledb_query_condition_free(&query_condition);
      combined_query_condition = tmp_query_condition;
    }
  }

  // Set the query condition.
  if (combined_query_condition != nullptr) {
    rc = tiledb_query_set_condition(ctx_, query, combined_query_condition);
    REQUIRE(rc == TILEDB_OK);
  }

  // Submit the query.
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Check query status.
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize the query, a no-op for non-global writes.
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_condition_free(&combined_query_condition);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

struct Instance {
  Instance(
      SmokeTestFx& fx,
      const vector<SmokeTestFx::test_attr_t>& test_attrs,
      const vector<SmokeTestFx::test_dim_t>& test_dims,
      tiledb_array_type_t array_type,
      tiledb_layout_t cell_order,
      tiledb_layout_t tile_order,
      tiledb_layout_t write_order,
      tiledb_encryption_type_t encryption_type)
      : fx_(fx)
      , test_attrs_(test_attrs)
      , test_dims_(test_dims)
      , array_type_(array_type)
      , cell_order_(cell_order)
      , tile_order_(tile_order)
      , write_order_(write_order)
      , encryption_type_(encryption_type) {
  }

  SmokeTestFx& fx_;
  const vector<SmokeTestFx::test_attr_t>& test_attrs_;
  const vector<SmokeTestFx::test_dim_t>& test_dims_;
  tiledb_array_type_t array_type_;
  tiledb_layout_t cell_order_;
  tiledb_layout_t tile_order_;
  tiledb_layout_t write_order_;
  tiledb_encryption_type_t encryption_type_;

  struct Buffers {
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> bufsizes_;
    std::vector<SmokeTestFx::test_query_buffer_t> bufs_;

    Buffers(
        uint64_t total_cells,
        std::span<const SmokeTestFx::test_dim_t> test_dims,
        std::span<const SmokeTestFx::test_attr_t> test_attrs) {
      bufsizes_.reserve(test_dims.size() + test_attrs.size());
      bufs_.reserve(test_dims.size() + test_attrs.size());
      for (const auto& dim : test_dims) {
        bufsizes_.push_back(std::make_tuple(
            total_cells * tiledb_datatype_size(dim.type_), 0, 0));
        bufs_.emplace_back(
            dim.name_,
            malloc(std::get<0>(bufsizes_.back())),
            &std::get<0>(bufsizes_.back()),
            nullptr,
            nullptr,
            nullptr,
            nullptr);
      }
      for (const auto& attr : test_attrs) {
        const bool is_var = attr.cell_val_num_ == constants::var_num;
        const uint64_t values_per_cell = (is_var ? 2 : attr.cell_val_num_);
        bufsizes_.push_back(std::make_tuple(
            values_per_cell * total_cells * tiledb_datatype_size(attr.type_),
            (is_var ? total_cells * sizeof(uint64_t) : 0),
            (attr.nullable_ ? total_cells * sizeof(uint8_t) : 0)));
        bufs_.emplace_back(
            attr.name_,
            malloc(std::get<0>(bufsizes_.back())),
            &std::get<0>(bufsizes_.back()),
            (is_var ? malloc(std::get<1>(bufsizes_.back())) : nullptr),
            (is_var ? &std::get<1>(bufsizes_.back()) : nullptr),
            static_cast<uint8_t*>(
                attr.nullable_ ? malloc(std::get<2>(bufsizes_.back())) :
                                 nullptr),
            (attr.nullable_ ? &std::get<2>(bufsizes_.back()) : nullptr));
      }
    }
    Buffers(const Buffers&) = delete;

    ~Buffers() {
      for (const auto& qbuf : bufs_) {
        if (qbuf.buffer_) {
          free(qbuf.buffer_);
        }
        if (qbuf.buffer_offset_) {
          free(qbuf.buffer_offset_);
        }
        if (qbuf.buffer_validity_) {
          free(qbuf.buffer_validity_);
        }
      }
    }
  };

  bool write_coords() const {
    return array_type_ == TILEDB_SPARSE;
  }

  /**
   * @return the total number of cells in the instance
   */
  uint64_t total_cells() const {
    uint64_t total_cells = 1;
    vector<uint64_t> dim_ranges;
    for (const auto& test_dim : test_dims_) {
      const uint64_t max_range = ((uint64_t*)(test_dim.domain_))[1];
      const uint64_t min_range = ((uint64_t*)(test_dim.domain_))[0];
      const uint64_t range = max_range - min_range + 1;
      total_cells *= range;
      dim_ranges.emplace_back(range);
    }
    return total_cells;
  }

  bool skip_write() const {
    // Skip row-major and col-major writes for sparse arrays.
    if (array_type_ == TILEDB_SPARSE && (write_order_ == TILEDB_ROW_MAJOR ||
                                         write_order_ == TILEDB_COL_MAJOR)) {
      return true;
    }

    // Skip unordered writes for dense arrays.
    if (array_type_ == TILEDB_DENSE) {
      if (write_order_ == TILEDB_UNORDERED) {
        return true;
      }
    }

    // String_ascii, float32, and float64 types can only be
    // written to sparse arrays.
    if (array_type_ == TILEDB_DENSE) {
      for (const auto& test_attr : test_attrs_) {
        if (test_attr.type_ == TILEDB_STRING_ASCII ||
            test_attr.type_ == TILEDB_FLOAT32 ||
            test_attr.type_ == TILEDB_FLOAT64) {
          return true;
        }
      }
    }
    return false;
  }

  bool skip_read(
      tiledb_layout_t read_order,
      const std::vector<std::shared_ptr<SmokeTestFx::test_query_condition_t>>
          query_conditions) const {
    // Skip unordered reads and global order reads for dense arrays.
    if (array_type_ == TILEDB_DENSE) {
      if (read_order == TILEDB_UNORDERED || read_order == TILEDB_GLOBAL_ORDER) {
        return true;
      } else if (!query_conditions.empty()) {
        // Reading dimension buffers on a dense array with a query condition
        // is unsupported.
        // RR: is this still true?
        return true;
      }
    }
    // If a query condition filters on an attribute name that does not
    // exist, skip this permutation of the smoke test.
    for (const auto& test_query_condition : query_conditions) {
      bool attr_exists_for_cond = false;
      for (const auto& test_attr : test_attrs_) {
        if (test_attr.name_ == test_query_condition->name_) {
          attr_exists_for_cond = true;
          break;
        }
      }

      if (!attr_exists_for_cond) {
        return true;
      }
    }

    return false;
  }

  void create() {
    fx_.create_array(
        array_type_,
        test_dims_,
        test_attrs_,
        cell_order_,
        tile_order_,
        encryption_type_);
  }

  std::shared_ptr<Buffers> write() {
    const uint64_t total_cells = this->total_cells();

    std::shared_ptr<Buffers> wbuf(new Buffers(
        total_cells,
        write_coords() ? test_dims_ :
                         std::span<const SmokeTestFx::test_dim_t>(),
        test_attrs_));

    const auto astart = (write_coords() ? test_dims_.size() : 0);

    // Create the write buffer for attribute "a".
    REQUIRE(test_attrs_[0].name_ == "a");
    int32_t* a_write_buffer =
        static_cast<int32_t*>(wbuf->bufs_[astart + 0].buffer_);
    uint8_t* a_validity = wbuf->bufs_[astart + 0].buffer_validity_;
    for (uint64_t i = 0; i < total_cells; i++) {
      a_write_buffer[i] = i;
    }
    for (uint64_t i = 0; i < total_cells; i++) {
      a_validity[i] = rand() % 2;
    }

    // Create the write buffers for attribute "b".
    if (test_attrs_.size() >= 2) {
      REQUIRE(test_attrs_[1].name_ == "b");
      int32_t* b_write_buffer =
          static_cast<int32_t*>(wbuf->bufs_[astart + 1].buffer_);
      for (uint64_t i = 0; i < (total_cells * 2); i++) {
        b_write_buffer[i] = i;
      }

      uint64_t* b_write_buffer_offset =
          static_cast<uint64_t*>(wbuf->bufs_[astart + 1].buffer_offset_);
      for (uint64_t i = 0; i < total_cells; i++) {
        b_write_buffer_offset[i] =
            i * tiledb_datatype_size(test_attrs_[1].type_) * 2;
      }
    }

    // Create the write buffer for attribute "c".
    char* c_write_buffer = nullptr;
    if (test_attrs_.size() >= 3) {
      REQUIRE(test_attrs_[2].name_ == "c");
      const uint64_t cell_len = test_attrs_[2].cell_val_num_;
      const uint64_t type_size = tiledb_datatype_size(test_attrs_[2].type_);
      REQUIRE(cell_len == 2);
      REQUIRE(type_size == 1);
      REQUIRE(!test_attrs_[2].nullable_);

      c_write_buffer = static_cast<char*>(wbuf->bufs_[astart + 2].buffer_);
      for (uint64_t i = 0; i < total_cells; i++) {
        c_write_buffer[(i * 2)] = 'a';
        c_write_buffer[(i * 2) + 1] = 'a' + (i % 10);
      }
    }

    // Define dimension query write vectors for sparse arrays.
    if (write_coords()) {
      vector<uint64_t> ranges;
      for (size_t d = 0; d < test_dims_.size(); d++) {
        const auto& test_dim = test_dims_[d];

        const uint64_t max_range =
            static_cast<const uint64_t*>(test_dim.domain_)[1];
        const uint64_t min_range =
            static_cast<const uint64_t*>(test_dim.domain_)[0];
        const uint64_t range = (max_range - min_range) + 1;

        REQUIRE(tiledb_datatype_size(test_dim.type_) == sizeof(uint64_t));

        uint64_t* d_write_buffer =
            static_cast<uint64_t*>(wbuf->bufs_[d].buffer_);

        for (uint64_t i = 0; i < total_cells; ++i) {
          uint64_t j = 1;
          for (const auto& range : ranges) {
            j *= range;
          }

          d_write_buffer[i] = ((i / j) % range) + 1;
        }

        ranges.emplace_back(range);
      }
    }

    // Execute the write query.
    fx_.write(wbuf->bufs_, write_order_, encryption_type_);

    return wbuf;
  }

  std::pair<uint64_t, std::shared_ptr<Buffers>> read(
      tiledb_layout_t read_order,
      const std::vector<std::shared_ptr<SmokeTestFx::test_query_condition_t>>&
          query_conditions,
      tiledb_query_condition_combination_op_t combination_op) {
    const uint64_t total_cells = this->total_cells();

    std::shared_ptr<Buffers> rbuf(new Buffers(
        total_cells,
        write_coords() ? test_dims_ :
                         std::span<const SmokeTestFx::test_dim_t>(),
        test_attrs_));
    const auto astart = (write_coords() ? test_dims_.size() : 0);

    // Create the read buffer for attribute "a".
    int32_t* a_read_buffer =
        static_cast<int32_t*>(rbuf->bufs_[astart + 0].buffer_);
    for (uint64_t i = 0; i < total_cells; i++) {
      a_read_buffer[i] = 0;
    }
    uint8_t* a_read_buffer_validity = rbuf->bufs_[astart + 0].buffer_validity_;
    for (uint64_t i = 0; i < total_cells; i++) {
      a_read_buffer_validity[i] = 0;
    }

    // Create the read buffers for attribute "b".
    if (test_attrs_.size() >= 2) {
      int32_t* b_read_buffer =
          static_cast<int32_t*>(rbuf->bufs_[astart + 1].buffer_);
      for (uint64_t i = 0; i < total_cells * 2; i++) {
        b_read_buffer[i] = 0;
      }

      uint64_t* b_read_buffer_offset =
          static_cast<uint64_t*>(rbuf->bufs_[astart + 1].buffer_offset_);
      for (uint64_t i = 0; i < total_cells; i++) {
        b_read_buffer_offset[i] = 0;
      }
    }

    // Create the read buffers for attribute "c".
    if (test_attrs_.size() >= 3) {
      char* c_read_buffer = static_cast<char*>(rbuf->bufs_[astart + 2].buffer_);
      for (uint64_t i = 0; i < total_cells; i++) {
        c_read_buffer[(i * 2)] = 0;
        c_read_buffer[(i * 2) + 1] = 0;
      }
    }

    // If we wrote dimension buffers, allocate dimension read buffers.
    if (write_coords()) {
      for (size_t d = 0; d < test_dims_.size(); d++) {
        const auto& test_dim = test_dims_[d];
        REQUIRE(tiledb_datatype_size(test_dim.type_) == sizeof(uint64_t));
        uint64_t* const d_read_buffer =
            static_cast<uint64_t*>(rbuf->bufs_[d].buffer_);

        for (uint64_t i = 0; i < total_cells; ++i) {
          d_read_buffer[i] = 0;
        }
      }
    }

    // This logic assumes that all dimensions are of type TILEDB_UINT64.
    uint64_t subarray_size = 2 * test_dims_.size() * sizeof(uint64_t);
    uint64_t* subarray_full = (uint64_t*)malloc(subarray_size);
    for (uint64_t i = 0; i < test_dims_.size(); ++i) {
      const uint64_t min_range = ((uint64_t*)(test_dims_[i].domain_))[0];
      const uint64_t max_range = ((uint64_t*)(test_dims_[i].domain_))[1];
      subarray_full[(i * 2)] = min_range;
      subarray_full[(i * 2) + 1] = max_range;
    }

    // Read from the array.
    fx_.read(
        query_conditions,
        rbuf->bufs_,
        subarray_full,
        read_order,
        encryption_type_,
        combination_op);

    // Calculate the number of cells read from the "a" read buffer.
    const uint64_t cells_read = (*rbuf->bufs_[astart].buffer_size_) /
                                tiledb_datatype_size(test_attrs_[0].type_);

    return std::make_pair(cells_read, rbuf);
  }

  void compare(
      uint64_t cells_read,
      Buffers& rbuf,
      Buffers& wbuf,
      const std::vector<std::shared_ptr<SmokeTestFx::test_query_condition_t>>&
          query_conditions,
      tiledb_query_condition_combination_op_t combination_op) const {
    const uint64_t total_cells = this->total_cells();

    const auto astart = (write_coords() ? test_dims_.size() : 0);

    const auto* a_write_buffer =
        static_cast<const int32_t*>(wbuf.bufs_[astart + 0].buffer_);
    const auto* a_write_buffer_validity =
        wbuf.bufs_[astart + 0].buffer_validity_;

    const auto* a_read_buffer =
        static_cast<const int32_t*>(rbuf.bufs_[astart + 0].buffer_);

    std::vector<std::pair<uint64_t*, uint64_t>> d_write_buffers;
    std::vector<std::pair<uint64_t*, uint64_t>> d_read_buffers;
    if (write_coords()) {
      for (size_t d = 0; d < test_dims_.size(); d++) {
        d_write_buffers.push_back(std::make_pair(
            static_cast<uint64_t*>(wbuf.bufs_[d].buffer_),
            *wbuf.bufs_[d].buffer_size_));
        d_read_buffers.push_back(std::make_pair(
            static_cast<uint64_t*>(rbuf.bufs_[d].buffer_),
            *rbuf.bufs_[d].buffer_size_));
      }
    }

    // Map each cell value to a bool that indicates whether or
    // not we expect it in the read results.
    unordered_map<int32_t, bool> expected_a_values_read;
    unordered_map<string, bool> expected_c_values_read;
    for (uint64_t i = 0; i < total_cells; ++i) {
      expected_a_values_read[i] = true;
      if (test_attrs_.size() >= 3) {
        const auto* c_write_buffer =
            static_cast<const char*>(wbuf.bufs_[astart + 2].buffer_);
        expected_c_values_read[string(&c_write_buffer[i * 2], 2)] = true;
      }
    }

    // Populate the expected values maps. We only filter on attributes
    // "a" and "c".
    for (const auto& test_query_condition : query_conditions) {
      if (test_query_condition->name_ == "a") {
        for (uint64_t i = 0; i < total_cells; ++i) {
          const bool expected = test_query_condition->cmp(&a_write_buffer[i]) &&
                                a_write_buffer_validity[i];
          REQUIRE(
              (combination_op == TILEDB_AND || combination_op == TILEDB_OR));
          if (combination_op == TILEDB_AND) {
            expected_a_values_read[i] = expected_a_values_read[i] && expected;
          } else {
            expected_a_values_read[i] = expected_a_values_read[i] || expected;
          }
        }
      } else {
        REQUIRE(test_query_condition->name_ == "c");
        const auto* c_write_buffer =
            static_cast<const char*>(wbuf.bufs_[astart + 2].buffer_);
        for (uint64_t i = 0; i < total_cells; ++i) {
          const bool expected =
              test_query_condition->cmp(&c_write_buffer[(i * 2)]);
          REQUIRE(
              (combination_op == TILEDB_AND || combination_op == TILEDB_OR));
          if (combination_op == TILEDB_AND) {
            expected_c_values_read[string(&c_write_buffer[i * 2], 2)] =
                expected_c_values_read[string(&c_write_buffer[i * 2], 2)] &&
                expected;
          } else {
            expected_c_values_read[string(&c_write_buffer[i * 2], 2)] =
                expected_c_values_read[string(&c_write_buffer[i * 2], 2)] ||
                expected;
          }
        }
      }
    }

    // When we check the values on "a", store a vector of the cell indexes
    // from the write-buffer. We can use this to ensure that the values
    // in the other attributes are similarly ordered.
    vector<uint64_t> cell_idx_vec;

    // Check the read values on "a".
    uint64_t non_null_cells = 0;
    for (uint64_t i = 0; i < cells_read; ++i) {
      const int32_t cell_value = ((int32_t*)a_read_buffer)[i];

      if (cell_value != std::numeric_limits<int32_t>::min()) {
        non_null_cells++;
        REQUIRE(expected_a_values_read[cell_value]);

        // We expect to read a unique cell value exactly once.
        expected_a_values_read[cell_value] = false;
      }

      // The cell value is the cell index in the write buffers.
      cell_idx_vec.emplace_back(cell_value);
    }

    // Check the read on "b".
    if (test_attrs_.size() >= 2) {
      const uint64_t type_size = tiledb_datatype_size(test_attrs_[1].type_);

      const auto* b_write_buffer =
          static_cast<const int32_t*>(wbuf.bufs_[astart + 1].buffer_);
      const auto* b_read_buffer =
          static_cast<const int32_t*>(rbuf.bufs_[astart + 1].buffer_);
      const uint64_t b_read_buffer_size = *rbuf.bufs_[astart + 1].buffer_size_;
      const auto* b_read_buffer_offset =
          static_cast<const uint64_t*>(rbuf.bufs_[astart + 1].buffer_offset_);

      // Null cells will have the fill value of length 1,
      // others the value of length 2.
      auto expected_size =
          (cells_read - non_null_cells + 2 * non_null_cells) * type_size;
      REQUIRE(b_read_buffer_size == expected_size);
      for (uint64_t i = 0; i < cells_read; ++i) {
        auto offset = b_read_buffer_offset[i] / type_size;
        if (((int32_t*)a_read_buffer)[i] ==
            std::numeric_limits<int32_t>::min()) {
          REQUIRE(
              ((int32_t*)b_read_buffer)[offset] ==
              std::numeric_limits<int32_t>::min());
        } else {
          const uint64_t write_i = cell_idx_vec[i];
          REQUIRE(
              ((int32_t*)b_read_buffer)[offset] ==
              ((int32_t*)b_write_buffer)[(write_i * 2)]);
          REQUIRE(
              ((int32_t*)b_read_buffer)[offset + 1] ==
              ((int32_t*)b_write_buffer)[(write_i * 2) + 1]);
        }
      }
    }

    // Check the read on "c"
    if (test_attrs_.size() >= 3) {
      const auto* c_write_buffer =
          static_cast<const char*>(wbuf.bufs_[astart + 2].buffer_);
      const auto* c_read_buffer =
          static_cast<const char*>(rbuf.bufs_[astart + 2].buffer_);
      const uint64_t c_read_buffer_size = *rbuf.bufs_[astart + 2].buffer_size_;

      const uint64_t cell_len = test_attrs_[2].cell_val_num_;
      const uint64_t type_size = tiledb_datatype_size(test_attrs_[2].type_);
      REQUIRE(c_read_buffer_size == cell_len * cells_read * type_size);

      for (uint64_t i = 0; i < cells_read; ++i) {
        REQUIRE(expected_c_values_read[string(
            &c_read_buffer[(i * cell_len)], cell_len)]);

        const uint64_t write_i = cell_idx_vec[i];
        for (uint64_t j = 0; j < cell_len; ++j) {
          REQUIRE(
              c_read_buffer[(i * cell_len) + j] ==
              c_write_buffer[(write_i * cell_len) + j]);
        }
      }
    }

    // Check the read on the dimensions.
    for (size_t d = 0; d < d_read_buffers.size(); ++d) {
      REQUIRE(d_read_buffers[d].second / sizeof(uint64_t) == cells_read);

      for (uint64_t i = 0; i < cells_read; ++i) {
        const uint64_t write_i = cell_idx_vec[i];
        REQUIRE(
            d_read_buffers[d].first[i] == d_write_buffers[d].first[write_i]);
      }
    }
  }
};

TEST_CASE_METHOD(
    SmokeTestFx,
    "C API: Test a dynamic range of arrays",
    "[capi][smoke][longtest]") {
  // Build a vector of attributes.
  vector<test_attr_t> attrs;
  attrs.emplace_back("a", TILEDB_INT32, 1, true);
  attrs.emplace_back("b", TILEDB_INT32, TILEDB_VAR_NUM, false);
  attrs.emplace_back("c", TILEDB_STRING_ASCII, 2, false);

  // Build a vector of query conditions.
  vector<vector<shared_ptr<test_query_condition_t>>> query_conditions_vec;
  query_conditions_vec.push_back({});
  query_conditions_vec.push_back({make_condition<int32_t>("a", TILEDB_LT, 4)});
  query_conditions_vec.push_back({make_condition<int32_t>("a", TILEDB_GT, 3)});
  query_conditions_vec.push_back({make_condition<int32_t>("a", TILEDB_LE, 20)});
  query_conditions_vec.push_back({make_condition<int32_t>("a", TILEDB_GE, 3)});
  query_conditions_vec.push_back({make_condition<int32_t>("a", TILEDB_EQ, 7)});
  query_conditions_vec.push_back({make_condition<int32_t>("a", TILEDB_NE, 10)});

  query_conditions_vec.push_back({
      make_condition<int32_t>("a", TILEDB_GT, 6),
      make_condition<int32_t>("a", TILEDB_LE, 20),
  });
  query_conditions_vec.push_back({
      make_condition<int32_t>("a", TILEDB_LT, 30),
      make_condition<int32_t>("a", TILEDB_GE, 7),
      make_condition<int32_t>("a", TILEDB_NE, 9),
  });

  query_conditions_vec.push_back(
      {make_condition<const char*>("c", TILEDB_LT, "ae")});
  query_conditions_vec.push_back(
      {make_condition<const char*>("c", TILEDB_GE, "ad")});
  query_conditions_vec.push_back(
      {make_condition<const char*>("c", TILEDB_EQ, "ab")});

  query_conditions_vec.push_back(
      {make_condition<int32_t>("a", TILEDB_LT, 30),
       make_condition<const char*>("c", TILEDB_GE, "ad")});

  // Build a vector of dimensions.
  vector<test_dim_t> dims;
  const uint64_t d1_domain[] = {1, 9};
  const uint64_t d1_tile_extent = 3;
  dims.emplace_back("d1", TILEDB_UINT64, d1_domain, d1_tile_extent);
  const uint64_t d2_domain[] = {1, 10};
  const uint64_t d2_tile_extent = 5;
  dims.emplace_back("d2", TILEDB_UINT64, d2_domain, d2_tile_extent);
  const uint64_t d3_domain[] = {1, 15};
  const uint64_t d3_tile_extent = 5;
  dims.emplace_back("d3", TILEDB_UINT64, d3_domain, d3_tile_extent);

  // Generate test conditions
  auto num_attrs{GENERATE(1, 2, 3)};
  auto num_dims{GENERATE(1, 2, 3)};
  auto array_type{GENERATE(TILEDB_DENSE, TILEDB_SPARSE)};
  auto cell_order{GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR)};
  auto tile_order{GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR)};
  auto encryption_type{GENERATE(TILEDB_NO_ENCRYPTION, TILEDB_AES_256_GCM)};
  auto write_order{GENERATE(TILEDB_ROW_MAJOR, TILEDB_UNORDERED)};

  DYNAMIC_SECTION(
      array_type_str((ArrayType)array_type)
      << " array with " << num_attrs << " attribute(s) and " << num_dims
      << " dimension(s)." << layout_str((Layout)cell_order) << " cells, "
      << layout_str((Layout)tile_order) << " tiles, "
      << layout_str((Layout)write_order) << " writes, "
      << encryption_type_str((EncryptionType)encryption_type)
      << " encryption") {
    vector<test_attr_t> test_attrs(attrs.begin(), attrs.begin() + num_attrs);
    vector<test_dim_t> test_dims(dims.begin(), dims.begin() + num_dims);

    Instance a(
        *this,
        test_attrs,
        test_dims,
        array_type,
        cell_order,
        tile_order,
        write_order,
        encryption_type);
    a.create();

    if (a.skip_write()) {
      return;
    }

    auto wbuf = a.write();

    for (tiledb_layout_t read_order :
         {TILEDB_ROW_MAJOR, TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER}) {
      for (size_t q = 0; q < query_conditions_vec.size(); q++) {
        if (a.skip_read(read_order, query_conditions_vec[q])) {
          continue;
        }
        for (const tiledb_query_condition_combination_op_t combination_op :
             {TILEDB_AND, TILEDB_OR}) {
          DYNAMIC_SECTION(
              layout_str((Layout)read_order)
              << " reads, [" << std::to_string(q) << "] conditions, "
              << combination_op_str(combination_op)) {
            auto rbuf =
                a.read(read_order, query_conditions_vec[q], combination_op);
            a.compare(
                rbuf.first,
                *rbuf.second.get(),
                *wbuf.get(),
                query_conditions_vec[q],
                combination_op);
          }
        }
      }
    }
  }
}
