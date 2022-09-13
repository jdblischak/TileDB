/**
 * @file   experimental_helpers.h
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
 * This file declares some test suite helper functions for experimental
 * features.
 */

#ifndef TILEDB_TEST_EXPERIMENTAL_HELPERS_H
#define TILEDB_TEST_EXPERIMENTAL_HELPERS_H

#include "helpers.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/c_api/experimental/tiledb_dimension_label.h"
#include "tiledb/sm/c_api/experimental/tiledb_struct_def.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

/**
 * Helper method for adding an internal dimension label to an array schema.
 *
 * TODO: Add param docs
 */
void add_dimension_label(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const std::string& label_name,
    const uint32_t dim_idx,
    tiledb_label_order_t label_order,
    tiledb_datatype_t label_datatype,
    const void* label_domain,
    const void* label_tile_extent,
    const void* index_tile_extent,
    optional<uint32_t> label_cell_val_num = nullopt,
    optional<std::pair<tiledb_filter_type_t, int>> label_filters = nullopt,
    optional<std::pair<tiledb_filter_type_t, int>> index_filters = nullopt,
    optional<uint64_t> capacity = nullopt,
    optional<bool> allows_dups = nullopt);

#endif
