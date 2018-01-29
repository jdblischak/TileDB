/**
 * @file unit-buffer.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests the `Buffer` class.
 */

#include <buffer.h>
#include <catch.hpp>

using namespace tiledb;

TEST_CASE("Buffer: Test default constructor with write void*", "[buffer]") {
  // Write a char array
  Status st;
  char data[3] = {1, 2, 3};
  auto buff = new Buffer();
  CHECK(buff->size() == 0);
  st = buff->write(data, sizeof(data));
  REQUIRE(st.ok());
  CHECK(buff->offset() == 3);
  CHECK(buff->size() == sizeof(data));
  CHECK(buff->alloced_size() == 3);
  buff->reset_offset();
  CHECK(buff->offset() == 0);

  // Read a single char value
  char val = 0;
  st = buff->read(&val, sizeof(char));
  REQUIRE(st.ok());
  CHECK(val == 1);
  CHECK(buff->offset() == 1);

  // Read two values
  char readtwo[2] = {0, 0};
  st = buff->read(readtwo, 2);
  REQUIRE(st.ok());
  CHECK(readtwo[0] == 2);
  CHECK(readtwo[1] == 3);
  CHECK(buff->offset() == 3);

  // Reallocate
  st = buff->realloc(10);
  REQUIRE(st.ok());
  CHECK(buff->size() == 3);
  CHECK(buff->alloced_size() == 10);
  CHECK(buff->offset() == 3);

  delete buff;
}
