//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <string>

#include "db/dbformat.h"
#include "port/port_posix.h"

namespace rocksdb {

class RWMutex;

typedef uint64_t SequenceNumber;

using std::string;

class ValidationMap {
  public:

  explicit ValidationMap(uint32_t column_family_id);

  ~ValidationMap();

  Status Put(const Slice& key, SequenceNumber seq);

  SequenceNumber GetLatestSequenceNumber(const Slice &key);

  mutable port::RWMutex map_mutex_;

private:
  uint32_t column_family_id_;
  std::unordered_map<string, SequenceNumber> map;

  // No copying allowed
  ValidationMap(const ValidationMap&);
  ValidationMap& operator=(const ValidationMap&);
};

}  // namespace rocksdb
