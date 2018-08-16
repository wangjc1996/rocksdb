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
//#include "db/range_del_aggregator.h"
//#include "db/read_callback.h"
//#include "db/version_edit.h"
//#include "monitoring/instrumented_mutex.h"
//#include "options/cf_options.h"
//#include "rocksdb/db.h"
//#include "rocksdb/env.h"
//#include "rocksdb/memtablerep.h"
//#include "util/allocator.h"
//#include "util/concurrent_arena.h"
//#include "util/dynamic_bloom.h"
//#include "util/hash.h"

namespace rocksdb {

class Mutex;
class DirtyVersion;

typedef uint64_t SequenceNumber;
typedef uint64_t TxnNumber;

using std::string;

class DirtyBuffer {
 public:

  explicit DirtyBuffer(uint32_t column_family_id);

  ~DirtyBuffer();

  Status Put(string& key, string& value, SequenceNumber seq, TxnNumber txn_id);

 private:
  uint32_t column_family_id_;
  std::unordered_map<string, std::shared_ptr<DirtyVersion>> map;

  // No copying allowed
  DirtyBuffer(const DirtyBuffer&);
  DirtyBuffer& operator=(const DirtyBuffer&);
};

class DirtyVersion {
 public:

  explicit DirtyVersion(string& key, string& value, SequenceNumber seq, TxnNumber txn_id);

  ~DirtyVersion();

 private:

  string& key_;
  string& value_;
  SequenceNumber seq_;
  TxnNumber txn_id_;

  // No copying allowed
  DirtyVersion(const DirtyVersion&);
  DirtyVersion& operator=(const DirtyVersion&);

};

}  // namespace rocksdb
