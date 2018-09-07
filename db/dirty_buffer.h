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
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "db/dbformat.h"
#include "port/port_posix.h"

namespace rocksdb {

class RWMutex;
class DirtyVersion;

typedef uint64_t SequenceNumber;
using TransactionID = uint64_t;

using std::string;

struct DirtyReadBufferContext {
  bool *is_dirty_read;
  bool *found_dirty;
  SequenceNumber seq;
  TransactionID txn_id;
};

class DirtyBuffer {
 public:

  explicit DirtyBuffer(uint32_t column_family_id);

  ~DirtyBuffer();

  Status Put(const Slice& key, const Slice& value, SequenceNumber seq, TransactionID txn_id);

  Status GetDirty(const Slice& key, std::string* value, DirtyReadBufferContext* context);

  Status Remove(const std::unordered_set<string>* keys, TransactionID txn_id);

  mutable port::RWMutex map_mutex_;

 private:
  uint32_t column_family_id_;
  std::unordered_map<string, DirtyVersion*> map;

  // No copying allowed
  DirtyBuffer(const DirtyBuffer&);
  DirtyBuffer& operator=(const DirtyBuffer&);
};

class DirtyVersion {
 public:

  explicit DirtyVersion(const Slice& key, const Slice& value, SequenceNumber seq, TransactionID txn_id);

  ~DirtyVersion();

  inline Slice GetValue() { return value_; };

  inline SequenceNumber GetSeq() { return seq_; };

  inline TransactionID GetTxnId() { return txn_id_; };

 private:

  Slice key_;
  Slice value_;
  SequenceNumber seq_;
  TransactionID txn_id_;

  // No copying allowed
  DirtyVersion(const DirtyVersion&);
  DirtyVersion& operator=(const DirtyVersion&);

};

}  // namespace rocksdb
