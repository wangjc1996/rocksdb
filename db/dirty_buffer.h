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
#include <mutex>

#include "db/dbformat.h"
#include "util/murmurhash.h"

namespace rocksdb {

class DirtyVersion;
class ReadRecord;

typedef uint64_t SequenceNumber;
using TransactionID = uint64_t;

using std::string;
using std::mutex;

struct DirtyReadBufferContext {
  bool *found_dirty = nullptr;
  SequenceNumber seq = 0;
  // dirty value written by which txn
  TransactionID txn_id = 0;
  // txn operating the dirty read
  TransactionID self_txn_id = 0;
};

struct DirtyWriteBufferContext {
    // w-w dependency id
    TransactionID wrtie_txn_id = 0;
    // anti-dependency ids
    std::vector<TransactionID> read_txn_ids;
};

class DirtyBuffer {
 public:

  explicit DirtyBuffer(uint32_t column_family_id, int size);

  ~DirtyBuffer();

  Status Put(const Slice& key, const Slice& value, SequenceNumber seq, TransactionID txn_id, DirtyWriteBufferContext *context);

  Status GetDirty(const Slice& key, std::string* value, DirtyReadBufferContext* context);

  Status Remove(const Slice& key, TransactionID txn_id);

 private:
  uint32_t column_family_id_;
  std::vector<mutex> locks_;
  int size_;

  DirtyVersion **dirty_array_;

  int GetPosition(const Slice &key);
  mutex* GetLock(const int pos);

  // No copying allowed
  DirtyBuffer(const DirtyBuffer&);
  DirtyBuffer& operator=(const DirtyBuffer&);
};

class DirtyVersion {
 public:

  explicit DirtyVersion(const Slice& key, const Slice& value, SequenceNumber seq, TransactionID txn_id);

  ~DirtyVersion();

  inline Slice GetKey() { return key_; };

  inline Slice GetValue() { return value_; };

  inline SequenceNumber GetSeq() { return seq_; };

  inline TransactionID GetTxnId() { return txn_id_; };

 private:

  friend class DirtyBuffer;

  Slice key_;
  Slice value_;
  SequenceNumber seq_;
  TransactionID txn_id_;

  DirtyVersion* link_older = nullptr;
  DirtyVersion* link_newer = nullptr;

  ReadRecord* readRecord = nullptr;

  // No copying allowed
  DirtyVersion(const DirtyVersion&);
  DirtyVersion& operator=(const DirtyVersion&);

};

class ReadRecord {
 public:

  explicit ReadRecord(TransactionID txn_id);

  ~ReadRecord();

  inline TransactionID GetTxnId() { return txn_id_; };

 private:

  friend class DirtyBuffer;
  friend class DirtyVersion;

  TransactionID txn_id_;

  ReadRecord* link_older = nullptr;

  // No copying allowed
  ReadRecord(const ReadRecord&);
  ReadRecord& operator=(const ReadRecord&);

};

}  // namespace rocksdb
