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
#include "port/port_posix.h"
#include "util/murmurhash.h"
#include "rocksdb/utilities/dirty_buffer_scan.h"

namespace rocksdb {

class RWMutex;
class DirtyVersion;
class WriteInfo;
class ScanInfo;

typedef uint64_t SequenceNumber;
using TransactionID = uint64_t;

using std::string;
using std::mutex;

struct DirtyScanBufferContext {
  TransactionID self_txn_id = 0;
  // dependency ids
  std::vector<TransactionID> txn_ids;
};

struct DirtyReadBufferContext {
  bool found_dirty = false;
  SequenceNumber seq = 0;
  // dirty value written by which txn
  TransactionID txn_id = 0;
  // txn operating the dirty read
  TransactionID self_txn_id = 0;
  // whether found a deletion
  bool deletion = false;
};

struct DirtyWriteBufferContext {
    // w-w dependency id
    TransactionID wrtie_txn_id = 0;
    // anti-dependency ids & scan-dependency ids
    std::vector<TransactionID> read_txn_ids;
};

class DirtyBuffer {
 public:

  explicit DirtyBuffer(uint32_t column_family_id, int size);

  ~DirtyBuffer();

  Status Put(const string& key, const string& value, SequenceNumber seq, TransactionID txn_id, DirtyWriteBufferContext *context);

  Status Delete(const string& key, SequenceNumber seq, TransactionID txn_id, DirtyWriteBufferContext *context);

  Status Get(const string& key, string* value, DirtyReadBufferContext* context);

  Status Scan(const ReadOptions& read_options, DirtyBufferScanCallback* callback, DirtyScanBufferContext* context);

  Status Remove(const string& key, TransactionID txn_id);

  Status RemoveScanInfo(TransactionID txn_id);

  Status MakeOperationVisible(const string& key, TransactionID txn_id);

  Status MakeScanOperationVisible(TransactionID txn_id);

 private:
  uint32_t column_family_id_;
  std::vector<mutex> locks_;
  int size_;

  DirtyVersion **dirty_array_;

  // scan operations are exclusive, inserts are not
  port::RWMutex buffer_mutex;

  // protect the read write of the scan_list
  port::RWMutex scan_list_mutex;
  // record the txn ids which have perform the dirty buffer scan
  std::vector<ScanInfo*> scan_list;

  int GetPosition(const string &key);
  mutex* GetLock(int pos);

  // No copying allowed
  DirtyBuffer(const DirtyBuffer&);
  DirtyBuffer& operator=(const DirtyBuffer&);
};

class DirtyVersion {
 public:

  // normal write operation
  explicit DirtyVersion(const string &key, const string &value, SequenceNumber seq, TransactionID txn_id);

  // delete operation
  explicit DirtyVersion(const string &key, SequenceNumber seq, TransactionID txn_id);

  // normal read operation
  explicit DirtyVersion(const string &key, TransactionID txn_id);

  ~DirtyVersion();

 private:

  friend class DirtyBuffer;

  bool is_write;

  string key_;
  TransactionID txn_id_;

  WriteInfo* write_info = nullptr;

  DirtyVersion* link_older = nullptr;
  DirtyVersion* link_newer = nullptr;

  bool skip = true;

  // No copying allowed
  DirtyVersion(const DirtyVersion&);
  DirtyVersion& operator=(const DirtyVersion&);

};

class WriteInfo {
public:

  // normal write operation
  explicit WriteInfo(const string &value, SequenceNumber seq);

  // delete operation
  explicit WriteInfo(SequenceNumber seq);

  ~WriteInfo();

private:

  friend class DirtyBuffer;
  friend class DirtyVersion;

  string value_;
  SequenceNumber seq_;
  bool deletion_;

  // No copying allowed
  WriteInfo(const WriteInfo&);
  WriteInfo& operator=(const WriteInfo&);

};

class ScanInfo {
public:

  // normal write operation
  explicit ScanInfo(TransactionID txn_id, const ReadOptions& read_options);

  ~ScanInfo();

private:

  friend class DirtyBuffer;
  friend class DirtyVersion;

  TransactionID txn_id_;

  string iterate_lower_bound_;
  string iterate_upper_bound_;
  bool skip = true;

  // No copying allowed
  ScanInfo(const ScanInfo&);
  ScanInfo& operator=(const ScanInfo&);

};

}  // namespace rocksdb
