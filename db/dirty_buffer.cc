//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dirty_buffer.h"

namespace rocksdb {


  DirtyBuffer::DirtyBuffer(uint32_t column_family_id)
  : column_family_id_(column_family_id) {

  }

  DirtyBuffer::~DirtyBuffer() {

  }

  Status DirtyBuffer::Put(const Slice& key, const Slice& value, SequenceNumber seq, TransactionID txn_id) {
    WriteLock wl(&map_mutex_);
    map[key.ToString()] = new DirtyVersion(key, value, seq, txn_id);
    return Status::OK();
  }

  Status DirtyBuffer::GetDirty(const Slice &key, std::string *value, DirtyReadBufferContext *context) {
    ReadLock rl(&map_mutex_);
    auto it = map.find(key.ToString());
    if (it != map.end()) {
      *(context->found_dirty) = true;
      DirtyVersion* dirty = it->second;
      Slice stored_value = dirty->GetValue();
      value->assign(stored_value.data(), stored_value.size());
      context->seq = dirty->GetSeq();
      context->txn_id = dirty->GetTxnId();
      return Status::OK();
    }
    return Status::NotFound();
  }

  Status DirtyBuffer::Remove(const std::unordered_set<string>* keys, TransactionID txn_id) {
    WriteLock rl(&map_mutex_);
    const std::unordered_set<string> key_set = *keys;
    for (auto set_it = key_set.begin(); set_it != key_set.end(); set_it++) {
      string key = *set_it;
      auto it = map.find(key);
      if (it != map.end()) {
        DirtyVersion* dirty = it->second;
        TransactionID stored_txn_id = dirty->GetTxnId();
        if (stored_txn_id == txn_id) {
          map.erase(it);
        }
      }
    }
    return Status::OK();
  }

  DirtyVersion::DirtyVersion(const Slice& key, const Slice& value, SequenceNumber seq, TransactionID txn_id)
    : key_(key), value_(value), seq_(seq), txn_id_(txn_id){

  }

  DirtyVersion::~DirtyVersion() {

  }

}  // namespace rocksdb
