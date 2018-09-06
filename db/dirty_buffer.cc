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
    map[key.ToString()] = std::unique_ptr<DirtyVersion>(new DirtyVersion(key, value, seq, txn_id));
    return Status::OK();
  }

  Status DirtyBuffer::GetDirty(const Slice &key, std::string *value, DirtyReadBufferContext *context) {
    ReadLock rl(&map_mutex_);
    if (map.find(key.ToString()) != map.end()) {
      *(context->found_dirty) = true;
      DirtyVersion* dirty = map.at(key.ToString()).get();
      Slice stored_value = dirty->GetValue();
      value->assign(stored_value.data(), stored_value.size());
      context->seq = dirty->GetSeq();
      context->txn_id = dirty->GetTxnId();
      return Status::OK();
    }
    return Status::NotFound();
  }

  DirtyVersion::DirtyVersion(const Slice& key, const Slice& value, SequenceNumber seq, TransactionID txn_id)
    : key_(key), value_(value), seq_(seq), txn_id_(txn_id){

  }

  DirtyVersion::~DirtyVersion() {

  }

}  // namespace rocksdb
