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


  DirtyBuffer::DirtyBuffer(uint32_t column_family_id, int size)
      : column_family_id_(column_family_id),
        locks_(size),
        size_(size) {
    dirty_array_ = new DirtyVersion *[size]();
  }

  DirtyBuffer::~DirtyBuffer() {
    delete[] dirty_array_;
  }

  Status DirtyBuffer::Put(const Slice &key, const Slice &value, SequenceNumber seq, TransactionID txn_id, DirtyWriteBufferContext *context) {
    int position = GetPosition(key);
    WriteLock wl(GetLock(position));

    //get dependency ids
    auto *dirty = dirty_array_[position];
    while (dirty != nullptr) {
      if (key.compare(dirty->GetKey()) != 0 || dirty->txn_id_ == txn_id) {
        dirty = dirty->link_older;
        continue;
      }
      // w-w dependency
      context->wrtie_txn_id = dirty->GetTxnId();
      // anti-dependency
      auto *pending = dirty->readRecord;
      while (pending != nullptr) {
        if (pending->txn_id_ != txn_id) {
          context->read_txn_ids.emplace_back(pending->txn_id_);
        }
        pending = pending->link_older;
      }
      break;
    }

    auto *current = new DirtyVersion(key, value, seq, txn_id);
    auto *header = dirty_array_[position];
    if (header == nullptr) {
      dirty_array_[position] = current;
    } else {
      current->link_older = header;
      header->link_newer = current;
      dirty_array_[position] = current;
    }

    return Status::OK();
  }

  Status DirtyBuffer::GetDirty(const Slice &key, std::string *value, DirtyReadBufferContext *context) {
    int position = GetPosition(key);
    WriteLock wl(GetLock(position));
    auto *dirty = dirty_array_[position];
    while (dirty != nullptr) {
      if (key.compare(dirty->GetKey()) != 0) {
        dirty = dirty->link_older;
        continue;
      }
      *(context->found_dirty) = true;
      Slice stored_value = dirty->GetValue();
      value->assign(stored_value.data(), stored_value.size());
      context->seq = dirty->GetSeq();
      context->txn_id = dirty->GetTxnId();
      // record dirty read operation
      auto *new_record = new ReadRecord(context->self_txn_id);
      if (dirty->readRecord != nullptr) {
        auto *old_header = dirty->readRecord;
        new_record->link_older = old_header;
      }
      dirty->readRecord = new_record;
      return Status::OK();
    }
    return Status::NotFound();
  }

  Status DirtyBuffer::Remove(const Slice &key, TransactionID txn_id) {
    int position = GetPosition(key);
    WriteLock wl(GetLock(position));
    auto *dirty = dirty_array_[position];
    while (dirty != nullptr) {
      TransactionID stored_txn_id = dirty->GetTxnId();
      if (stored_txn_id != txn_id || key.compare(dirty->GetKey()) != 0) {
        dirty = dirty->link_older;
        continue;
      }
      if (dirty->link_newer == nullptr) {
        //head of the linked list
        auto *new_header = dirty->link_older;
        if (new_header == nullptr) {
          dirty_array_[position] = nullptr;
          delete dirty;
          break;
        } else {
          dirty_array_[position] = new_header;
          new_header->link_newer = nullptr;
        }
      } else if (dirty->link_older == nullptr) {
        //end of the linked list, not single item in the linked list
        auto *former = dirty->link_newer;
        former->link_older = nullptr;
        dirty->link_newer = nullptr;
      } else {
        //middle of the linked list
        auto *former = dirty->link_newer;
        auto *latter = dirty->link_older;
        former->link_older = latter;
        latter->link_newer = former;
      }
      DirtyVersion *temp = dirty->link_older;
      delete dirty;
      dirty = temp;
    }
    return Status::OK();
  }

  int DirtyBuffer::GetPosition(const Slice &key) {
    static murmur_hash hash;
    return static_cast<int>(hash(key) % size_);
  }

  port::RWMutex *DirtyBuffer::GetLock(const int pos) {
    return &locks_[pos];
  }

  DirtyVersion::DirtyVersion(const Slice &key, const Slice &value, SequenceNumber seq, TransactionID txn_id)
      : key_(key), value_(value), seq_(seq), txn_id_(txn_id) {

  }

  DirtyVersion::~DirtyVersion() {
    if (readRecord != nullptr) {
      auto *pending = readRecord;
      while (pending != nullptr) {
        auto *current = pending;
        pending = current->link_older;
        delete current;
      }
    }
  }

  ReadRecord::ReadRecord(TransactionID txn_id) : txn_id_(txn_id) {

  }

  ReadRecord::~ReadRecord() {

  }

}  // namespace rocksdb
