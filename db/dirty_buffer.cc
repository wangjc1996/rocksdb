//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dirty_buffer.h"

using std::lock_guard;

namespace rocksdb {


  DirtyBuffer::DirtyBuffer(uint32_t column_family_id, int size)
      : column_family_id_(column_family_id),
        locks_(size),
        size_(size) {
    dirty_array_ = new DirtyVersion *[size]();
  }

  DirtyBuffer::~DirtyBuffer() {
    for (int position = 0; position < size_; ++position) {
      auto *dirty = dirty_array_[position];
      // make sure buffer is clean
      if (dirty != nullptr) assert(false);
    }

    delete[] dirty_array_;
  }

  Status DirtyBuffer::Put(const string &key, const string &value, SequenceNumber seq, TransactionID txn_id, DirtyWriteBufferContext *context) {
    int position = GetPosition(key);
    lock_guard<mutex> lock_guard(*GetLock(position));

    //get dependency ids
    auto *dirty = dirty_array_[position];
    while (dirty != nullptr) {
      if (key.compare(dirty->key_) != 0 || dirty->txn_id_ == txn_id) {
        dirty = dirty->link_older;
        continue;
      }

      if (dirty->is_write) {
        // w-w dependency
        context->wrtie_txn_id = dirty->txn_id_;
        break;
      } else {
        // anti-dependency
        context->read_txn_ids.emplace_back(dirty->txn_id_);
        dirty = dirty->link_older;
        continue;
      }
    }

    // insert write operation
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

  Status DirtyBuffer::GetDirty(const string &key, std::string *value, DirtyReadBufferContext *context) {
    int position = GetPosition(key);
    lock_guard<mutex> lock_guard(*GetLock(position));

    // insert read operation
    auto *current = new DirtyVersion(key, context->self_txn_id);
    auto *header = dirty_array_[position];
    if (header == nullptr) {
      dirty_array_[position] = current;
    } else {
      current->link_older = header;
      header->link_newer = current;
      dirty_array_[position] = current;
    }

    // search dirty version in buffer
    auto *dirty = dirty_array_[position];
    while (dirty != nullptr) {
      if (key.compare(dirty->key_) != 0 || !dirty->is_write) {
        dirty = dirty->link_older;
        continue;
      }
      context->found_dirty = true;
      assert(dirty->write_info != nullptr);
      Slice stored_value = dirty->write_info->value_;
      value->assign(stored_value.data(), stored_value.size());
      context->seq = dirty->write_info->seq_;
      context->txn_id = dirty->txn_id_;
      return Status::OK();
    }
    return Status::NotFound();
  }

  Status DirtyBuffer::Remove(const string &key, TransactionID txn_id) {
    int position = GetPosition(key);
    lock_guard<mutex> lock_guard(*GetLock(position));
    auto *dirty = dirty_array_[position];
    while (dirty != nullptr) {
      TransactionID stored_txn_id = dirty->txn_id_;
      if (stored_txn_id != txn_id) {
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

  int DirtyBuffer::GetPosition(const string &key) {
    static murmur_hash hash;
    return static_cast<int>(hash(key) % size_);
  }

  mutex *DirtyBuffer::GetLock(const int pos) {
    return &locks_[pos];
  }

  DirtyVersion::DirtyVersion(const string &key, const string &value, SequenceNumber seq, TransactionID txn_id)
      : key_(key), txn_id_(txn_id) {
    is_write = true;
    write_info = new WriteInfo(value, seq);
  }

  DirtyVersion::DirtyVersion(const string &key, TransactionID txn_id)
    : key_(key), txn_id_(txn_id) {
    is_write = false;
    write_info = nullptr;
  }

  DirtyVersion::~DirtyVersion() {
      delete write_info;
  }

  WriteInfo::WriteInfo(const string &value, SequenceNumber seq)
      : value_(value), seq_(seq) {
  }

  WriteInfo::~WriteInfo() {

  }

}  // namespace rocksdb
