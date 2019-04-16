//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dirty_buffer.h"
#include "rocksdb/comparator.h"

using std::lock_guard;

namespace rocksdb {


  DirtyBuffer::DirtyBuffer(uint32_t column_family_id, int size)
      : column_family_id_(column_family_id),
        locks_(size),
        size_(size) {
    dirty_array_ = new DirtyVersion *[size]();
    scan_list.reserve(64);
  }

  DirtyBuffer::~DirtyBuffer() {
    for (int position = 0; position < size_; ++position) {
      auto *dirty = dirty_array_[position];
      // make sure buffer is clean
      if (dirty != nullptr) assert(false);
    }
    assert(scan_list.empty());

    delete[] dirty_array_;
  }

  Status DirtyBuffer::Put(const string &key, const string &value, SequenceNumber seq, TransactionID txn_id, DirtyWriteBufferContext *context) {

    // not exclusive on the whole buffer
    ReadLock rl(&buffer_mutex);

    int position = GetPosition(key);
    lock_guard<mutex> lock_guard(*GetLock(position));

    // scan dependency ids
    {
      ::lock_guard<mutex> scan_list_guard(scan_list_mutex);
      for (auto it = scan_list.begin(); it != scan_list.end(); ++it) {
        // most recent scan id is at the end of the vector
        if (*it != txn_id) context->read_txn_ids.emplace_back(*it);
        else break;
      }
    }
    //get other dependency ids
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

  Status DirtyBuffer::Delete(const string &key, SequenceNumber seq, TransactionID txn_id, DirtyWriteBufferContext *context) {

    // not exclusive on the whole buffer
    ReadLock rl(&buffer_mutex);

    int position = GetPosition(key);
    lock_guard<mutex> lock_guard(*GetLock(position));

    // scan dependency ids
    {
      ::lock_guard<mutex> scan_list_guard(scan_list_mutex);
      for (auto it = scan_list.begin(); it != scan_list.end(); ++it) {
        // most recent scan id is at the end of the vector
        if (*it != txn_id) context->read_txn_ids.emplace_back(*it);
        else break;
      }
    }
    //get other dependency ids
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

    // insert delete operation
    auto *current = new DirtyVersion(key, seq, txn_id);
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

  Status DirtyBuffer::Get(const string &key, std::string *value, DirtyReadBufferContext *context) {

    // not exclusive on the whole buffer
    ReadLock rl(&buffer_mutex);

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
      if (dirty->write_info->deletion_) {
        context->found_dirty = true;
        assert(dirty->write_info != nullptr && dirty->write_info->deletion_);
        context->deletion = true;
        context->seq = dirty->write_info->seq_;
        context->txn_id = dirty->txn_id_;
      } else {
        context->found_dirty = true;
        assert(dirty->write_info != nullptr);
        Slice stored_value = dirty->write_info->value_;
        value->assign(stored_value.data(), stored_value.size());
        context->deletion = false;
        context->seq = dirty->write_info->seq_;
        context->txn_id = dirty->txn_id_;
      }
      return Status::OK();
    }
    return Status::NotFound();
  }

  Status DirtyBuffer::Scan(const ReadOptions& read_options, DirtyBufferScanCallback* callback, DirtyScanBufferContext* context) {

    // scan operation is exclusive
    WriteLock wl(&buffer_mutex);

    {
      // add range query operation to the access list
      assert(context->self_txn_id > 0 && context->self_txn_id != ULONG_MAX);
      lock_guard<mutex> scan_list_guard(scan_list_mutex);
      if(std::find(scan_list.begin(), scan_list.end(), context->self_txn_id) == scan_list.end()) {
        scan_list.emplace_back(context->self_txn_id);
      }
    }

    const Comparator *comparator = BytewiseComparator();

    for (int position = 0; position < size_; ++position) {
      // search dirty version in buffer
      auto *dirty = dirty_array_[position];
      while (dirty != nullptr) {
        if (!dirty->is_write) {
          dirty = dirty->link_older;
          continue;
        }

        Slice target(dirty->key_);
        if (comparator->Compare(target, *read_options.iterate_lower_bound) < 0
            || comparator->Compare(target, *read_options.iterate_upper_bound) >= 0) {
          dirty = dirty->link_older;
          continue;
        }

        // found a valid entry
        if (dirty->write_info->deletion_) {
          callback->InvokeDeletion(dirty->key_);
        } else {
          callback->Invoke(dirty->key_, dirty->write_info->value_);
        }
        // track dependency
        if (dirty->txn_id_ == context->self_txn_id) {
          // this kind of value should be seen, but should not be added to dependency list
          printf("Find a dirty version which is inserted by the transaction itself\n");
        } else {
          if(std::find(context->txn_ids.begin(), context->txn_ids.end(), dirty->txn_id_) == context->txn_ids.end()) {
            context->txn_ids.emplace_back(dirty->txn_id_);
          }
        }

        dirty = dirty->link_older;

      }
    }
    return Status::OK();
  }

  Status DirtyBuffer::Remove(const string &key, TransactionID txn_id) {

    // not exclusive on the whole buffer
    ReadLock rl(&buffer_mutex);

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

  Status DirtyBuffer::RemoveScanInfo(TransactionID txn_id) {
    {
      assert(txn_id > 0 && txn_id != ULONG_MAX);
      lock_guard<mutex> scan_list_guard(scan_list_mutex);
      auto it = std::find(scan_list.begin(), scan_list.end(), txn_id);
      if(it != scan_list.end()) {
        scan_list.erase(it);
      }
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

  // normal write operation
  DirtyVersion::DirtyVersion(const string &key, const string &value, SequenceNumber seq, TransactionID txn_id)
      : key_(key), txn_id_(txn_id) {
    is_write = true;
    write_info = new WriteInfo(value, seq);
  }

  // delete operation
  DirtyVersion::DirtyVersion(const string &key, SequenceNumber seq, TransactionID txn_id)
      : key_(key), txn_id_(txn_id) {
    is_write = true;
    write_info = new WriteInfo(seq);
  }

  // normal read operation
  DirtyVersion::DirtyVersion(const string &key, TransactionID txn_id)
    : key_(key), txn_id_(txn_id) {
    is_write = false;
    write_info = nullptr;
  }

  DirtyVersion::~DirtyVersion() {
      delete write_info;
  }

  // normal write operation
  WriteInfo::WriteInfo(const string &value, SequenceNumber seq)
      : value_(value), seq_(seq), deletion_(false) {
  }

  // delete operation
  WriteInfo::WriteInfo(SequenceNumber seq)
      : seq_(seq), deletion_(true) {
  }

  WriteInfo::~WriteInfo() {

  }

}  // namespace rocksdb
