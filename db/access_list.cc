//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/access_list.h"

namespace rocksdb {


  AccessList::AccessList(uint32_t column_family_id)
      : column_family_id_(column_family_id),
        locks_(10000) {

  }

  AccessList::~AccessList() {

  }

  Status AccessList::Add(const Slice &key, PessimisticTransaction *txn, SequenceNumber seq, TransactionID txn_id) {
    WriteLock wl(GetLock(key.ToString()));
    auto *current = new AccessInfo(txn, seq, txn_id);
    auto *header = map[key.ToString()];
    if (header == nullptr) {
      map[key.ToString()] = current;
    } else {
      current->link_older = header;
      header->link_newer = current;
      map[key.ToString()] = current;
    }
    return Status::OK();
  }

  PessimisticTransaction* AccessList::Get(const Slice &key) {
    ReadLock rl(GetLock(key.ToString()));
    auto it = map.find(key.ToString());
    if (it != map.end()) {
      AccessInfo *info = it->second;
      return info->GetTxn();
    }
    assert(false);
    return nullptr;
  }

  Status AccessList::Remove(const Slice &key, TransactionID txn_id) {
    WriteLock wl(GetLock(key.ToString()));
    auto it = map.find(key.ToString());
    if (it != map.end()) {
      AccessInfo *dirty = it->second;
      while (dirty != nullptr) {
        TransactionID stored_txn_id = dirty->GetTxnId();
        if (stored_txn_id != txn_id) {
          dirty = dirty->link_older;
          continue;
        }
        if (dirty->link_newer == nullptr) {
          //head of the linked list
          auto *new_header = dirty->link_older;
          if (new_header == nullptr) {
            map.erase(it);
            break;
          } else {
            map[key.ToString()] = new_header;
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
        dirty = dirty->link_older;
      }
    } else {
      assert(false);
    }
    return Status::OK();
  }

  port::RWMutex* AccessList::GetLock(const Slice &key) {
    static murmur_hash hash;
    return &locks_[hash(key) % locks_.size()];
  }

  AccessInfo::AccessInfo(PessimisticTransaction *txn, SequenceNumber seq, TransactionID txn_id)
      : txn_(txn), seq_(seq), txn_id_(txn_id) {

  }

  AccessInfo::~AccessInfo() {

  }

}  // namespace rocksdb
