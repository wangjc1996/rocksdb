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

#include "db/dbformat.h"
#include "util/murmurhash.h"
#include "util/mutexlock.h"

namespace rocksdb {

  class AccessInfo;
  class PessimisticTransaction;

  typedef uint64_t SequenceNumber;
  using TransactionID = uint64_t;

  using std::string;

  class AccessList {
  public:

    AccessList(uint32_t column_family_id);

    ~AccessList();

    Status Add(const Slice &key, PessimisticTransaction* txn, SequenceNumber seq, TransactionID txn_id);

    PessimisticTransaction* Get(const Slice &key);

    Status Remove(const Slice &key, TransactionID txn_id);

  private:
    uint32_t column_family_id_;
    std::vector<port::RWMutex> locks_;
    std::unordered_map<string, AccessInfo*> map;

    port::RWMutex* GetLock(const Slice &key);

    // No copying allowed
    AccessList(const AccessList &);
    AccessList &operator=(const AccessList &);
  };

  class AccessInfo {
  public:

    explicit AccessInfo(PessimisticTransaction* txn, SequenceNumber seq, TransactionID txn_id);

    ~AccessInfo();

    inline SequenceNumber GetSeq() { return seq_; };

    inline PessimisticTransaction *GetTxn() { return txn_; };

    inline TransactionID GetTxnId() { return txn_id_; };

  private:

    friend class AccessList;

    PessimisticTransaction *txn_;
    SequenceNumber seq_;
    TransactionID txn_id_;

    AccessInfo* link_older = nullptr;
    AccessInfo* link_newer = nullptr;

    // No copying allowed
    AccessInfo(const AccessInfo &);

    AccessInfo &operator=(const AccessInfo &);

  };

}  // namespace rocksdb
