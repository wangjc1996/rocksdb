//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/validation_map.h"

namespace rocksdb {

  ValidationMap::ValidationMap(uint32_t column_family_id)
      : column_family_id_(column_family_id) {

  }

  ValidationMap::~ValidationMap() {

  }

  Status ValidationMap::Put(const Slice& key, SequenceNumber seq) {
    WriteLock wl(&map_mutex_);
    map[key.ToString()] = seq;
    return Status::OK();
  }

  SequenceNumber ValidationMap::GetLatestSequenceNumber(const Slice &key) {
    ReadLock rl(&map_mutex_);
    if (map.find(key.ToString()) != map.end()) {
      return map.at(key.ToString());
    }
    return kMaxSequenceNumber;
  }

}  // namespace rocksdb
