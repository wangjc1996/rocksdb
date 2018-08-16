//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dirty_buffer.h"

//#include <algorithm>
//#include <limits>
//#include <memory>
//
//#include "db/dbformat.h"
//#include "db/merge_context.h"
//#include "db/merge_helper.h"
//#include "db/pinned_iterators_manager.h"
//#include "db/read_callback.h"
//#include "monitoring/perf_context_imp.h"
//#include "monitoring/statistics.h"
//#include "port/port.h"
//#include "rocksdb/comparator.h"
//#include "rocksdb/env.h"
//#include "rocksdb/iterator.h"
//#include "rocksdb/merge_operator.h"
//#include "rocksdb/slice_transform.h"
//#include "rocksdb/write_buffer_manager.h"
//#include "table/internal_iterator.h"
//#include "table/iterator_wrapper.h"
//#include "table/merging_iterator.h"
//#include "util/arena.h"
//#include "util/autovector.h"
//#include "util/coding.h"
//#include "util/memory_usage.h"
//#include "util/murmurhash.h"
//#include "util/mutexlock.h"
//#include "util/util.h"

namespace rocksdb {


  DirtyBuffer::DirtyBuffer(uint32_t column_family_id)
  : column_family_id_(column_family_id) {

  }

  DirtyBuffer::~DirtyBuffer() {

  }

  Status DirtyBuffer::Put(string& key, string& value, SequenceNumber seq, TxnNumber txn_id) {
    std::shared_ptr<DirtyVersion> insert_item(new DirtyVersion(key, value, seq, txn_id));
    map.insert({key, insert_item});
    return Status::OK();
  }

  DirtyVersion::DirtyVersion(string& key, string& value, SequenceNumber seq, TxnNumber txn_id)
      : key_(key),value_(value), seq_(seq), txn_id_(txn_id) {

  }

  DirtyVersion::~DirtyVersion() {

  }

}  // namespace rocksdb
