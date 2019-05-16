
#pragma once

#ifndef ROCKSDB_LITE

#include <string>
#include <vector>

#include "rocksdb/status.h"

namespace rocksdb {

using std::string;
using std::vector;

class DirtyBufferScanCallback {
public:
  virtual ~DirtyBufferScanCallback() {}

  // Will be called when a write record is found when scaning the dirty buffer
  virtual Status Invoke(const string &key, const string &value, const uint64_t dep_txn_id) = 0;

  // Will be called when a delete record is found when scaning the dirty buffer
  virtual Status InvokeDeletion(const string &key) = 0;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
