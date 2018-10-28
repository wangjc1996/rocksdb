#pragma once
#ifndef ROCKSDB_LITE

#include <string>

#include "include/rocksdb/utilities/state_info.h"

namespace rocksdb {
class TransactionStateMgr {
public:
  TransactionStateMgr();
  StateInfoInternal* GetState(uint32_t column_family_id, const std::string& key);

private:
  static constexpr size_t kReservedSlots = 2621440;  // => total size = 4 * 2 * 2621440 = 20 MB
  StateInfoInternal states_[kReservedSlots];

  // No copying allowed
  TransactionStateMgr(const TransactionStateMgr&) = delete;
  void operator=(const TransactionStateMgr&) = delete;
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
