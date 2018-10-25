#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/transaction_state_mgr.h"

#include <memory.h>
#include <string>

#include "util/murmurhash.h"

namespace rocksdb {
TransactionStateMgr::TransactionStateMgr() {
  memset(&states_, 0, sizeof(states_));
}

StateInfoInternal* TransactionStateMgr::GetState(uint32_t column_family_id, const std::string& key) {
  static murmur_hash hash;
  return &states_[(hash(key) + column_family_id) % kReservedSlots];
}
}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
