#pragma once
#ifndef ROCKSDB_LITE

#include <chrono>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <atomic>

#include "include/rocksdb/utilities/state_info.h"
#include "monitoring/instrumented_mutex.h"
#include "util/autovector.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"
#include "util/hash_map.h"
#include "util/thread_local.h"

namespace rocksdb {

struct StateMapStripe {
  explicit StateMapStripe(std::shared_ptr<TransactionDBMutexFactory> factory) {
    stripe_mutex = factory->AllocateMutex();
    assert(stripe_mutex);
  }

  std::shared_ptr<TransactionDBMutex> stripe_mutex;

  std::unordered_map<std::string, std::atomic<uint64_t>> keys;
};

struct StateMap {
  explicit StateMap(size_t num_stripes, std::shared_ptr<TransactionDBMutexFactory> factory)
      : num_stripes_(num_stripes) {
    state_map_stripes_.reserve(num_stripes_);
    for (size_t i = 0; i < num_stripes; i++) {
      StateMapStripe* stripe = new StateMapStripe(factory);
      state_map_stripes_.push_back(stripe);
    }
  }

  ~StateMap() {
    for (auto stripe : state_map_stripes_) {
      delete stripe;
    }
  }

  // Number of sepearate StateMapStripes to create, each with their own Mutex
  const size_t num_stripes_;

  std::vector<StateMapStripe*> state_map_stripes_;

  size_t GetStripe(const std::string& key) const;
};

class TransactionStateMgr {
 public:
  TransactionStateMgr(size_t default_num_stripes, 
    std::shared_ptr<TransactionDBMutexFactory> mutex_factory)
    : default_num_stripes_(default_num_stripes),
      mutex_factory_(mutex_factory) {}

  ~TransactionStateMgr();

  // Creates a new StateMap for this column family.  Caller should guarantee
  // that this column family does not already exist.
  void AddColumnFamily(uint32_t column_family_id);

  // Deletes the StateMap for this column family.  Caller should guarantee that
  // this column family is no longer in use.
  void RemoveColumnFamily(uint32_t column_family_id);

  std::atomic<uint64_t>* GetState(uint32_t column_family_id, const std::string& key);

 private:
  // Default number of lock map stripes per column family
  const size_t default_num_stripes_;

  // The following lock order must be satisfied in order to avoid deadlocking
  // ourselves.
  //   - state_map_mutex_
  //   - stripe mutexes in ascending cf id, ascending stripe order
  //   - wait_txn_map_mutex_
  //
  // Must be held when accessing/modifying state_maps_.
  InstrumentedMutex state_map_mutex_;

  // Map of ColumnFamilyId to locked key info
  using StateMaps = std::unordered_map<uint32_t, StateMap*>;
  StateMaps state_maps_;

  // Used to allocate mutexes/condvars to use when locking keys
  std::shared_ptr<TransactionDBMutexFactory> mutex_factory_;

  StateMap* GetStateMap(uint32_t column_family_id);

  // No copying allowed
  TransactionStateMgr(const TransactionStateMgr&);
  void operator=(const TransactionStateMgr&);
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
