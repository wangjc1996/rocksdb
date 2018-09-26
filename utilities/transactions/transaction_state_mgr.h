#pragma once
#ifndef ROCKSDB_LITE

#include <chrono>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <atomic>

#include "monitoring/instrumented_mutex.h"
#include "util/autovector.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"
#include "util/hash_map.h"
#include "util/thread_local.h"

namespace rocksdb {
struct StateInfo {
  StateInfo(std::atomic<uint64_t>* handle) : handle_(handle) {}

  void IncreaseRead(bool optimistic);
  void DecreaseRead(bool optimisitc);;
  void IncreaseWrite(bool optimistic);
  void DecreaseWrite(bool optimisitc);;

  private:
    std::atomic<uint64_t>* handle_;
    static const uint64_t kBaseMask;
    static const uint64_t kOptimisticReadMask;
    static const uint64_t kOptimisticWriteMask;
    static const uint64_t kPessimisticReadMask;
    void IncreaseImpl(uint64_t mask, size_t offset);
    void DecreaseImpl(uint64_t mask, size_t offset);
};

struct StateMap;
struct StateMapStripe;
class Slice;

namespace {
void UnrefStateMapsCache(void* ptr) {
  // Called when a thread exits or a ThreadLocalPtr gets destroyed.
  auto state_maps_cache =
      static_cast<std::unordered_map<uint32_t, std::shared_ptr<StateMap>>*>(ptr);
  delete state_maps_cache;
}
}  // anonymous namespace

class TransactionStateMgr {
 public:
  TransactionStateMgr(size_t default_num_stripes, 
    std::shared_ptr<TransactionDBMutexFactory> mutex_factory)
    : default_num_stripes_(default_num_stripes),
      state_maps_cache_(new ThreadLocalPtr(&UnrefStateMapsCache)),
      mutex_factory_(mutex_factory) {}

  ~TransactionStateMgr();

  // Creates a new StateMap for this column family.  Caller should guarantee
  // that this column family does not already exist.
  void AddColumnFamily(uint32_t column_family_id);

  // Deletes the StateMap for this column family.  Caller should guarantee that
  // this column family is no longer in use.
  void RemoveColumnFamily(uint32_t column_family_id);

  StateInfo GetState(uint32_t column_family_id, const std::string& key);

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
  using StateMaps = std::unordered_map<uint32_t, std::shared_ptr<StateMap>>;
  StateMaps state_maps_;

  // Thread-local cache of entries in state_maps_.  This is an optimization
  // to avoid acquiring a mutex in order to look up a StateMap
  std::unique_ptr<ThreadLocalPtr> state_maps_cache_;

  // Used to allocate mutexes/condvars to use when locking keys
  std::shared_ptr<TransactionDBMutexFactory> mutex_factory_;

  std::shared_ptr<StateMap> GetStateMap(uint32_t column_family_id);

  // No copying allowed
  TransactionStateMgr(const TransactionStateMgr&);
  void operator=(const TransactionStateMgr&);
};

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
