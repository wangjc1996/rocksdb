#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/transaction_state_mgr.h"

#include <inttypes.h>

#include <algorithm>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "monitoring/perf_context_imp.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction_db_mutex.h"
#include "util/cast_util.h"
#include "util/murmurhash.h"
#include "util/sync_point.h"
#include "util/thread_local.h"
#include "utilities/transactions/pessimistic_transaction_db.h"

namespace rocksdb {

//  state bitset struct:
// 64: tpl write (one bit since it's exclusive)
// 63 - 43: tpl read
// 42 - 22: occ write
// 21 - 1: occ read
const uint64_t StateInfo::kBaseMask = 0x1FFFFF;
const uint64_t StateInfo::kOptimisticReadMask = kBaseMask;
const uint64_t StateInfo::kOptimisticWriteMask = kBaseMask << 16;
const uint64_t StateInfo::kPessimisticReadMask = kBaseMask << 32;
const uint64_t StateInfo::kPessimisticWriteMask = kBaseMask << 48;

struct StateMapStripe {
  explicit StateMapStripe(std::shared_ptr<TransactionDBMutexFactory> factory) {
    stripe_mutex = factory->AllocateMutex();
    stripe_cv = factory->AllocateCondVar();
    assert(stripe_mutex);
  }

  std::shared_ptr<TransactionDBMutex> stripe_mutex;

  std::unordered_map<std::string, std::atomic<uint64_t>> keys;
};

// Map of #num_stripes StateMapStripes
struct StateMap {
  explicit StateMap(size_t num_stripes,
                   std::shared_ptr<TransactionDBMutexFactory> factory)
      : num_stripes_(num_stripes) {
    state_map_stripes_.reserve(num_stripes);
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

void StateInfo::IncreaseImpl(uint64_t mask, size_t offset) {
  uint64_t old_val = handle_->load();
  while (true) {
    uint64_t count = (old_val & mask) >> offset;
    count++;
    assert(count <= kBaseMask);
    uint64_t new_val = (old_val & ~mask) | (count << offset);
    if (handle_->compare_exchange_weak(old_val, new_val)) {
      break;
    }
  }
}

void StateInfo::DecreaseImpl(uint64_t mask, size_t offset) {
  uint64_t old_val = handle_->load();
  while (true) {
    uint64_t count = (old_val & mask) >> offset;
    assert(count > 0);
    count--;
    uint64_t new_val = (old_val & ~mask) | (count << offset);
    if (handle_->compare_exchange_weak(old_val, new_val)) {
      break;
    }
  }
}

void StateInfo::IncreaseRead(bool optimistic) {
  if (optimistic) {
    IncreaseImpl(kOptimisticReadMask, 0);
  } else {
    IncreaseImpl(kPessimisticReadMask, 32);
  }
}

void StateInfo::IncreaseWrite(bool optimistic) {
  if (optimistic) {
    IncreaseImpl(kOptimisticWriteMask, 16);
  } else {
    IncreaseImpl(kPessimisticWriteMask, 48);
  }
}

void StateInfo::DecreaseRead(bool optimistic) {
  if (optimistic) {
    DecreaseImpl(kOptimisticReadMask, 0);
  } else {
    DecreaseImpl(kPessimisticReadMask, 32);
  }
}

void StateInfo::DecreaseWrite(bool optimistic) {
  if (optimistic) {
    DecreaseImpl(kOptimisticWriteMask, 16);
  } else {
    DecreaseImpl(kPessimisticWriteMask, 48);
  }
}

TransactionStateMgr::~TransactionStateMgr() {}

size_t StateMap::GetStripe(const std::string& key) const {
  assert(num_stripes_ > 0);
  static murmur_hash hash;
  size_t stripe = hash(key) % num_stripes_;
  return stripe;
}

void TransactionStateMgr::AddColumnFamily(uint32_t column_family_id) {
  InstrumentedMutexLock l(&state_map_mutex_);

  if (state_maps_.find(column_family_id) == state_maps_.end()) {
    state_maps_.emplace(column_family_id,
                       std::shared_ptr<StateMap>(
                           new StateMap(default_num_stripes_, mutex_factory_)));
  } else {
    // column_family already exists in lock map
    assert(false);
  }
}

void TransactionStateMgr::RemoveColumnFamily(uint32_t column_family_id) {
  // Remove lock_map for this column family.  Since the lock map is stored
  // as a shared ptr, concurrent transactions can still keep using it
  // until they release their references to it.
  {
    InstrumentedMutexLock l(&state_map_mutex_);

    auto state_maps_iter = state_maps_.find(column_family_id);
    assert(state_maps_iter != state_maps_.end());

    state_maps_.erase(state_maps_iter);
  }  // lock_map_mutex_

  // Clear all thread-local caches
  autovector<void*> local_caches;
  state_maps_cache_->Scrape(&local_caches, nullptr);
  for (auto cache : local_caches) {
    delete static_cast<StateMaps*>(cache);
  }
}

StateInfo TransactionStateMgr::GetState(uint32_t column_family_id, const std::string& key) {
  std::shared_ptr<StateMap> state_map_ptr = GetStateMap(column_family_id);
  StateMap* state_map = state_map_ptr.get();
  assert(state_map != nullptr);
  // Need to lock the mutex for the stripe that this key hashes to
  size_t stripe_num = state_map->GetStripe(key);
  assert(state_map->state_map_stripes_.size() > stripe_num);
  StateMapStripe* stripe = state_map->state_map_stripes_.at(stripe_num);

  stripe->stripe_mutex->Lock();

  auto stripe_iter = stripe->keys.find(key);

  if (stripe_iter == stripe->keys.end()) {
    stripe->keys.emplace(std::piecewise_construct, 
                        std::forward_as_tuple(key), 
                        std::forward_as_tuple(0));
    stripe_iter = stripe->keys.find(key);
  }
  assert(stripe_iter != stripe->keys.end());
  stripe->stripe_mutex->UnLock();

  return StateInfo(&stripe_iter->second);
}

// Look up the StateMap shared_ptr for a given column_family_id.
// Note:  The StateMap is only valid as long as the caller is still holding on
//   to the returned shared_ptr.
std::shared_ptr<StateMap> TransactionStateMgr::GetStateMap(
    uint32_t column_family_id) {
  // First check thread-local cache
  if (state_maps_cache_->Get() == nullptr) {
    state_maps_cache_->Reset(new StateMaps());
  }

  auto state_maps_cache = static_cast<StateMaps*>(state_maps_cache_->Get());

  auto state_map_iter = state_maps_cache->find(column_family_id);
  if (state_map_iter != state_maps_cache->end()) {
    // Found lock map for this column family.
    return state_map_iter->second;
  }

  // Not found in local cache, grab mutex and check shared StateMaps
  InstrumentedMutexLock l(&state_map_mutex_);

  state_map_iter = state_maps_.find(column_family_id);
  if (state_map_iter == state_maps_.end()) {
    return std::shared_ptr<StateMap>(nullptr);
  } else {
    // Found lock map.  Store in thread-local cache and return.
    std::shared_ptr<StateMap>& state_map = state_map_iter->second;
    state_maps_cache->insert({column_family_id, state_map});

    return state_map;
  }
}
}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
