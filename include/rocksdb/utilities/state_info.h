#pragma once
namespace rocksdb {

static constexpr uint8_t kTotalStates = 3;

static constexpr uint8_t kOptimisticCleanReadIndex = 0;
static constexpr uint8_t kOptimisticDirtyReadIndex = 1;
static constexpr uint8_t kOptimisticWriteIndex = 2;

template <bool read, bool is_dirty>
static constexpr uint8_t GetStateIndex() {
  return read
         ? (is_dirty ? kOptimisticDirtyReadIndex : kOptimisticCleanReadIndex)
         : kOptimisticWriteIndex;
}

using StateUnit = uint16_t;
using StateInfoInternal = StateUnit[kTotalStates];

struct StateInfo {
  StateInfoInternal* handle;

  StateInfo(StateInfoInternal* info) : handle(info) {}
  StateInfo() : StateInfo(nullptr) {}

  void SetHandle(StateInfoInternal* h) { handle = h; }

  template <bool read, bool is_dirty>
  inline void IncreaseAccess() {
    constexpr uint8_t index = GetStateIndex<read, is_dirty>();
#define atomic_inc(P) __sync_add_and_fetch((P), 1)
    atomic_inc(((StateUnit*)handle) + index);
#undef atomic_inc
  }

  template <bool read, bool is_dirty>
  inline void DecreaseAccess() {
    constexpr uint8_t index = GetStateIndex<read, is_dirty>();
#define atomic_dec(P) __sync_add_and_fetch((P), -1) 
    atomic_dec(((StateUnit*)handle) + index);
#undef atomic_dec
  }
};
}
