#pragma once
#include <atomic>

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
    static const uint64_t kPessimisticWriteMask;
    void IncreaseImpl(uint64_t mask, size_t offset);
    void DecreaseImpl(uint64_t mask, size_t offset);
};
}
