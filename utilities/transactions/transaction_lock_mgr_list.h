#pragma once
#ifndef ROCKSDB_LITE

#include <functional>

#include "rocksdb/utilities/transaction.h"
#include "util/autovector.h"

// Taken from DBx1000
/************************************************/
// LIST helper (read from head & write to tail)
/************************************************/
#define LIST_GET_HEAD(lhead, ltail, en) {\
  en = lhead; \
  lhead = lhead->next; \
  if (lhead) lhead->prev = NULL; \
  else ltail = NULL; \
  en->next = NULL; }
#define LIST_PUT_TAIL(lhead, ltail, en) {\
  en->next = NULL; \
  en->prev = NULL; \
  if (ltail) { en->prev = ltail; ltail->next = en; ltail = en; } \
  else { lhead = en; ltail = en; }}
#define LIST_PUT_HEAD(lhead, ltail, en) {\
  en->next = NULL; \
  en->prev = NULL; \
  if (lhead) { en->next = lhead; lhead->prev = en; lhead = en; } \
  else { lhead = en; ltail = en; }}
#define LIST_REMOVE(lhead, ltail, entry) { \
  if (entry->next) entry->next->prev = entry->prev; \
  else ltail = entry->prev; \
  if (entry->prev) entry->prev->next = entry->next; \
  else lhead = entry->next; }

namespace rocksdb {

static int KEY = 0;

enum LockType : unsigned char {
  lNotHeld = 0,
  lShared = 1,
  lExclusive = 2
};

struct LockEntry {
  LockType type;
  TransactionID tid;
  uint64_t expiration_time;
  std::function<void()> callback;

  LockEntry* next = nullptr;
  LockEntry* prev = nullptr;

  void grant_lock() { callback(); }

  LockEntry(TransactionID tid_, uint64_t time, bool ex,
      std::function<void()> callback_)
    : type(ex ? lExclusive : lShared), tid(tid_), expiration_time(time),
      callback(callback_) {}
};

struct LockList {
  LockType holder_type = lNotHeld;
  LockEntry* owners = nullptr;
  LockEntry* waiters = nullptr;
  LockEntry* owners_tail = nullptr;
  LockEntry* waiters_tail = nullptr;
  uint64_t expiration_time = 0;
  
  bool nowaiters() { return waiters == nullptr; }
  bool grab(TransactionID id, bool exclusive, uint64_t new_expr_time,
      std::function<void()> callback);
  bool drop(TransactionID id, bool special = false);
  void fill_auto(autovector<TransactionID>* auto_);

  int key;
  LockList() {
      key = ++KEY;
  }
private:
  LockEntry* find_owner_tid(TransactionID id);
};

} // namespace rocksdb

#endif // ROCKSDB_LITE
