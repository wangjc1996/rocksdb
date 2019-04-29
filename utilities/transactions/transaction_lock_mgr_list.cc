#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/transaction_lock_mgr_list.h"
#include <iostream>

using std::cout;
using std::endl;

namespace rocksdb {

bool LockList::grab(TransactionID id, bool exclusive, uint64_t new_expr_time,
        std::function<void()> callback) {
    LockEntry* owner_ptr = find_owner_tid(id);

    // Already held
    if (owner_ptr != nullptr) {
        cout << "Txn " << id << " reaquired lock " << key << endl;
        //cout << "Found owner " << owner_ptr->tid << endl;
        // `owner_ptr being not null means `id owns the lock. If it owns it in
        // exclusive mode then the requested mode is irrelevant, if the request
        // is shared then the held lock type regardless can accommodate it.
        if (owner_ptr->type == lExclusive || !exclusive) {
            expiration_time = std::max(expiration_time, new_expr_time);
            return true;
        }

        // Lock held in shared mode but requested in exclusive: needs upgrade
        owner_ptr->type = lExclusive;
        owner_ptr->callback = callback;

        if (owners->tid == id && owners->next == nullptr) {
            // Only owner is tid, can perform upgrade
            expiration_time = std::max(expiration_time, new_expr_time);
            holder_type = lExclusive;
            return true;
        } else {
            // TODO, FIXME (Conrad): This can lead to ACID violations
            assert(false);

            owner_ptr->expiration_time = new_expr_time;
            LIST_REMOVE(owners, owners_tail, owner_ptr);
            LIST_PUT_HEAD(waiters, waiters_tail, owner_ptr);
            return false;
        }
    } else {
        // If already in waiters make sure lock type checks out and return
        // false for lock still not held
        for (LockEntry* head = waiters; head != nullptr; head = head->next) {
            if (head->tid == id) {
                cout << "Txn " << id << " double requested lock for " << 
                    key << endl;
                assert(exclusive == (head->type == lExclusive));
                return false;
            }
        }
    }

    LockEntry* entry = new LockEntry(id, new_expr_time, exclusive, callback);

    if (holder_type == lNotHeld) {
        holder_type = entry->type;
        expiration_time = entry->expiration_time;
        LIST_PUT_HEAD(owners, owners_tail, entry);
        cout << "Txn " << id << " grabbing unheld lock " << key << endl;
        return true;
    } else if (!exclusive && holder_type == lShared && nowaiters()) {
        expiration_time = std::max(expiration_time, entry->expiration_time);
        LIST_PUT_TAIL(owners, owners_tail, entry);
        cout << "Txn " << id << " grabbing shared lock " << key << endl;
        return true;
    } else {
        LIST_PUT_TAIL(waiters, waiters_tail, entry);
        cout << "Txn " << id << " waiting for lock " << key << endl;
        return false;
    }

}

bool LockList::drop(TransactionID id) {
    LockEntry* owner_ptr = find_owner_tid(id);
    assert(owner_ptr != nullptr);

    cout << "Txn " << id << " dropping lock " << key << endl;
    LIST_REMOVE(owners, owners_tail, owner_ptr);
    if (owners == nullptr && waiters != nullptr) {
        assert(owners_tail == nullptr && waiters_tail != nullptr);

        LockEntry* en;
        do { // Grant one exclusive lock or as many shared as are at head
            LIST_GET_HEAD(waiters, waiters_tail, en);
            expiration_time = std::max(expiration_time, en->expiration_time);
            holder_type = en->type;
            en->grant_lock();
            cout << "Txn " << id << " giving lock " << key <<  " to " << en->tid << endl;
            LIST_PUT_TAIL(owners, owners_tail, en);
        } while (waiters != nullptr && 
                waiters->type == lShared &&
                holder_type == lShared);
    } else if (owners == nullptr && waiters == nullptr) {
        holder_type = lNotHeld;
    }

    delete owner_ptr;
    return true;
}

void LockList::fill_auto(autovector<TransactionID>* auto_) {
    for (LockEntry* en = owners; en != nullptr; en = en->next)
        auto_->push_back(en->tid);
}

LockEntry* LockList::find_owner_tid(TransactionID id) {
    for (LockEntry* head = owners; head != nullptr; head = head->next)
        if (head->tid == id)
            return head;
    return nullptr;
}

} // namespace rocksdb

#endif // ROCKSDB_LITE
