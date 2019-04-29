#include <iostream>
#include <string>
#include "utilities/transactions/transaction_lock_mgr_list.h"

using std::cout;
using std::endl;

using namespace rocksdb;

LockList ll;

std::string msg(bool ret) {
  return ret ? "received lock" : "did NOT receive lock";
}

std::string ex(bool excl) {
  return excl ? "exclusive" : "shared";
}

void grab(TransactionID id, bool exclusive, uint64_t expr,
    std::function<void()> cb) {
  cout << "Transaction " << id << " trying to get " << ex(exclusive);
  cout  << " lock and " << msg(ll.grab(id, exclusive, expr, cb)) << endl;
}

void drop(TransactionID id) {
  cout << "Dropping lock of " << id << endl;
  ll.drop(id);
}

void test1() {
  cout << "Executing test1 -----------------------------------------" << endl;
  LockList ll1;
  ll = ll1;

  grab(123, false, 0, [](){
      cout << "Txn 123 received " << ex(false) << " lock from callback" << endl;
  });

  grab(2, false, 0, [](){
      cout << "Txn 2 received " << ex(false) << " lock from callback" << endl;
  });

  grab(3, true, 0, [](){
      cout << "Txn 3 received " << ex(true) << " lock from callback" << endl;
  });

  grab(4, false, 0, [](){
      cout << "Txn 4 received " << ex(false) << " lock from callback" << endl;
  });

/*  cout << "Txn at waiter head: " << ll.waiters->tid << endl;*/

  //grab(2, true, 0, [](){
      //cout << "Txn 2 received " << ex(true) << " lock from callback" << endl;
  //});

  /*cout << "Txn at waiter head: " << ll.waiters->tid << endl;*/
}

void test2() {
  cout << endl;
  cout << "Executing test2 -----------------------------------------" << endl;
  LockList ll2;
  ll = ll2;

  grab(123, false, 0, [](){
      cout << "Txn 123 received " << ex(false) << " lock from callback" << endl;
  });

  grab(123, true, 0, [](){
      cout << "Txn 123 received " << ex(true) << " lock from callback" << endl;
  });

  grab(3, false, 0, [](){
      cout << "Txn 3 received " << ex(false) << " lock from callback" << endl;
  });
}

void test3() {
  cout << endl;
  cout << "Executing test3 -----------------------------------------" << endl;
  LockList ll3;
  ll = ll3;

  grab(123, false, 0, [](){
      cout << "Txn 123 received " << ex(false) << " lock from callback" << endl;
  });

  grab(123, true, 0, [](){
      cout << "Txn 123 received " << ex(true) << " lock from callback" << endl;
  });

  grab(3, false, 0, [](){
      cout << "Txn 3' received " << ex(false) << " lock from callback" << endl;
  });

  drop(123);
  drop(3);
  cout << "owner, owner_tail, waiters, waiters_tail: " << ll.owners << ", "
    << ll.owners_tail << ", " << ll.waiters << ", " << ll.waiters_tail << endl;
}

void test4() {
  cout << "Executing test4 -----------------------------------------" << endl;
  LockList ll4;
  ll = ll4;

  grab(123, false, 0, [](){
      cout << "Txn 123 received " << ex(false) << " lock from callback" << endl;
  });

  grab(2, false, 0, [](){
      cout << "Txn 2 received " << ex(false) << " lock from callback" << endl;
  });

  grab(3, true, 0, [](){
      cout << "Txn 3 received " << ex(true) << " lock from callback" << endl;
  });

  grab(4, false, 0, [](){
      cout << "Txn 4 received " << ex(false) << " lock from callback" << endl;
  });

  grab(5, false, 0, [](){
      cout << "Txn 5 received " << ex(false) << " lock from callback" << endl;
  });

  drop(123); drop(2); drop(3);
}
int main() {
  test1();
  test2();
  test3();
  test4();
}
