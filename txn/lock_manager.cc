
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"
using namespace std;

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  // if deque not found, create new deque and add Lock Request
  if (lock_table_.find(key) == lock_table_.end()) {
    deque<LockRequest> *new_deque = new deque<LockRequest>();
    new_deque->push_back(LockRequest(EXCLUSIVE, txn));
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, new_deque));

    // signal instant lock access
    txn_waits_[txn] = 0;
    return true;
  } 
  // if deque found, add Lock Request to deque
  else {
    deque<LockRequest> *lock_deq = lock_table_[key];
    lock_deq->push_back(LockRequest(EXCLUSIVE, txn));

    // if this is the only lock request
    if (lock_deq->size() == 1) {
      // signal instant lock access
      txn_waits_[txn] = 0;
      return true;
    } 
    // if there are other lock requests
    else {
      // signal wait
      txn_waits_[txn]++;
      return false;
    }
  }
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  // if deque not found, return
  if (lock_table_.find(key) == lock_table_.end()) {
    return;
  } 
  // if deque found
  else {
    deque<LockRequest> *lock_deq = lock_table_[key];

    // if deque is empty, return
    if (lock_deq->size() == 0) {
      return;
    } 
    // if deque is not empty
    else {
      // if txn is not the front of the deque, make txn a zombie
      if (lock_deq->front().txn_ != txn) {
        // make txn a zombie
        txn_waits_.erase(txn);
        deque<LockRequest>::iterator it = lock_deq->begin();
        for (; it != lock_deq->end(); it++) {
          if (it->txn_ == txn) {
            lock_deq->erase(it);
            break;
          }
        }
        return;
      } 
      // if txn is the front of the deque
      else {
        // pop front of deque
        lock_deq->pop_front();

        // if deque is empty, remove deque from lock table
        if (lock_deq->size() == 0) {
          lock_table_.erase(key);
        } 
        // if deque is not empty
        else {
          while (lock_deq->size() > 0) {
            // if next txn is a zombie, remove it
            if (txn_waits_.find(lock_deq->front().txn_) == txn_waits_.end()) {
              lock_deq->pop_front();
              continue;
            } 
            // if next txn is not a zombie, signal instant lock access
            else {
              if (--txn_waits_[lock_deq->front().txn_] == 0) {
                ready_txns_->push_back(lock_deq->front().txn_);
              }
              break;
            }
          }
        }
      }
    }
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  // Clear owners vector
  owners->clear();

  // if deque not found, return UNLOCKED
  if (lock_table_.find(key) == lock_table_.end()) {
    return UNLOCKED;
  } 
  // if deque found
  else {
    deque<LockRequest> *lock_deq = lock_table_[key];

    // if deque is empty, return UNLOCKED
    if (lock_deq->size() == 0) {
      return UNLOCKED;
    } 
    // if deque is not empty, return EXCLUSIVE
    else {
      LockRequest lock = lock_deq->front();
      owners->push_back(lock.txn_);
      return EXCLUSIVE;
    }
  }
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  if (lock_table_.find(key) == lock_table_.end()) {
    deque<LockRequest> *new_deque = new deque<LockRequest>();
    new_deque->push_back(LockRequest(EXCLUSIVE, txn));
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, new_deque));

    // signal instant lock access
    txn_waits_[txn] = 0;
    return true;
  } else {
    deque<LockRequest> *lock_deq = lock_table_[key];
    lock_deq->push_back(LockRequest(EXCLUSIVE, txn));

    // if this is the only lock request
    if (lock_deq->size() == 1) {
      // signal instant lock access
      txn_waits_[txn] = 0;
      return true;
    } else {
      // signal wait
      txn_waits_[txn]++;
      return false;
    }
  }
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // if deque not found, create new deque and add Lock Request
  if (lock_table_.find(key) == lock_table_.end()) {
    deque<LockRequest> *new_deque = new deque<LockRequest>();
    new_deque->push_back(LockRequest(SHARED, txn));
    lock_table_.insert(pair<Key, deque<LockRequest>*>(key, new_deque));

    // signal instant lock access
    txn_waits_[txn] = 0;
    return true;
  } 
  // if deque found
  else {
    deque<LockRequest> *lock_deq = lock_table_[key];
    lock_deq->push_back(LockRequest(SHARED, txn));

    // if this is the only lock request
    if (lock_deq->size() == 1) {
      // signal instant lock access
      txn_waits_[txn] = 0;
      return true;
    }
    // if there are other lock requests
    else {
      // Check if there is a write lock request
      deque<LockRequest>::iterator it = lock_deq->begin();
      for (; it != lock_deq->end(); it++) {
        if (it->mode_ == EXCLUSIVE) {
          // signal wait
          txn_waits_[txn]++;
          return false;
        }
      }

      // signal instant lock access (Only shared locks exist)
      txn_waits_[txn] = 0;
      return true;
    }
  }
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  deque<LockRequest> *lock_deq = lock_table_[key];

  deque<LockRequest>::iterator it = lock_deq->begin();
  for (; it != lock_deq->end(); it++) {
    if (it->txn_ == txn) {
      lock_deq->erase(it);
      break;
    }
  }
  
  vector<Txn*> owners; Status(key, &owners);

  for (auto owner : owners) {
    auto txn = txn_waits_.find(owner);
    if (txn != txn_waits_.end() && --(txn->second) == 0) {
      ready_txns_->push_back(owner);
      txn_waits_.erase(txn);
    }
  }
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  owners->clear();

  // if deque not found, return UNLOCKED
  if (lock_table_.find(key) == lock_table_.end()) {
    return UNLOCKED;
  }
  // if deque found
  else {
    deque<LockRequest> *lock_deq = lock_table_[key];

    // if deque is empty, return UNLOCKED
    if (lock_deq->size() == 0) {
      return UNLOCKED;
    }
    // if deque is not empty
    else {
      deque<LockRequest>::iterator it = lock_deq->begin();

      // Check if current lock is exclusive
      if (it->mode_ == EXCLUSIVE) {
        owners->push_back(it->txn_);
        return EXCLUSIVE;
      }
      // Return shared lock
      else{
        for (; it != lock_deq->end() && it->mode_ != EXCLUSIVE; it++) {
          owners->push_back(it->txn_);
        }
        return SHARED;
      }
    }
  }
}