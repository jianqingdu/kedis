//
//  key_lock.h
//  kedis
//
//  Created by ziteng on 17/7/25.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __KEY_LOCK_H__
#define __KEY_LOCK_H__

#include "util.h"

// key lock is used when multiple thread are operating on the same key in complex structure
void init_key_lock();

void lock_key(int db_idx, const string& key);
void unlock_key(int db_idx, const string& key);

// keys are in set, so they are sorted and unique, which prevent deadlock
void lock_keys(int db_idx, set<string>& keys);
void unlock_keys(int db_idx, set<string>& keys);

void spinlock_db(int db_idx);
void lock_db(int db_idx);
void unlock_db(int db_idx);

// when get snapshot/flushdb we need block/freeze all threads use these functions
void spinlock_all();
void lock_all();
void unlock_all();

class KeyLockGuard {
public:
    KeyLockGuard(int db_idx, const string& key) : db_idx_(db_idx), key_(key) {
        lock_key(db_idx_, key_);
    }
    ~KeyLockGuard() {
        unlock_key(db_idx_, key_);
    }
private:
    int db_idx_;
    string  key_;
    //bool incr_;
};

#endif /* __KEY_LOCK_H__ */
