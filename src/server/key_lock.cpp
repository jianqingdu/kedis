//
//  key_lock.cpp
//  kedis
//
//  Created by ziteng on 17/7/25.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "key_lock.h"
#include "server.h"

class KeyLock {
public:
    KeyLock() {
        ref_count_ = 0;
    }
    ~KeyLock() {}
    
    void IncrRef() {
        ref_count_++;
    }
    void DecrRef() {
        ref_count_--;
    }
    int GetRef() {
        return ref_count_;
    }
    
    void Lock() {
        key_mtx_.lock();
    }
    void Unlock() {
        key_mtx_.unlock();
    }
private:
    int ref_count_;
    mutex key_mtx_;
};

struct KeyLockMap {
    map<string, KeyLock*> key_lock_map;
    list<KeyLock*> key_lock_pool;
    mutex map_mutex;
};
vector<KeyLockMap*> g_key_lock_vector;

void init_key_lock()
{
    for (int i = 0; i < g_server.db_num; i++) {
        KeyLockMap* lock_map = new KeyLockMap;
        g_key_lock_vector.push_back(lock_map);
    }
}

void lock_key(int db_idx, const string& key)
{
    KeyLock* kl = NULL;
    KeyLockMap* kl_map = g_key_lock_vector[db_idx];
    kl_map->map_mutex.lock();
    g_server.operating_count_vec[db_idx]++;

    auto it = kl_map->key_lock_map.find(key);
    if (it == kl_map->key_lock_map.end()) {
        if (kl_map->key_lock_pool.empty()) {
            kl = new KeyLock();
        } else {
            kl = kl_map->key_lock_pool.front();
            kl_map->key_lock_pool.pop_front();
        }
        kl_map->key_lock_map.insert(make_pair(key, kl));
    } else {
        kl = it->second;
    }
    
    kl->IncrRef();
    kl_map->map_mutex.unlock();
    
    kl->Lock();
}

void unlock_key(int db_idx, const string& key)
{
    g_server.operating_count_vec[db_idx]--;
    
    KeyLockMap* kl_map = g_key_lock_vector[db_idx];
    kl_map->map_mutex.lock();
    auto it = kl_map->key_lock_map.find(key);
    if (it != kl_map->key_lock_map.end()) {
        KeyLock* kl = it->second;
        kl->DecrRef();
        kl->Unlock();
        if (kl->GetRef() == 0) {
            kl_map->key_lock_map.erase(key);
            kl_map->key_lock_pool.push_back(kl);
        }
    }
    
    kl_map->map_mutex.unlock();
}

void lock_keys(int db_idx, set<string>& keys)
{
    list<KeyLock*> key_lock_list;
    KeyLockMap* kl_map = g_key_lock_vector[db_idx];
    kl_map->map_mutex.lock();
    g_server.operating_count_vec[db_idx]++;
    
    for (auto it_key = keys.begin(); it_key != keys.end(); it_key++) {
        const string& key = *it_key;
        KeyLock* kl = NULL;
        auto it = kl_map->key_lock_map.find(key);
        if (it == kl_map->key_lock_map.end()) {
            if (kl_map->key_lock_pool.empty()) {
                kl = new KeyLock();
            } else {
                kl = kl_map->key_lock_pool.front();
                kl_map->key_lock_pool.pop_front();
            }
            kl_map->key_lock_map.insert(make_pair(key, kl));
        } else {
            kl = it->second;
        }
        
        key_lock_list.push_back(kl);
        kl->IncrRef();
    }
    
    kl_map->map_mutex.unlock();
    
    for (auto it = key_lock_list.begin(); it != key_lock_list.end(); it++) {
        KeyLock* kl = *it;
        kl->Lock();
    }
}

void unlock_keys(int db_idx, set<string>& keys)
{
    g_server.operating_count_vec[db_idx]--;
    
    KeyLockMap* kl_map = g_key_lock_vector[db_idx];
    kl_map->map_mutex.lock();
    for (auto it_key = keys.begin(); it_key != keys.end(); it_key++) {
        const string& key = *it_key;
        auto it = kl_map->key_lock_map.find(key);
        if (it != kl_map->key_lock_map.end()) {
            KeyLock* kl = it->second;
            kl->DecrRef();
            kl->Unlock();
            if (kl->GetRef() == 0) {
                kl_map->key_lock_map.erase(key);
                kl_map->key_lock_pool.push_back(kl);
            }
        }
    }
    
    kl_map->map_mutex.unlock();
}

void spinlock_db(int db_idx)
{
    g_key_lock_vector[db_idx]->map_mutex.lock();
    while (g_server.operating_count_vec[db_idx]) {
        usleep(500);
    }
}

void lock_db(int db_idx)
{
    g_key_lock_vector[db_idx]->map_mutex.lock();
}

void unlock_db(int db_idx)
{
    g_key_lock_vector[db_idx]->map_mutex.unlock();
}

void spinlock_all()
{
    for (int i = 0; i < g_server.db_num; i++) {
        spinlock_db(i);
    }
}

void lock_all()
{
    for (int i = 0; i < g_server.db_num; i++) {
        lock_db(i);
    }
}

void unlock_all()
{
    for (int i = 0; i < g_server.db_num; i++) {
        unlock_db(i);
    }
}
