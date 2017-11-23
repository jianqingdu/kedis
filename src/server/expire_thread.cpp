//
//  expire_thread.cpp
//  kedis
//
//  Created by ziteng on 17/9/27.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "expire_thread.h"
#include "server.h"
#include "encoding.h"
#include "db_util.h"
#include "simple_log.h"

void ExpireThread::OnThreadRun()
{
    log_message(kLogLevelInfo, "expire thread start\n");
    m_running = true;
    while (m_running) {
        usleep(100000);
        
        uint64_t current_tick = get_tick_count();
        for (int i = 0; i < g_server.db_num; i++) {
            ScanKeyGuard scan_key_guard(i);
            
            rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[i];
            rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
            rocksdb::Slice prefix_key((char*)&KEY_TYPE_ZSET_SORT, 1);
            
            for (it->Seek(prefix_key); it->Valid(); it->Next()) {
                if (it->key()[0] != KEY_TYPE_TTL_SORT) {
                    break;
                }
                
                uint64_t ttl;
                string key;
                if (DecodeKey::Decode(it->key().ToString(), KEY_TYPE_TTL_SORT, ttl, key) != kDecodeOK) {
                    log_message(kLogLevelError, "db must be collapsed\n");
                    continue;
                }
                
                if (ttl <= current_tick) {
                    uint8_t key_type = it->value()[0];
                    delete_key(i, key, ttl, key_type);
                } else {
                    break;
                }
            }
            
            delete it;
        }
    }
}
