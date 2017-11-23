//
//  db_util.h
//  kedis
//
//  Created by ziteng on 17/9/21.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __DB_UTIL_H__
#define __DB_UTIL_H__

#include "util.h"
#include "key_lock.h"
#include "simple_log.h"
#include "client_conn.h"
#include "rocksdb/db.h"
#include "redis_parser.h"
#include "server.h"

#define FIELD_NOT_EXIST 0
#define FIELD_EXIST     1
#define DB_ERROR        2

const int kExpireKeyNotExist    = 0;
const int kExpireKeyExist       = 1; // also return key_type, ttl, count or value, this can save another Get method
const int kExpireDBError        = 2;

struct MetaData {
    uint8_t type;
    uint64_t ttl;
    string value;
    uint64_t count;
    uint64_t head_seq;
    uint64_t tail_seq;
    uint64_t current_seq;
};

void delete_key(int db_idx, const string& key, uint64_t ttl, uint8_t key_type);

int expire_key_if_needed(int db_idx, const string& key, MetaData& mdata, string* raw_value = nullptr);

rocksdb::Status put_meta_data(int db_idx, uint8_t key_type, const string& key, uint64_t ttl, uint64_t count,
                              rocksdb::WriteBatch* batch = NULL);
rocksdb::Status put_meta_data(int db_idx, uint8_t key_type, const string& key, uint64_t ttl, uint64_t count,
                              uint64_t head_seq, uint64_t tail_seq, uint64_t current_seq,
                              rocksdb::WriteBatch* batch = NULL);
rocksdb::Status put_ttl_data(int db_idx, uint64_t ttl, const string& key, uint8_t key_type, rocksdb::WriteBatch* batch = NULL);
rocksdb::Status del_ttl_data(int db_idx, uint64_t ttl, const string& key, rocksdb::WriteBatch* batch = NULL);
void put_kv_data(int db_idx, const string& key, const string& value, uint64_t ttl, rocksdb::WriteBatch* batch = NULL);

#define DB_BATCH_UPDATE(batch) \
rocksdb::Status s = g_server.db->Write(g_server.write_option, &batch); \
if (!s.ok()) { \
    log_message(kLogLevelError, "write batch failed: %s\n", batch.Data().c_str()); \
}

string double_to_string(double value);

int parse_scan_param(ClientConn* conn, const vector<string>& cmd_vec, int start_index, string& pattern, long& count);

// Glob-style pattern matching, 1-match, 0-unmatch
int stringmatchlen(const char *pattern, int patternLen, const char *str, int strLen, int nocase);

#endif /* __DB_UTIL_H__ */
