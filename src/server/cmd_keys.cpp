//
//  cmd_keys.cpp
//  kedis
//
//  Created by ziteng on 17/7/25.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "cmd_keys.h"
#include "db_util.h"
#include "encoding.h"

void del_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int del_cnt = 0;
    int db_idx = conn->GetDBIndex();
    int cmd_size = (int)cmd_vec.size();
    set<string> keys;
    for (int i = 1; i < cmd_size; i++) {
        keys.insert(cmd_vec[i]);
    }
    
    lock_keys(db_idx, keys);
    
    for (int i = 1; i < (int)cmd_vec.size(); i++) {
        MetaData mdata;
        int ret = expire_key_if_needed(db_idx, cmd_vec[i], mdata);
        if (ret == kExpireKeyExist) {
            delete_key(db_idx, cmd_vec[i], mdata.ttl, mdata.type);
            del_cnt++;
        }
    }
    
    if (del_cnt > 0) {
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
    }
    
    unlock_keys(db_idx, keys);
    
    conn->SendInteger(del_cnt);
}

void exists_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendInteger(0);
    } else {
        conn->SendInteger(1);
    }
}

static void generic_ttl_command(ClientConn* conn, const vector<string>& cmd_vec, bool output_ms)
{
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    uint64_t now = get_tick_count();
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendInteger(-2);
    } else {
        if (mdata.ttl == 0) {
            conn->SendInteger(-1);
        } else {
            int64_t expire_ttl = mdata.ttl - now;
            if (!output_ms) {
                expire_ttl = (expire_ttl + 500) / 1000;
            }
            
            conn->SendInteger(expire_ttl);
        }
    }
}

void ttl_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_ttl_command(conn, cmd_vec, false);
}

void pttl_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_ttl_command(conn, cmd_vec, true);
}

void type_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendSimpleString("none");
    } else {
        string name = get_type_name(mdata.type);
        conn->SendSimpleString(name);
    }
}

static void generic_expire_command(ClientConn* conn, const vector<string>& cmd_vec, uint64_t base_time, bool second_unit)
{
    unsigned long ttl;
    if (get_ulong_from_string(cmd_vec[2], ttl) == CODE_ERROR) {
        conn->SendError("value is not an integer or out of range");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    const string& key = cmd_vec[1];
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, key);
    int ret = expire_key_if_needed(db_idx, key, mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendInteger(0);
    } else {
        if (second_unit) {
            ttl *= 1000;
        }
        
        ttl += base_time;
        if (ttl <= get_tick_count()) {
            delete_key(db_idx, key, mdata.ttl, mdata.type);
            conn->SendInteger(1);
            return;
        }
        
        rocksdb::WriteBatch batch;
        if (mdata.ttl != 0) {
            del_ttl_data(db_idx, mdata.ttl, key, &batch);
        } else {
            g_server.ttl_key_count_vec[db_idx]++;
        }
        
        if (mdata.type == KEY_TYPE_STRING) {
            put_kv_data(db_idx, key, mdata.value, ttl, &batch);
        } else if (mdata.type == KEY_TYPE_LIST) {
            put_meta_data(db_idx, mdata.type, key, ttl, mdata.count, mdata.head_seq, mdata.tail_seq,
                          mdata.current_seq, &batch);
        } else {
            put_meta_data(db_idx, mdata.type, key, ttl, mdata.count, &batch);
        }
        
        put_ttl_data(db_idx, ttl, key, mdata.type, &batch);
        DB_BATCH_UPDATE(batch)
        
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(1);
    }
}

void expire_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_expire_command(conn, cmd_vec, get_tick_count(), true);
}

void expireat_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_expire_command(conn, cmd_vec, 0, true);
}

void pexpire_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_expire_command(conn, cmd_vec, get_tick_count(), false);
}

void pexpireat_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_expire_command(conn, cmd_vec, 0, false);
}

void persist_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendInteger(0);
    } else {
        if (mdata.ttl != 0) {
            g_server.ttl_key_count_vec[db_idx]--;
            del_ttl_data(db_idx, mdata.ttl, cmd_vec[1], &batch);
    
            if (mdata.type == KEY_TYPE_STRING) {
                put_kv_data(db_idx, cmd_vec[1], mdata.value, 0, &batch);
            } else if (mdata.type == KEY_TYPE_LIST) {
                put_meta_data(db_idx, mdata.type, cmd_vec[1], 0, mdata.count, mdata.head_seq, mdata.tail_seq,
                              mdata.current_seq, &batch);
            } else {
                put_meta_data(db_idx, mdata.type, cmd_vec[1], 0, mdata.count, &batch);
            }
            DB_BATCH_UPDATE(batch)
            g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
            conn->SendInteger(1);
        } else {
            conn->SendInteger(0);
        }
    }
}

// get a random key from the first 64 keys
void randomkey_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    EncodeKey key_prefix(KEY_TYPE_META, "");
    rocksdb::Slice encode_prefix = key_prefix.GetEncodeKey();
    int db_idx = conn->GetDBIndex();
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
    
    vector<string> keys;
    int seek_cnt = 0;
    for (it->Seek(encode_prefix); it->Valid() && (int)keys.size() < 64; it->Next(), seek_cnt++) {
        if (it->key()[0] != KEY_TYPE_META) {
            break;
        }
        string encode_key = it->key().ToString();
        string key;
        
        int ret = DecodeKey::Decode(encode_key, KEY_TYPE_META, key);
        if (ret == kDecodeErrorType) {
            break;
        }
        
        ByteStream stream((uchar_t*)it->value().data(), (uint32_t)it->value().size());
        try {
            uint8_t type;
            uint64_t ttl;
            stream >> type;
            stream >> ttl;
            if (!ttl || ttl > get_tick_count()) {
                keys.push_back(key);
            }
        } catch (ParseException& ex) {
            conn->SendError("db error");
            delete it;
            return;
        }
    }
    
    delete it;
    
    if (keys.empty()) {
        conn->SendRawResponse(kNullBulkString);
    } else {
        int rand_idx = rand() % (int)keys.size();
        conn->SendBulkString(keys[rand_idx]);
    }
}

void keys_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    const string& pattern = cmd_vec[1];
    int pattern_size = (int)pattern.size();
    int idx = 0;
    for (; idx < pattern_size; idx++) {
        char ch = pattern.at(idx);
        if (ch == '*' || ch == '?' || ch == '[' || ch == '\\') {
            break;
        }
    }
    
    // extract normal pattern to reduce key scan range
    string prefix = pattern.substr(0, idx);
    
    EncodeKey key_prefix(KEY_TYPE_META, prefix);
    rocksdb::Slice encode_prefix = key_prefix.GetEncodeKey();
    int db_idx = conn->GetDBIndex();
    
    ScanKeyGuard scan_key_guard(db_idx);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
    
    vector<string> keys;
    for (it->Seek(encode_prefix); it->Valid(); it->Next()) {
        if (!it->key().starts_with(encode_prefix)) {
            break;
        }
        string encode_key = it->key().ToString();
        string key;
        
        int ret = DecodeKey::Decode(encode_key, KEY_TYPE_META, key);
        if (ret == kDecodeErrorType) {
            break;
        }
        
        if (!stringmatchlen(pattern.c_str(), (int)pattern.size(), key.c_str(), (int)key.size(), 0)) {
            continue;
        }
        
        ByteStream stream((uchar_t*)it->value().data(), (uint32_t)it->value().size());
        try {
            uint8_t type;
            uint64_t ttl;
            stream >> type;
            stream >> ttl;
            if (!ttl || ttl > get_tick_count()) {
                keys.push_back(key);
            }
        } catch (ParseException& ex) {
            conn->SendError("db error");
            delete it;
            return;
        }
    }
    
    delete it;
    conn->SendArray(keys);
}

// SCAN cursor [MATCH pattern] [COUNT count]
// cursor is a string, that is diffreret from Redis scan command
void scan_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long count = 10;
    string pattern;
    if (parse_scan_param(conn, cmd_vec, 2, pattern, count) == CODE_ERROR) {
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    EncodeKey cursor_key(KEY_TYPE_META, cmd_vec[1]);
    ScanKeyGuard scan_key_guard(db_idx);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
    
    string cursor;
    vector<string> keys;
    for (it->Seek(cursor_key.GetEncodeKey()); it->Valid(); it->Next()) {
        if (it->key()[0] != KEY_TYPE_META) {
            break;
        }
        string encode_key = it->key().ToString();
        string key;
        
        if (DecodeKey::Decode(encode_key, KEY_TYPE_META, key) != kDecodeOK) {
            conn->SendError("invalid meta key");
            delete it;
            return;
        }
        
        if ((long)keys.size() >= count) {
            cursor = key;
            break;
        }
        
        if (pattern.empty()) {
            keys.push_back(key);
        } else {
            if (stringmatchlen(pattern.c_str(), (int)pattern.size(), key.c_str(), (int)key.size(), 0)) {
                keys.push_back(key);
            }
        }
    }
    
    delete it;
    
    string start_resp = "*2\r\n";
    conn->SendRawResponse(start_resp);
    conn->SendBulkString(cursor);
    conn->SendArray(keys);
}
