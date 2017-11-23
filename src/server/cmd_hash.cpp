//
//  cmd_hash.cpp
//  kedis
//
//  Created by ziteng on 17/7/20.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "cmd_hash.h"
#include "db_util.h"
#include "encoding.h"

static rocksdb::Status put_hash_field(int db_idx, const string& key, const string& field, const string& value,
                                      rocksdb::WriteBatch* batch = NULL)
{
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey field_key(KEY_TYPE_HASH_FIELD, key, field);
    EncodeValue field_value(KEY_TYPE_HASH_FIELD, value);
    rocksdb::Status s;
    if (batch) {
        s = batch->Put(cf_handle, field_key.GetEncodeKey(), field_value.GetEncodeValue());
    } else {
        s = g_server.db->Put(g_server.write_option, cf_handle, field_key.GetEncodeKey(), field_value.GetEncodeValue());
    }
    
    if (!s.ok()) {
        log_message(kLogLevelError, "put_hash_filed failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

int get_hash_field(int db_idx, const string& key, const string& field, string& value)
{
    EncodeKey hash_field_key(KEY_TYPE_HASH_FIELD, key, field);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    string encode_value;
    
    rocksdb::Status status = g_server.db->Get(g_server.read_option, cf_handle, hash_field_key.GetEncodeKey(), &encode_value);
    if (status.ok()) {
        if (DecodeValue::Decode(encode_value, KEY_TYPE_HASH_FIELD, value) == kDecodeOK) {
            return FIELD_EXIST;
        } else {
            return DB_ERROR;
        }
    } else if (status.IsNotFound()) {
        return FIELD_NOT_EXIST;
    } else {
        return DB_ERROR;
    }
}

void hdel_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_HASH) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        rocksdb::WriteBatch batch;
        int del_cnt = 0;
        int cmd_size = (int)cmd_vec.size();
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        for (int i = 2; i < cmd_size; i++) {
            EncodeKey hash_field_key(KEY_TYPE_HASH_FIELD, cmd_vec[1], cmd_vec[i]);
            string value;
            
            rocksdb::Status status = g_server.db->Get(g_server.read_option, cf_handle, hash_field_key.GetEncodeKey(), &value);
            if (status.ok()) {
                batch.Delete(cf_handle, hash_field_key.GetEncodeKey());
                del_cnt++;
            }
        }
        
        if (del_cnt > 0) {
            mdata.count -= del_cnt;
            if (mdata.count == 0) {
                delete_key(db_idx, cmd_vec[1], mdata.ttl, KEY_TYPE_HASH);
            } else {
                put_meta_data(db_idx, KEY_TYPE_HASH, cmd_vec[1], mdata.ttl, mdata.count, &batch);
                DB_BATCH_UPDATE(batch)
            }
            
            g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        }
        
        conn->SendInteger(del_cnt);
    }
}

void hexists_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_HASH) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        string value;
        int result = get_hash_field(db_idx, cmd_vec[1], cmd_vec[2], value);
        if (result == FIELD_EXIST) {
            conn->SendInteger(1);
        } else if (result == FIELD_NOT_EXIST) {
            conn->SendInteger(0);
        } else {
            conn->SendError("db error");
        }
    }
}

void hget_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendRawResponse(kNullBulkString);
    } else {
        if (mdata.type != KEY_TYPE_HASH) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        string value;
        int result = get_hash_field(db_idx, cmd_vec[1], cmd_vec[2], value);
        if (result == FIELD_EXIST) {
            conn->SendBulkString(value);
        } else if (result == FIELD_NOT_EXIST) {
            conn->SendRawResponse(kNullBulkString);
        } else {
            conn->SendError("db error");
        }
    }
}

const int kObjHashKey = 0x1;
const int kObjHashValue = 0x2;
static void generic_hgetall_command(ClientConn* conn, const vector<string>& cmd_vec, int obj_flag)
{
    vector<string> hash_vec;
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendArray(hash_vec);
    } else {
        if (mdata.type != KEY_TYPE_HASH) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        EncodeKey prefix_key(KEY_TYPE_HASH_FIELD, cmd_vec[1]);
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);

        uint64_t seek_cnt = 0;
        for (it->Seek(prefix_key.GetEncodeKey()); it->Valid() && seek_cnt < mdata.count; it->Next(), seek_cnt++) {
            string encode_key = it->key().ToString();
            string encode_value = it->value().ToString();
            string key, field, value;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_HASH_FIELD, key, field) != kDecodeOK) {
                conn->SendError("invalid hash key field");
                delete it;
                return;
            }
            
            if (DecodeValue::Decode(encode_value, KEY_TYPE_HASH_FIELD, value) != kDecodeOK) {
                conn->SendError("invalid hash key value");
                delete it;
                return;
            }
            
            if (obj_flag & kObjHashKey) {
                hash_vec.push_back(field);
            }
            
            if (obj_flag & kObjHashValue) {
                hash_vec.push_back(value);
            }
        }
        
        delete it;
        conn->SendArray(hash_vec);
    }
}

void hgetall_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_hgetall_command(conn, cmd_vec, kObjHashKey|kObjHashValue);
}

void hkeys_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_hgetall_command(conn, cmd_vec, kObjHashKey);
}

void hvals_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_hgetall_command(conn, cmd_vec, kObjHashValue);
}

void hincrby_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long incr;
    if (get_long_from_string(cmd_vec[3], incr) == CODE_ERROR) {
        conn->SendError("increment is not a valid integer");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        g_server.key_count_vec[db_idx]++;
        put_meta_data(db_idx, KEY_TYPE_HASH, cmd_vec[1], 0, 1, &batch);
        put_hash_field(db_idx, cmd_vec[1], cmd_vec[2], cmd_vec[3], &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(incr);
    } else {
        if (mdata.type != KEY_TYPE_HASH) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        string value;
        int result = get_hash_field(db_idx, cmd_vec[1], cmd_vec[2], value);
        if (result == FIELD_EXIST) {
            long old_value;
            if (get_long_from_string(value, old_value) == CODE_OK) {
                if ((incr < 0 && old_value < 0 && incr < (LLONG_MIN-old_value)) ||
                    (incr > 0 && old_value > 0 && incr > (LLONG_MAX-old_value))) {
                    conn->SendError("increment or decrement would overflow");
                    return;
                }
                
                long new_value = old_value + incr;
                put_hash_field(db_idx, cmd_vec[1], cmd_vec[2], to_string(new_value));
                conn->SendInteger(new_value);
            } else {
                conn->SendError("hash value is not an integer");
                return;
            }
        } else if (result == FIELD_NOT_EXIST) {
            put_hash_field(db_idx, cmd_vec[1], cmd_vec[2], cmd_vec[3], &batch);
            put_meta_data(db_idx, KEY_TYPE_HASH, cmd_vec[1], mdata.ttl, mdata.count + 1, &batch);
            DB_BATCH_UPDATE(batch)
            conn->SendInteger(incr);
        } else {
            conn->SendError("db error");
            return;
        }
        
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
    }
}

void hincrbyfloat_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    double incr;
    if (get_double_from_string(cmd_vec[3], incr) == CODE_ERROR) {
        conn->SendError("increment is not a valid integer");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        g_server.key_count_vec[db_idx]++;
        put_meta_data(db_idx, KEY_TYPE_HASH, cmd_vec[1], 0, 1, &batch);
        put_hash_field(db_idx, cmd_vec[1], cmd_vec[2], cmd_vec[3], &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendBulkString(cmd_vec[3]);
    } else {
        if (mdata.type != KEY_TYPE_HASH) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        string value;
        int result = get_hash_field(db_idx, cmd_vec[1], cmd_vec[2], value);
        if (result == FIELD_EXIST) {
            double old_value;
            if (get_double_from_string(value, old_value) == CODE_OK) {
                double new_value = old_value + incr;
                string str_value = double_to_string(new_value);
                put_hash_field(db_idx, cmd_vec[1], cmd_vec[2], str_value);
                conn->SendBulkString(str_value);
            } else {
                conn->SendError("hash value is not an float");
                return;
            }
        } else if (result == FIELD_NOT_EXIST) {
            put_hash_field(db_idx, cmd_vec[1], cmd_vec[2], cmd_vec[3], &batch);
            put_meta_data(db_idx, KEY_TYPE_HASH, cmd_vec[1], mdata.ttl, mdata.count + 1, &batch);
            DB_BATCH_UPDATE(batch)
            conn->SendBulkString(cmd_vec[3]);
        } else {
            conn->SendError("db error");
            return;
        }
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
    }
}

void hlen_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_HASH) {
            conn->SendRawResponse(kWrongTypeError);
        } else {
            conn->SendInteger(mdata.count);
        }
    }
}

void hmget_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    vector<string> hash_vec;
    int db_idx = conn->GetDBIndex();
    int cmd_size = (int)cmd_vec.size();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        for (int i = 2; i < cmd_size; i++) {
            hash_vec.push_back("");
        }
        conn->SendArray(hash_vec);
    } else {
        if (mdata.type != KEY_TYPE_HASH) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        for (int i = 2; i < cmd_size; i++) {
            string value;
            int result = get_hash_field(db_idx, cmd_vec[1], cmd_vec[i], value);
            if (result == FIELD_EXIST) {
                hash_vec.push_back(value);
            } else if (result == FIELD_NOT_EXIST) {
                hash_vec.push_back("");
            } else {
                conn->SendError("db error");
                return;
            }
        }
        
        conn->SendArray(hash_vec);
    }
}

void hmset_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int cmd_size = (int)cmd_vec.size();
    if (cmd_size % 2 != 0) {
        conn->SendError("wrong number of argument for HMSET");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        g_server.key_count_vec[db_idx]++;
        for (int i = 2; i < cmd_size; i += 2) {
            put_hash_field(db_idx, cmd_vec[1], cmd_vec[i], cmd_vec[i + 1], &batch);
        }
        
        uint64_t count = (cmd_size - 2) / 2;
        put_meta_data(db_idx, KEY_TYPE_HASH, cmd_vec[1], 0, count, &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendSimpleString("OK");
    } else {
        if (mdata.type != KEY_TYPE_HASH) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        int add_cnt = 0;
        for (int i = 2; i < cmd_size; i += 2) {
            string value;
            int result = get_hash_field(db_idx, cmd_vec[1], cmd_vec[i], value);
            if (result == FIELD_EXIST) {
                if (value != cmd_vec[i + 1]) {
                    put_hash_field(db_idx, cmd_vec[1], cmd_vec[i], cmd_vec[i + 1], &batch);
                }
            } else if (result == FIELD_NOT_EXIST) {
                put_hash_field(db_idx, cmd_vec[1], cmd_vec[i], cmd_vec[i + 1], &batch);
                add_cnt++;
            } else {
                conn->SendError("db error");
                return;
            }
        }
        
        if (add_cnt > 0) {
            put_meta_data(db_idx, KEY_TYPE_HASH, cmd_vec[1], mdata.ttl, mdata.count + add_cnt, &batch);
        }
        
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendSimpleString("OK");
    }
}

void hscan_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long count = 10;
    string pattern;
    if (parse_scan_param(conn, cmd_vec, 3, pattern, count) == CODE_ERROR) {
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    EncodeKey prefix_key(KEY_TYPE_HASH_FIELD, cmd_vec[1]);
    EncodeKey cursor_key(KEY_TYPE_HASH_FIELD, cmd_vec[1], cmd_vec[2]);
    
    ScanKeyGuard scan_key_guard(db_idx);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
    
    string cursor;
    vector<string> fields;
    for (it->Seek(cursor_key.GetEncodeKey()); it->Valid(); it->Next()) {
        if (!it->key().starts_with(prefix_key.GetEncodeKey())) {
            break;
        }
        string encode_key = it->key().ToString();
        string encode_value = it->value().ToString();
        string key, field, value;
        
        if (DecodeKey::Decode(encode_key, KEY_TYPE_HASH_FIELD, key, field) != kDecodeOK) {
            conn->SendError("invalid hash key field");
            delete it;
            return;
        }
        
        if (DecodeValue::Decode(encode_value, KEY_TYPE_HASH_FIELD, value) != kDecodeOK) {
            conn->SendError("invalid hash key value");
            delete it;
            return;
        }
        
        
        if ((long)fields.size() >= count * 2) {
            cursor = field;
            break;
        }
        
        if (pattern.empty()) {
            fields.push_back(field);
            fields.push_back(value);
        } else {
            if (stringmatchlen(pattern.c_str(), (int)pattern.size(), field.c_str(), (int)field.size(), 0)) {
                fields.push_back(field);
                fields.push_back(value);
            }
        }
    }
    
    delete it;
    
    string start_resp = "*2\r\n";
    conn->SendRawResponse(start_resp);
    conn->SendBulkString(cursor);
    conn->SendArray(fields);
}

static void generic_hset_command(ClientConn* conn, const vector<string>& cmd_vec, bool nx)
{
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        g_server.key_count_vec[db_idx]++;
        put_hash_field(db_idx, cmd_vec[1], cmd_vec[2], cmd_vec[3], &batch);
        put_meta_data(db_idx, KEY_TYPE_HASH, cmd_vec[1], 0, 1, &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(1);
    } else {
        if (mdata.type != KEY_TYPE_HASH) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        int new_field = 1;
        string value;
        int result = get_hash_field(db_idx, cmd_vec[1], cmd_vec[2], value);
        if (result == FIELD_EXIST) {
            new_field = 0;
            if (!nx) {
                put_hash_field(db_idx, cmd_vec[1], cmd_vec[2], cmd_vec[3]);
            }
        } else if (result == FIELD_NOT_EXIST) {
            put_hash_field(db_idx, cmd_vec[1], cmd_vec[2], cmd_vec[3], &batch);
            put_meta_data(db_idx, KEY_TYPE_HASH, cmd_vec[1], mdata.ttl, mdata.count + 1, &batch);
            DB_BATCH_UPDATE(batch)
        } else {
            conn->SendError("db error");
            return;
        }
        
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(new_field);
    }
}

void hset_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_hset_command(conn, cmd_vec, false);
}

void hsetnx_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_hset_command(conn, cmd_vec, true);
}

void hstrlen_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_HASH) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        string value;
        int result = get_hash_field(db_idx, cmd_vec[1], cmd_vec[2], value);
        if (result == FIELD_EXIST) {
            conn->SendInteger(value.size());
        } else if (result == FIELD_NOT_EXIST) {
            conn->SendInteger(0);
        } else {
            conn->SendError("db error");
        }
    }
}
