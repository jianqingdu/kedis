//
//  cmd_string.cpp
//  kedis
//
//  Created by ziteng on 17/7/20.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "cmd_string.h"
#include "db_util.h"
#include "encoding.h"

void append_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        g_server.key_count_vec[db_idx]++;
        put_kv_data(db_idx, cmd_vec[1], cmd_vec[2], 0);
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(cmd_vec[2].size());
    } else {
        if (mdata.type != KEY_TYPE_STRING) {
            conn->SendRawResponse(kWrongTypeError);
        } else {
            string new_value = mdata.value + cmd_vec[2];
            put_kv_data(db_idx, cmd_vec[1], new_value, mdata.ttl);
            g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
            conn->SendInteger(new_value.size());
        }
    }
}

void getrange_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long start, end;
    if (get_long_from_string(cmd_vec[2], start) == CODE_ERROR || get_long_from_string(cmd_vec[3], end) == CODE_ERROR) {
        conn->SendError("value is not an integer or out of range");
        return;
    }
    
    if (start < 0 && end < 0 && start > end) {
        conn->SendRawResponse(kEmptyBulkString);
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendRawResponse(kEmptyBulkString);
    } else {
        if (mdata.type != KEY_TYPE_STRING) {
            conn->SendRawResponse(kWrongTypeError);
        } else {
            long str_len = (long)mdata.value.size();
            
            // Convert negative indexes
            if (start < 0) start = str_len + start;
            if (end < 0) end = str_len + end;
            if (start < 0) start = 0;
            if (end < 0) end = 0;
            if (end >= str_len) end = str_len-1;
            
            // Precondition: end >= 0 && end < strlen, so the only condition where
            // nothing can be returned is: start > end.
            if (start > end || str_len == 0) {
                conn->SendRawResponse(kEmptyBulkString);
            } else {
                long len = end - start + 1;
                string sub_str = mdata.value.substr(start, len);
                conn->SendBulkString(sub_str);
            }
        }
    }
}

void setrange_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long offset;
    if (get_long_from_string(cmd_vec[2], offset) == CODE_ERROR) {
        conn->SendError("offset is not a valid integer");
        return;
    }

    // check offset: return error message exact the same with Redis so we can pass unit test
    if (offset < 0) {
        conn->SendError("offset is out of range");
        return;
    }
    
    if (offset + (int)cmd_vec[3].size() > 512 * 1024 * 1024) {
        conn->SendError("string exceeds maximum allowed size (512MB)");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        if (cmd_vec[3].empty()) {
            conn->SendInteger(0);
            return;
        }
        
        g_server.key_count_vec[db_idx]++;
        string value;
        value.append(offset, (char)0);
        value.append(cmd_vec[3]);
        put_kv_data(db_idx, cmd_vec[1], value, 0);
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger((long)value.size());
    } else {
        if (mdata.type != KEY_TYPE_STRING) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        long old_size = (long)mdata.value.size();
        if (old_size >= offset) {
            mdata.value.replace(offset, cmd_vec[3].size(), cmd_vec[3]);
        } else {
            mdata.value.append(offset - old_size, (char)0);
            mdata.value.append(cmd_vec[3]);
        }
        
        put_kv_data(db_idx, cmd_vec[1], mdata.value, mdata.ttl);
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(mdata.value.size());
    }
}

void strlen_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_STRING) {
            conn->SendRawResponse(kWrongTypeError);
        } else {
            long size = (long)mdata.value.size();
            conn->SendInteger(size);
        }
    }
}

void get_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_STRING) {
            conn->SendRawResponse(kWrongTypeError);
        } else {
            if (mdata.value != "") {
                conn->SendBulkString(mdata.value);
            } else {
                conn->SendRawResponse(kEmptyBulkString);
            }
        }
    }
}

void getset_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        put_kv_data(db_idx, cmd_vec[1], cmd_vec[2], 0);
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendRawResponse(kNullBulkString);
    } else {
        if (mdata.type != KEY_TYPE_STRING) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        if (mdata.ttl > 0) {
            del_ttl_data(db_idx, mdata.ttl, cmd_vec[1], &batch);
        }
        
        put_kv_data(db_idx, cmd_vec[1], cmd_vec[2], 0, &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendBulkString(mdata.value);
    }
}

static void generic_set_command(ClientConn* conn, const string& key, const string& value, const string& ttl_str,
                                bool unit_second, bool nx, bool xx, bool setnx_resp = false) // setnx must return an integer
{
    long ttl;
    if (get_long_from_string(ttl_str, ttl) == CODE_ERROR) {
        conn->SendError("value is not an integer or out of range");
        return;
    }
    
    if (ttl < 0) {
        conn->SendError("invalid expire time");
        return;
    }
    
    if (unit_second) {
        ttl *= 1000;
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, key);
    int ret = expire_key_if_needed(db_idx, key, mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
        return;
    }
    
    if ((nx && (ret == kExpireKeyExist)) || (xx && (ret ==kExpireKeyNotExist))) {
        if (setnx_resp) {
            conn->SendInteger(0);
        } else {
            conn->SendRawResponse(kNullBulkString);
        }
        return;
    }
    
    if (ret == kExpireKeyExist) {
        delete_key(conn->GetDBIndex(), key, mdata.ttl, mdata.type);
    }
    
    g_server.key_count_vec[db_idx]++;
    if (ttl > 0) {
        g_server.ttl_key_count_vec[db_idx]++;
        ttl += get_tick_count();
    }
    
    put_kv_data(db_idx, key, value, ttl, &batch);
    if (ttl > 0) {
        put_ttl_data(db_idx, ttl, key, KEY_TYPE_STRING, &batch);
    }
    DB_BATCH_UPDATE(batch)
    g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
    if (setnx_resp) {
        conn->SendInteger(1);
    } else {
        conn->SendRawResponse("+OK\r\n");
    }
}

/* SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>] */
void set_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    string ttl_str = "0";
    bool unit_second = true;
    bool nx = false;
    bool xx = false;
    int cmd_size = (int)cmd_vec.size();
    for (int i = 3; i < cmd_size; i++) {
        bool has_next = (i != cmd_size -1);
        
        if (!strcasecmp(cmd_vec[i].c_str(), "NX")) {
            nx = true;
        } else if (!strcasecmp(cmd_vec[i].c_str(), "XX")) {
            xx = true;
        } else if (!strcasecmp(cmd_vec[i].c_str(), "EX") && has_next) {
            ttl_str = cmd_vec[++i];
        } else if (!strcasecmp(cmd_vec[i].c_str(), "PX") && has_next) {
            ttl_str = cmd_vec[++i];
            unit_second = false;
        } else {
            conn->SendError("syntax error");
            return;
        }
    }
    
    generic_set_command(conn, cmd_vec[1], cmd_vec[2], ttl_str, unit_second, nx, xx);
}

void setex_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_set_command(conn, cmd_vec[1], cmd_vec[3], cmd_vec[2], true, false, false);
}

void setnx_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_set_command(conn, cmd_vec[1], cmd_vec[2], "0", true, true, false, true);
}

void psetex_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_set_command(conn, cmd_vec[1], cmd_vec[3], cmd_vec[2], false, false, false);
}

void mset_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int cmd_size = (int)cmd_vec.size();
    if (cmd_size % 2 == 0) {
        conn->SendError("wrong number of arguments for MSET");
        return;
    }
    
    rocksdb::WriteBatch batch;
    int db_idx = conn->GetDBIndex();
    set<string> keys;
    for (int i = 1; i < cmd_size; i += 2) {
        keys.insert(cmd_vec[i]);
    }
    
    lock_keys(db_idx, keys);
    
    for (int i = 1; i < cmd_size; i += 2) {
        MetaData mdata;
        int ret = expire_key_if_needed(db_idx, cmd_vec[i], mdata);
        if (ret == kExpireDBError) {
            conn->SendError("db error");
            unlock_keys(db_idx, keys);
            return;
        } else if (ret == kExpireKeyExist) {
            delete_key(db_idx, cmd_vec[i], mdata.ttl, mdata.type);
        }
        
        put_kv_data(db_idx, cmd_vec[i], cmd_vec[i + 1], 0, &batch);
    }
    
    DB_BATCH_UPDATE(batch)
    g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
    g_server.key_count_vec[db_idx] += (long)keys.size();
    unlock_keys(db_idx, keys);
    
    conn->SendSimpleString("OK");
}

void msetnx_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int cmd_size = (int)cmd_vec.size();
    if (cmd_size % 2 == 0) {
        conn->SendError("wrong number of arguments for MSET");
        return;
    }
    
    rocksdb::WriteBatch batch;
    int db_idx = conn->GetDBIndex();
    set<string> keys;
    for (int i = 1; i < cmd_size; i += 2) {
        keys.insert(cmd_vec[i]);
    }
    
    lock_keys(db_idx, keys);
    
    for (int i = 1; i < cmd_size; i += 2) {
        MetaData mdata;
        int ret = expire_key_if_needed(db_idx, cmd_vec[i], mdata);
        if (ret == kExpireDBError) {
            conn->SendError("db error");
            unlock_keys(db_idx, keys);
            return;
        } else if (ret == kExpireKeyExist) {
            conn->SendInteger(0);
            unlock_keys(db_idx, keys);
            return;
        } else {
            put_kv_data(db_idx, cmd_vec[i], cmd_vec[i + 1], 0, &batch);
        }
    }
    
    DB_BATCH_UPDATE(batch);
    g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
    g_server.key_count_vec[db_idx] += (long)keys.size();
    unlock_keys(db_idx, keys);
    
    conn->SendInteger(1);
}

void mget_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    vector<string> value_vec;
    int db_idx = conn->GetDBIndex();
    int cmd_size = (int)cmd_vec.size();
    set<string> keys;
    for (int i = 1; i < cmd_size; i++) {
        keys.insert(cmd_vec[i]);
    }
    
    lock_keys(db_idx, keys);
    
    for (int i = 1; i < cmd_size; i++) {
        MetaData mdata;
        int ret = expire_key_if_needed(db_idx, cmd_vec[i], mdata);
        if (ret == kExpireDBError) {
            unlock_keys(db_idx, keys);
            conn->SendError("db error");
            return;
        } else if (ret == kExpireKeyNotExist) {
            value_vec.push_back("");
        } else {
            if (mdata.type != KEY_TYPE_STRING) {
                value_vec.push_back("");
            } else {
                value_vec.push_back(mdata.value);
            }
        }
    }
    
    unlock_keys(db_idx, keys);
    conn->SendArray(value_vec);
}

static void incr_decr_command(ClientConn* conn, const vector<string>& cmd_vec, const string& key, long incr_value)
{
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, key);
    int ret = expire_key_if_needed(db_idx, key, mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        g_server.key_count_vec[db_idx]++;
        put_kv_data(db_idx, key, to_string(incr_value), 0);
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(incr_value);
    } else {
        if (mdata.type != KEY_TYPE_STRING) {
            conn->SendRawResponse(kWrongTypeError);
        } else {
            long old_value;
            if (get_long_from_string(mdata.value, old_value) == CODE_ERROR) {
                conn->SendError("value is not an integer");
            } else {
                long new_value = old_value + incr_value;
                
                put_kv_data(db_idx, key, to_string(new_value), mdata.ttl);
                g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
                conn->SendInteger(new_value);
            }
        }
    }
}

void incr_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    incr_decr_command(conn, cmd_vec, cmd_vec[1], 1);
}

void decr_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    incr_decr_command(conn, cmd_vec, cmd_vec[1], -1);
}

void incrby_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long incr;
    if (get_long_from_string(cmd_vec[2], incr) == CODE_OK) {
        incr_decr_command(conn, cmd_vec, cmd_vec[1], incr);
    } else {
        conn->SendError("value is not an integer");
    }
}

void decrby_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long incr;
    if (get_long_from_string(cmd_vec[2], incr) == CODE_OK) {
        incr_decr_command(conn, cmd_vec, cmd_vec[1], -incr);
    } else {
        conn->SendError("value is not an integer");
    }
}

void incrbyfloat_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    double incr;
    if (get_double_from_string(cmd_vec[2], incr) == CODE_ERROR) {
        conn->SendError("value is not a float");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        g_server.key_count_vec[db_idx]++;
        put_kv_data(db_idx, cmd_vec[1], cmd_vec[2], 0);
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendBulkString(cmd_vec[2]);
    } else {
        if (mdata.type != KEY_TYPE_STRING) {
            conn->SendRawResponse(kWrongTypeError);
        } else {
            double old_value;
            if (get_double_from_string(mdata.value, old_value) == CODE_ERROR) {
                conn->SendError("value is not a valid float");
            } else {
                double new_value = old_value + incr;
                string str_value = double_to_string(new_value);
                
                put_kv_data(db_idx, cmd_vec[1], str_value, mdata.ttl);
                g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
                conn->SendBulkString(str_value);
            }
        }
    }
}

void setbit_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long bit_offset, on;
    if (get_long_from_string(cmd_vec[2], bit_offset) == CODE_ERROR || get_long_from_string(cmd_vec[3], on) == CODE_ERROR) {
        conn->SendError("offset or bit is not a integer");
        return;
    }
    
    if ((bit_offset < 0) || ((unsigned long long)bit_offset >> 3) >= (512*1024*1024)) {
        conn->SendError("bit offset is not an integer or out of range");
        return;
    }
    
    if (on & ~1) {
        // value can only take 0 or 1
        conn->SendError("bit is out of range");
        return;
    }
    
    long byte = bit_offset >> 3;
    int bit = 7 - (bit_offset & 0x7);
    int bitval = (int)(on << bit);
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        g_server.key_count_vec[db_idx]++;
        string value;
        value.append(byte, (char)0); // padding
        value.append(1, bitval);
        put_kv_data(db_idx, cmd_vec[1], value, 0);
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(0);
    } else {
        if (mdata.type != KEY_TYPE_STRING) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        int old_bitval = 0;
        int val_size = (int)mdata.value.size();
        if (val_size <= byte) {
            mdata.value.append(byte - val_size, (char)0); // padding
            mdata.value.append(1, bitval);
        } else {
            int val = mdata.value.at(byte);
            
            old_bitval = (val >> bit) & 0x1;
            val &= ~(1 << bit);
            val |= ((on & 0x1) << bit);
            mdata.value.at(byte) = val;
        }
        
        put_kv_data(db_idx, cmd_vec[1], mdata.value, mdata.ttl);
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(old_bitval);
    }
}

void getbit_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long bit_offset;
    if (get_long_from_string(cmd_vec[2], bit_offset) == CODE_ERROR) {
        conn->SendError("offset is not a integer");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendInteger(0);
    } else {
        if (mdata.type != KEY_TYPE_STRING) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        int byte = (int)bit_offset >> 3;
        int bit = 7 - (bit_offset & 0x7);
        
        int bitval = 0;
        int val_size = (int)mdata.value.size();
        if (val_size > byte) {
            int val = mdata.value.at(byte);
            bitval = (val >> bit) & 0x1;
        }
        
        conn->SendInteger(bitval);
    }
}

void bitcount_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_STRING) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        long val_len = (long)mdata.value.size();
        long start, end;
        int cmd_size = (int)cmd_vec.size();
        if (cmd_size == 4) {
            if (get_long_from_string(cmd_vec[2], start) == CODE_ERROR || get_long_from_string(cmd_vec[3], end) == CODE_ERROR) {
                conn->SendError("start or end is not valid integer");
                return;
            }
            
            /* Convert negative indexes */
            if (start < 0 && end < 0 && start > end) {
                conn->SendInteger(0);
                return;
            }
            if (start < 0) start = val_len + start;
            if (end < 0) end = val_len + end;
            if (start < 0) start = 0;
            if (end < 0) end = 0;
            if (end >= val_len) end = val_len - 1;
        } else if (cmd_size == 2) {
            // The whole string
            start = 0;
            end = val_len - 1;
        } else {
            conn->SendError("syntax error");
            return;
        }
        
        int bit_count = 0;
        for (long i = start; i <= end; i++) {
            int val = mdata.value.at(i);
            for (int j = 0; j < 8; j++) {
                if ((val >> j) & 0x1) {
                    bit_count++;
                }
            }
        }
        
        conn->SendInteger(bit_count);
    }
}

void bitpos_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long bit = 0;
    if (get_long_from_string(cmd_vec[2], bit) == CODE_ERROR) {
        conn->SendError("value is not an integer or out of range");
        return;
    }
    
    if (bit != 0 && bit != 1) {
        conn->SendError("bit must be 0 or 1");
        return;
    }
    
    // Parse start/end range if present
    long start = 0;
    long end = -1;
    int cmd_size = (int)cmd_vec.size();
    if (cmd_size == 4 || cmd_size == 5) {
        if (get_long_from_string(cmd_vec[3], start) == CODE_ERROR) {
            conn->SendError("value is not an integer or out of range");
            return;
        }
        
        if (cmd_size == 5) {
            if (get_long_from_string(cmd_vec[4], end) == CODE_ERROR) {
                conn->SendError("value is not an integer or out of range");
                return;
            }
        }
    } else if (cmd_size != 3) {
        conn->SendError("syntax error");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendInteger(-1);
    } else {
        if (mdata.type != KEY_TYPE_STRING) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        int val_len = (int)mdata.value.size();
        
        // Convert negative indexes
        if (start < 0) start = val_len + start;
        if (end < 0) end = val_len + end;
        if (start < 0) start = 0;
        if (end < 0) end = 0;
        if (end >= val_len) end = val_len - 1;

        long pos = -1;
        for (long i =  start; i <= end && pos == -1; i++) {
            uchar_t val = *((uchar_t*)mdata.value.data() + i);
            for (int j = 7; j>=0; j--) {
                int test_bit = (val >> j) & 0x1;
                if (test_bit == bit) {
                    pos = 8 * i + (8 - j - 1);
                    break;
                }
            }
        }
        
        conn->SendInteger(pos);
    }
}
