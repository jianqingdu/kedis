//
//  cmd_set.cpp
//  kedis
//
//  Created by ziteng on 17/7/20.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "cmd_set.h"
#include "db_util.h"
#include "encoding.h"

static rocksdb::Status put_set_member(int db_idx, const string& key, const string& member,
                                      rocksdb::WriteBatch* batch = NULL)
{
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey member_key(KEY_TYPE_SET_MEMBER, key, member);
    EncodeValue member_value(KEY_TYPE_SET_MEMBER);
    rocksdb::Status s;
    if (batch) {
        s = batch->Put(cf_handle, member_key.GetEncodeKey(), member_value.GetEncodeValue());
    } else {
        s = g_server.db->Put(g_server.write_option, cf_handle, member_key.GetEncodeKey(), member_value.GetEncodeValue());
    }
    
    if (!s.ok()) {
        log_message(kLogLevelError, "put_set_member failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

static rocksdb::Status del_set_member(int db_idx, const string& key, const string& member,
                                      rocksdb::WriteBatch* batch = NULL)
{
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey member_key(KEY_TYPE_SET_MEMBER, key, member);
    rocksdb::Status s;
    
    if (batch) {
        s = batch->Delete(cf_handle, member_key.GetEncodeKey());
    } else {
        s = g_server.db->Delete(g_server.write_option, cf_handle, member_key.GetEncodeKey());
    }
    
    if (!s.ok()) {
        log_message(kLogLevelError, "del_set_member failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

static int get_set_member(int db_idx, const string& key, const string& member)
{
    EncodeKey member_key(KEY_TYPE_SET_MEMBER, key, member);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    string encode_value;
    
    rocksdb::Status status = g_server.db->Get(g_server.read_option, cf_handle, member_key.GetEncodeKey(), &encode_value);
    if (status.ok()) {
        if (DecodeValue::Decode(encode_value, KEY_TYPE_SET_MEMBER) == kDecodeOK) {
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

void sadd_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int cmd_size = (int)cmd_vec.size();
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        g_server.key_count_vec[db_idx]++;
        for (int i = 2; i < cmd_size; i++) {
            put_set_member(db_idx, cmd_vec[1], cmd_vec[i], &batch);
        }
        uint64_t member_count = cmd_size - 2;
        put_meta_data(db_idx, KEY_TYPE_SET, cmd_vec[1], 0, member_count, &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(member_count);
    } else {
        if (mdata.type != KEY_TYPE_SET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        int add_cnt = 0;
        for (int i = 2; i < cmd_size; i++) {
            int result = get_set_member(db_idx, cmd_vec[1], cmd_vec[i]);
            if (result == FIELD_EXIST) {
                ;
            } else if (result == FIELD_NOT_EXIST) {
                put_set_member(db_idx, cmd_vec[1], cmd_vec[i], &batch);
                add_cnt++;
            } else {
                conn->SendError("db error");
                return;
            }
        }
        
        if (add_cnt > 0) {
            put_meta_data(db_idx, KEY_TYPE_SET, cmd_vec[1], mdata.ttl, mdata.count + add_cnt, &batch);
            DB_BATCH_UPDATE(batch)
            g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        }
        
        conn->SendInteger(add_cnt);
    }
}

void scard_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_SET) {
            conn->SendRawResponse(kWrongTypeError);
        } else {
            conn->SendInteger(mdata.count);
        }
    }
}

void sismember_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_SET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        int result = get_set_member(db_idx, cmd_vec[1], cmd_vec[2]);
        if (result == FIELD_EXIST) {
            conn->SendInteger(1);
        } else if (result == FIELD_NOT_EXIST) {
            conn->SendInteger(0);
        } else {
            conn->SendError("db error");
        }
    }
}

void smembers_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    vector<string> member_vec;
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendArray(member_vec);
    } else {
        if (mdata.type != KEY_TYPE_SET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        EncodeKey prefix_key(KEY_TYPE_SET_MEMBER, cmd_vec[1]);
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
        
        uint64_t seek_cnt = 0;
        for (it->Seek(prefix_key.GetEncodeKey()); it->Valid() && seek_cnt < mdata.count; it->Next(), seek_cnt++) {
            string encode_key = it->key().ToString();
            string encode_value = it->value().ToString();
            string key, member;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_SET_MEMBER, key, member) != kDecodeOK) {
                conn->SendError("invalid set key member");
                delete it;
                return;
            }
            
            if (DecodeValue::Decode(encode_value, KEY_TYPE_SET_MEMBER) != kDecodeOK) {
                conn->SendError("invalid set key value");
                delete it;
                return;
            }
            
            member_vec.push_back(member);
        }
        
        delete it;
        conn->SendArray(member_vec);
    }
}

void spop_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    unsigned long pop_cnt = 1;
    int cmd_size = (int)cmd_vec.size();
    if ((cmd_size == 3) && (get_ulong_from_string(cmd_vec[2], pop_cnt) == CODE_OK)) {
        ;
    } else if (cmd_size >= 3) {
        conn->SendError("syntax error");
        return;
    }
    
    vector<string> member_vec;
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendArray(member_vec);
    } else {
        if (mdata.type != KEY_TYPE_SET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        if (pop_cnt > mdata.count) {
            pop_cnt = mdata.count;
        }
        
        EncodeKey prefix_key(KEY_TYPE_SET_MEMBER, cmd_vec[1]);
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
        
        unsigned long seek_cnt = 0;
        for (it->Seek(prefix_key.GetEncodeKey()); it->Valid() && seek_cnt < pop_cnt; it->Next(), seek_cnt++) {
            string encode_key = it->key().ToString();
            string encode_value = it->value().ToString();
            string key, member;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_SET_MEMBER, key, member) != kDecodeOK) {
                conn->SendError("invalid set key member");
                delete it;
                return;
            }
            
            if (DecodeValue::Decode(encode_value, KEY_TYPE_SET_MEMBER) != kDecodeOK) {
                conn->SendError("invalid set key value");
                delete it;
                return;
            }
            
            member_vec.push_back(member);
            del_set_member(db_idx, cmd_vec[1], member, &batch);
        }
        
        mdata.count -= pop_cnt;
        if (mdata.count == 0) {
            delete_key(db_idx, cmd_vec[1], mdata.ttl, KEY_TYPE_SET);
            // delete_key will delete all kv data with this key, so we do not need to execute WriteBatch
        } else {
            put_meta_data(db_idx, KEY_TYPE_SET, cmd_vec[1], mdata.ttl, mdata.count, &batch);
            DB_BATCH_UPDATE(batch)
        }
        
        delete it;
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendArray(member_vec);
    }
}

void srandmember_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    bool has_count = false;
    bool negative_count = false;
    long count = 1;
    int cmd_size = (int)cmd_vec.size();
    if ((cmd_size == 3) && (get_long_from_string(cmd_vec[2], count) == CODE_OK)) {
        has_count = true;
        if (count < 0) {
            count = -count;
            negative_count = true;
        }
    } else if (cmd_size >= 3) {
        conn->SendError("syntax error");
        return;
    }
    
    vector<string> member_vec;
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        if (has_count) {
            conn->SendArray(member_vec);
        } else {
            conn->SendRawResponse(kNullBulkString);
        }
    } else {
        if (mdata.type != KEY_TYPE_SET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        EncodeKey prefix_key(KEY_TYPE_SET_MEMBER, cmd_vec[1]);
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
        
        long start, stop;
        if (count >= (int)mdata.count) {
            start = 0;
            stop = (int)mdata.count;
        } else {
            start = rand() % (mdata.count - count + 1);
            stop = start + count;
        }
        
        int seek_cnt = 0;
        for (it->Seek(prefix_key.GetEncodeKey()); it->Valid() && (seek_cnt < stop); it->Next(), seek_cnt++) {
            string encode_key = it->key().ToString();
            string encode_value = it->value().ToString();
            string key, member;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_SET_MEMBER, key, member) != kDecodeOK) {
                conn->SendError("invalid set key member");
                delete it;
                return;
            }
            
            if (DecodeValue::Decode(encode_value, KEY_TYPE_SET_MEMBER) != kDecodeOK) {
                conn->SendError("invalid set key value");
                delete it;
                return;
            }
            
            if (seek_cnt >= start) {
                member_vec.push_back(member);
            }
        }
        
        delete it;
        if (has_count) {
            if (negative_count && (count > (int)mdata.count)) {
                // push a random element multiple times for negative count
                for (int i = 0; i < (count - (int)mdata.count); i++) {
                    int rand_idx = rand() % member_vec.size();
                    string member = member_vec[rand_idx];
                    member_vec.push_back(member);
                }
            }
            conn->SendArray(member_vec);
        } else {
            conn->SendBulkString(member_vec[0]);
        }
    }
}

void srem_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_SET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        int del_cnt = 0;
        int cmd_size = (int)cmd_vec.size();
        for (int i = 2; i < cmd_size; i++) {
            int result = get_set_member(db_idx, cmd_vec[1], cmd_vec[i]);
            if (result == FIELD_EXIST) {
                del_set_member(db_idx, cmd_vec[1], cmd_vec[i], &batch);
                del_cnt++;
            } else if (result == DB_ERROR) {
                conn->SendError("db error");
                return;
            }
        }
        
        if (del_cnt > 0) {
            mdata.count -= del_cnt;
            if (mdata.count == 0) {
                delete_key(db_idx, cmd_vec[1], mdata.ttl, mdata.type);
            } else {
                put_meta_data(db_idx, KEY_TYPE_SET, cmd_vec[1], mdata.ttl, mdata.count, &batch);
                DB_BATCH_UPDATE(batch)
            }
            
            g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        }
        conn->SendInteger(del_cnt);
    }
}

void sscan_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long count = 10;
    string pattern;
    if (parse_scan_param(conn, cmd_vec, 3, pattern, count) == CODE_ERROR) {
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    EncodeKey prefix_key(KEY_TYPE_SET_MEMBER, cmd_vec[1]);
    EncodeKey cursor_key(KEY_TYPE_SET_MEMBER, cmd_vec[1], cmd_vec[2]);
    ScanKeyGuard scan_key_guard(db_idx);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
    
    string cursor;
    vector<string> members;
    for (it->Seek(cursor_key.GetEncodeKey()); it->Valid(); it->Next()) {
        if (!it->key().starts_with(prefix_key.GetEncodeKey())) {
            break;
        }
        string encode_key = it->key().ToString();
        string encode_value = it->value().ToString();
        string key, member;
        
        if (DecodeKey::Decode(encode_key, KEY_TYPE_SET_MEMBER, key, member) != kDecodeOK) {
            conn->SendError("invalid set member key");
            delete it;
            return;
        }
        
        if (DecodeValue::Decode(encode_value, KEY_TYPE_SET_MEMBER) != kDecodeOK) {
            conn->SendError("invalid set member value");
            delete it;
            return;
        }
        
        if ((long)members.size() >= count) {
            cursor = member;
            break;
        }
        
        if (pattern.empty()) {
            members.push_back(member);
        } else {
            if (stringmatchlen(pattern.c_str(), (int)pattern.size(), member.c_str(), (int)member.size(), 0)) {
                members.push_back(member);
            }
        }
    }
    
    delete it;
    
    string start_resp = "*2\r\n";
    conn->SendRawResponse(start_resp);
    conn->SendBulkString(cursor);
    conn->SendArray(members);
}
