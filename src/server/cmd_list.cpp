//
//  cmd_list.cpp
//  kedis
//
//  Created by ziteng on 17/7/20.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "cmd_list.h"
#include "db_util.h"
#include "encoding.h"

static rocksdb::Status put_list_element(int db_idx, const string& key, uint64_t seq, uint64_t prev_seq,
                                        uint64_t next_seq, const string& value, rocksdb::WriteBatch* batch = NULL)
{
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey element_key(KEY_TYPE_LIST_ELEMENT, key, seq);
    EncodeValue element_value(KEY_TYPE_LIST_ELEMENT, prev_seq, next_seq, value);
    rocksdb::Status s;
    
    if (batch) {
        s = batch->Put(cf_handle, element_key.GetEncodeKey(), element_value.GetEncodeValue());
    } else {
        s = g_server.db->Put(g_server.write_option, cf_handle, element_key.GetEncodeKey(), element_value.GetEncodeValue());
    }
    if (!s.ok()) {
        log_message(kLogLevelError, "put_list_element failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

static rocksdb::Status del_list_element(int db_idx, const string& key, uint64_t seq, rocksdb::WriteBatch* batch = NULL)
{
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey element_key(KEY_TYPE_LIST_ELEMENT, key, seq);
    rocksdb::Status s;
    
    if (batch) {
        s = batch->Delete(cf_handle, element_key.GetEncodeKey());
    } else {
        s = g_server.db->Delete(g_server.write_option, cf_handle, element_key.GetEncodeKey());
    }
    if (!s.ok()) {
        log_message(kLogLevelError, "del_set_member failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

static int get_list_element(int db_idx, const string& key, uint64_t seq, uint64_t& prev_seq, uint64_t& next_seq, string& value)
{
    EncodeKey element_key(KEY_TYPE_LIST_ELEMENT, key, seq);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    string encode_value;
    
    rocksdb::Status status = g_server.db->Get(g_server.read_option, cf_handle, element_key.GetEncodeKey(), &encode_value);
    if (status.ok()) {
        if (DecodeValue::Decode(encode_value, KEY_TYPE_LIST_ELEMENT, prev_seq, next_seq, value) == kDecodeOK) {
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

void lindex_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long index;
    if (get_long_from_string(cmd_vec[2], index) == CODE_ERROR) {
        conn->SendError("index is not valid");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendRawResponse(kNullBulkString);
    } else {
        if (mdata.type != KEY_TYPE_LIST) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        bool forward = index >= 0 ? true : false;
        uint64_t seq;
        
        if (forward) {
            seq = mdata.head_seq;
        } else {
            seq = mdata.tail_seq;
            index = -index - 1;
        }
        
        if (index >= (long)mdata.count) {
            conn->SendRawResponse(kNullBulkString);
            return;
        }
        
        uint64_t prev_seq, next_seq;
        string value;
        for (long i = 0; i <= index; i++) {
            value.clear();
            get_list_element(db_idx, cmd_vec[1], seq, prev_seq, next_seq, value);
            seq = forward ? next_seq : prev_seq;
        }
        
        conn->SendBulkString(value);
    }
}

// LINSERT key BEFORE|AFTER pivot value
void linsert_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    bool insert_before = false;
    if (!strcasecmp(cmd_vec[2].c_str(), "before")) {
        insert_before = true;
    } else if (!strcasecmp(cmd_vec[2].c_str(), "after")) {
        insert_before = false;
    } else {
        conn->SendError("syntax error");
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
        conn->SendInteger(0);
    } else {
        if (mdata.type != KEY_TYPE_LIST) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        bool inserted = false;
        uint64_t seq = mdata.head_seq;
        for (uint64_t i = 0; i < mdata.count; i++) {
            uint64_t prev_seq, next_seq;
            string value;
            int result = get_list_element(db_idx, cmd_vec[1], seq, prev_seq, next_seq, value);
            if (result != FIELD_EXIST) {
                conn->SendError("db error");
                return;
            }
            
            if (value == cmd_vec[3]) {
                uint64_t p_seq, n_seq;
                string v;
                if (insert_before) {
                    if (prev_seq == 0) {
                        // insert before the head element
                        mdata.head_seq = mdata.current_seq;
                        put_list_element(db_idx, cmd_vec[1], mdata.current_seq, 0, seq, cmd_vec[4], &batch);
                        put_list_element(db_idx, cmd_vec[1], seq, mdata.current_seq, next_seq, value, &batch);
                    } else {
                        // after insert, the three adjacent sequence are prev_seq, mdata.current_seq, seq
                        get_list_element(db_idx, cmd_vec[1], prev_seq, p_seq, n_seq, v);
                        
                        put_list_element(db_idx, cmd_vec[1], prev_seq, p_seq, mdata.current_seq, v, &batch);
                        put_list_element(db_idx, cmd_vec[1], mdata.current_seq, prev_seq, seq, cmd_vec[4], &batch);
                        put_list_element(db_idx, cmd_vec[1], seq, mdata.current_seq, next_seq, value, &batch);
                    }
                } else {
                    if (next_seq == 0) {
                        // insert after the tail element
                        mdata.tail_seq = mdata.current_seq;
                        put_list_element(db_idx, cmd_vec[1], mdata.current_seq, seq, 0, cmd_vec[4], &batch);
                        put_list_element(db_idx, cmd_vec[1], seq, prev_seq, mdata.current_seq, value, &batch);
                    } else {
                        // after insert, the three adjacent sequence are seq, mdata.current_seq, next_seq
                        put_list_element(db_idx, cmd_vec[1], seq, prev_seq, mdata.current_seq, value, &batch);
                        put_list_element(db_idx, cmd_vec[1], mdata.current_seq, seq, next_seq, cmd_vec[4], &batch);
                        
                        get_list_element(db_idx, cmd_vec[1], next_seq, p_seq, n_seq, v);
                        put_list_element(db_idx, cmd_vec[1], next_seq, mdata.current_seq, n_seq, v, &batch);
                    }
                }
                
                inserted = true;
                break;
            }
            
            seq = next_seq;
        }

        if (inserted) {
            mdata.count++;
            mdata.current_seq++;
            put_meta_data(db_idx, KEY_TYPE_LIST, cmd_vec[1], mdata.ttl, mdata.count, mdata.head_seq, mdata.tail_seq,
                          mdata.current_seq, &batch);
            DB_BATCH_UPDATE(batch)
            g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
            conn->SendInteger(mdata.count);
        } else {
            conn->SendInteger(-1);
        }
    }
}

void llen_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_LIST) {
            conn->SendRawResponse(kWrongTypeError);
        } else {
            conn->SendInteger(mdata.count);
        }
    }
}

void lpush_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int cmd_size = (int)cmd_vec.size();
    uint64_t element_count = cmd_size - 2;
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        g_server.key_count_vec[db_idx]++;
        uint64_t current_seq = 1;
        for (int i = 2; i < cmd_size; i++) {
            uint64_t prev_seq = (i == cmd_size - 1) ? 0 : current_seq + 1;
            uint64_t next_seq = (i == 2) ? 0 : current_seq - 1;
            put_list_element(db_idx, cmd_vec[1], current_seq, prev_seq, next_seq, cmd_vec[i], &batch);
            current_seq++;
        }
        
        uint64_t head_seq = current_seq - 1;
        uint64_t tail_seq = 1;
        put_meta_data(db_idx, KEY_TYPE_LIST, cmd_vec[1], 0, element_count, head_seq, tail_seq, current_seq, &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(element_count);
    } else {
        if (mdata.type != KEY_TYPE_LIST) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        uint64_t prev_seq, next_seq;
        string value;
        int result = get_list_element(db_idx, cmd_vec[1], mdata.head_seq, prev_seq, next_seq, value);
        if (result == FIELD_EXIST) {
            put_list_element(db_idx, cmd_vec[1], mdata.head_seq, mdata.current_seq, next_seq, value, &batch);
        } else {
            // element not exist or db error all means the list has broken
            conn->SendError("db error");
            return;
        }
        
        for (int i = 2; i < cmd_size; i++) {
            uint64_t prev_seq = (i == cmd_size - 1) ? 0 :  mdata.current_seq + 1;
            uint64_t next_seq = (i == 2) ? mdata.head_seq :  mdata.current_seq - 1;
            put_list_element(db_idx, cmd_vec[1], mdata.current_seq, prev_seq, next_seq, cmd_vec[i], &batch);
            mdata.current_seq++;
        }
    
        mdata.count += element_count;
        uint64_t head_seq = mdata.current_seq - 1;
        put_meta_data(db_idx, KEY_TYPE_LIST, cmd_vec[1], mdata.ttl, mdata.count, head_seq, mdata.tail_seq, mdata.current_seq, &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(mdata.count);
    }
}

void lrange_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long start, end;
    if (get_long_from_string(cmd_vec[2], start) == CODE_ERROR || get_long_from_string(cmd_vec[3], end) == CODE_ERROR) {
        conn->SendError("start|end is not a valid integer");
        return;
    }
    
    vector<string> list_vec;
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendArray(list_vec);
    } else {
        if (mdata.type != KEY_TYPE_LIST) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        /* convert negative indexes */
		long lcount = (long)mdata.count;
        if (start < 0) start = lcount + start;
        if (end < 0) end = lcount + end;
        if (start < 0) start = 0;
        
        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= lcount) {
            conn->SendArray(list_vec);
            return;
        }
        
        if (end >= lcount) end = lcount - 1;
        
        uint64_t seq = mdata.head_seq;
        for (long i = 0; i <= end; i++) {
            uint64_t prev_seq;
            uint64_t next_seq;
            string value;
            if (get_list_element(db_idx, cmd_vec[1], seq, prev_seq, next_seq, value) != FIELD_EXIST) {
                conn->SendError("db error");
                return;
            }
            
            seq = next_seq;
            if (i >= start) {
                list_vec.push_back(value);
            }
        }
        
        conn->SendArray(list_vec);
    }
}

void rpush_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    int cmd_size = (int)cmd_vec.size();
    uint64_t element_count = cmd_size - 2;
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        g_server.key_count_vec[db_idx]++;
        uint64_t current_seq = 1;
        for (int i = 2; i < cmd_size; i++) {
            uint64_t prev_seq = (i == 2) ? 0 : current_seq - 1;
            uint64_t next_seq = (i == cmd_size - 1) ? 0 : current_seq + 1;
            put_list_element(db_idx, cmd_vec[1], current_seq,  prev_seq, next_seq, cmd_vec[i], &batch);
            current_seq++;
        }
        
        uint64_t head_seq = 1;
        uint64_t tail_seq = current_seq - 1;
        put_meta_data(db_idx, KEY_TYPE_LIST, cmd_vec[1], 0, element_count, head_seq, tail_seq, current_seq, &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(element_count);
    } else {
        if (mdata.type != KEY_TYPE_LIST) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        uint64_t prev_seq, next_seq;
        string value;
        int result = get_list_element(db_idx, cmd_vec[1], mdata.tail_seq,  prev_seq, next_seq, value);
        if (result == FIELD_EXIST) {
            put_list_element(db_idx, cmd_vec[1], mdata.tail_seq, prev_seq, mdata.current_seq, value, &batch);
        } else {
            // element not exist or db error all means the list has broken
            conn->SendError("db error");
            return;
        }
        
        for (int i = 2; i < cmd_size; i++) {
            uint64_t prev_seq = (i == 2) ? mdata.tail_seq :  mdata.current_seq - 1;
            uint64_t next_seq = (i == cmd_size - 1) ? 0 :  mdata.current_seq + 1;
            put_list_element(db_idx, cmd_vec[1], mdata.current_seq, prev_seq, next_seq, cmd_vec[i], &batch);
            mdata.current_seq++;
        }
        
        mdata.count += element_count;
        mdata.tail_seq = mdata.current_seq - 1;
        put_meta_data(db_idx, KEY_TYPE_LIST, cmd_vec[1], mdata.ttl,  mdata.count, mdata.head_seq, mdata.tail_seq,
                      mdata.current_seq, &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(mdata.count);
    }
}

static void generic_pop_command(ClientConn* conn, const vector<string>& cmd_vec, bool pop_head)
{
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendRawResponse(kNullBulkString);
    } else {
        if (mdata.type != KEY_TYPE_LIST) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        uint64_t prev_seq, next_seq;
        string value;
        uint64_t seq = pop_head ? mdata.head_seq : mdata.tail_seq;
        
        int result = get_list_element(db_idx, cmd_vec[1], seq, prev_seq, next_seq, value);
        if (result != FIELD_EXIST) {
            // element not exist or db error all means the list has broken
            conn->SendError("db error");
            return;
        }
        
        if (mdata.count == 1) {
            // last element in the list, delete key
            delete_key(db_idx, cmd_vec[1], mdata.ttl, KEY_TYPE_LIST);
        } else {
            // delete list element, update the new head or tail element
            del_list_element(db_idx, cmd_vec[1], seq, &batch);
            
            uint64_t edge_seq = pop_head ? next_seq : prev_seq;
            string edge_value;
            int result = get_list_element(db_idx, cmd_vec[1], edge_seq, prev_seq, next_seq, edge_value);
            if (result != FIELD_EXIST) {
                // element not exist or db error all means the list has broken
                conn->SendError("db error");
                return;
            }
            
            if (pop_head) {
                put_list_element(db_idx, cmd_vec[1], edge_seq, 0, next_seq, edge_value, &batch);
                put_meta_data(db_idx, KEY_TYPE_LIST, cmd_vec[1], mdata.ttl, mdata.count - 1, edge_seq,
                              mdata.tail_seq, mdata.current_seq, &batch);
            } else {
                put_list_element(db_idx, cmd_vec[1], edge_seq, prev_seq, 0, edge_value, &batch);
                put_meta_data(db_idx, KEY_TYPE_LIST, cmd_vec[1], mdata.ttl, mdata.count - 1, mdata.head_seq,
                              edge_seq, mdata.current_seq, &batch);
            }
            DB_BATCH_UPDATE(batch)
        }
        
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendBulkString(value);
    }
}

void lpop_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_pop_command(conn, cmd_vec, true);
}

void rpop_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_pop_command(conn, cmd_vec, false);
}

void lpushx_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_LIST) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        uint64_t prev_seq, next_seq;
        string value;
        int result = get_list_element(db_idx, cmd_vec[1], mdata.head_seq, prev_seq, next_seq, value);
        if (result != FIELD_EXIST) {
            // element not exist or db error all means the list has broken
            conn->SendError("db error");
            return;
        }
        
        // update old head element
        put_list_element(db_idx, cmd_vec[1], mdata.head_seq, mdata.current_seq, next_seq, value, &batch);
        
        // update new head element
        put_list_element(db_idx, cmd_vec[1], mdata.current_seq, 0, mdata.head_seq, cmd_vec[2], &batch);
        
        // update meta data
        mdata.head_seq = mdata.current_seq;
        mdata.current_seq++;
        mdata.count++;
        put_meta_data(db_idx, KEY_TYPE_LIST, cmd_vec[1], mdata.ttl, mdata.count, mdata.head_seq, mdata.tail_seq,
                      mdata.current_seq, &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(mdata.count);
    }
}

void rpushx_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_LIST) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        uint64_t prev_seq, next_seq;
        string value;
        int result = get_list_element(db_idx, cmd_vec[1], mdata.tail_seq, prev_seq, next_seq, value);
        if (result != FIELD_EXIST) {
            // element not exist or db error all means the list has broken
            conn->SendError("db error");
            return;
        }
        
        // update old tail element
        put_list_element(db_idx, cmd_vec[1], mdata.tail_seq, prev_seq, mdata.current_seq, value, &batch);
        
        // update new tail element
        put_list_element(db_idx, cmd_vec[1], mdata.current_seq, mdata.tail_seq, 0, cmd_vec[2], &batch);
        
        // update meta data
        mdata.tail_seq = mdata.current_seq;
        mdata.current_seq++;
        mdata.count++;
        put_meta_data(db_idx, KEY_TYPE_LIST, cmd_vec[1], mdata.ttl, mdata.count, mdata.head_seq, mdata.tail_seq,
                      mdata.current_seq, &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(mdata.count);
    }
}

// lrem key count value
void lrem_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long toremove;
    if (get_long_from_string(cmd_vec[2], toremove) == CODE_ERROR) {
        conn->SendError("count is not a valid number");
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
        conn->SendInteger(0);
    } else {
        if (mdata.type != KEY_TYPE_LIST) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        bool forward = true;
        long removed = 0;
        if (toremove < 0) {
            toremove = -toremove;
            forward = false;
        }
        
        uint64_t count = mdata.count;
        uint64_t seq = forward ? mdata.head_seq : mdata.tail_seq;
        for (uint64_t i = 0; i < count; i++) {
            uint64_t prev_seq, next_seq;
            string value;
            int result = get_list_element(db_idx, cmd_vec[1], seq, prev_seq, next_seq, value);
            if (result != FIELD_EXIST) {
                // element not exist or db error all means the list has broken
                conn->SendError("db error");
                return;
            }
            
            if (value == cmd_vec[3]) {
                del_list_element(db_idx, cmd_vec[1], seq);
                
                value.clear();
                uint64_t p_seq, n_seq;
                if (prev_seq == 0) {
                    // this is the head of the list
                    mdata.head_seq = next_seq;
                    get_list_element(db_idx, cmd_vec[1], mdata.head_seq, p_seq, n_seq, value);
                    put_list_element(db_idx, cmd_vec[1], mdata.head_seq, 0, n_seq, value, &batch);
                } else if (next_seq == 0) {
                    // this is the tail of the list
                    mdata.tail_seq = prev_seq;
                    get_list_element(db_idx, cmd_vec[1], mdata.tail_seq, p_seq, n_seq, value);
                    put_list_element(db_idx, cmd_vec[1], mdata.tail_seq, p_seq, 0, value, &batch);
                } else {
                    // this is the middle element of the list
                    uint64_t seq2 = prev_seq;
                    get_list_element(db_idx, cmd_vec[1], seq2, p_seq, n_seq, value);
                    put_list_element(db_idx, cmd_vec[1], seq2, p_seq, next_seq, value, &batch);
                    
                    value.clear();
                    seq2 = next_seq;
                    get_list_element(db_idx, cmd_vec[1], seq2, p_seq, n_seq, value);
                    put_list_element(db_idx, cmd_vec[1], seq2, prev_seq, n_seq, value, &batch);
                }
                
                // update to db whenever delete an element, otherwise the process of delete
                // continuous same value elements will be very complex
                mdata.count--;
                if (mdata.count == 0) {
                    delete_key(db_idx, cmd_vec[1], mdata.ttl, KEY_TYPE_LIST);
                } else {
                    put_meta_data(db_idx, KEY_TYPE_LIST, cmd_vec[1], mdata.ttl, mdata.count, mdata.head_seq, mdata.tail_seq,
                                  mdata.current_seq, &batch);
                    DB_BATCH_UPDATE(batch)
                }
                
                removed++;
                if (toremove && (removed == toremove)) {
                    break;
                }
            }
            
            seq = forward ? next_seq : prev_seq;
        }
        
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendInteger(removed);
    }
}

void lset_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long index;
    if (get_long_from_string(cmd_vec[2], index) == CODE_ERROR) {
        conn->SendError("index is not valid");
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
        conn->SendError("no such key");
    } else {
        if (mdata.type != KEY_TYPE_LIST) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        bool forward = index >= 0 ? true : false;
        uint64_t seq;
        
        if (forward) {
            seq = mdata.head_seq;
        } else {
            seq = mdata.tail_seq;
            index = -index - 1;
        }
        
        if (index >= (long)mdata.count) {
            conn->SendError("index out of range");
            return;
        }
        
        uint64_t prev_seq = 0, next_seq = 0;
        string value;
        for (long i = 0; i <= index; i++) {
            value.clear();
            get_list_element(db_idx, cmd_vec[1], seq, prev_seq, next_seq, value);
            if (i == index) {
                break;
            }
            seq = forward ? next_seq : prev_seq;
        }
        
        put_list_element(db_idx, cmd_vec[1], seq, prev_seq, next_seq, cmd_vec[3]);
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendSimpleString("OK");
    }
}

// ltrim key start stop
void ltrim_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long start, end;
    if (get_long_from_string(cmd_vec[2], start) == CODE_ERROR || get_long_from_string(cmd_vec[3], end) == CODE_ERROR) {
        conn->SendError("start|stop is not a valid integer");
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
        conn->SendSimpleString("OK");
    } else {
        if (mdata.type != KEY_TYPE_LIST) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        /* convert negative indexes */
        long lcount = (long)mdata.count;
        if (start < 0) start = lcount + start;
        if (end < 0) end = lcount + end;
        if (start < 0) start = 0;
        
        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= lcount) {
            /* Out of range start or start > end result in empty list */
            delete_key(db_idx, cmd_vec[1], mdata.ttl, KEY_TYPE_LIST);
            conn->SendSimpleString("OK");
            return;
        }
        
        if (end >= lcount) end = lcount - 1;
        
        long ltrim = start;
        long rtrim = lcount - end - 1;
        
        string value;
        uint64_t prev_seq, next_seq;
        uint64_t seq = mdata.head_seq;
        for (long i = 0; i < ltrim; i++) {
            value.clear();
            get_list_element(db_idx, cmd_vec[1], seq, prev_seq, next_seq, value);
            del_list_element(db_idx, cmd_vec[1], seq, &batch);
            seq = next_seq;
            
            if (i == ltrim - 1) {
                mdata.head_seq = next_seq;
                value.clear();
                get_list_element(db_idx, cmd_vec[1], seq, prev_seq, next_seq, value);
                put_list_element(db_idx, cmd_vec[1], seq, 0, next_seq, value, &batch);
            }
        }
        
        seq = mdata.tail_seq;
        for (long i = 0; i < rtrim; i++) {
            value.clear();
            get_list_element(db_idx, cmd_vec[1], seq, prev_seq, next_seq, value);
            del_list_element(db_idx, cmd_vec[1], seq, &batch);
            seq = prev_seq;
            
            if (i == rtrim - 1) {
                mdata.tail_seq = prev_seq;
                value.clear();
                get_list_element(db_idx, cmd_vec[1], seq, prev_seq, next_seq, value);
                put_list_element(db_idx, cmd_vec[1], seq, prev_seq, 0, value, &batch);
            }
        }
        
        mdata.count = end - start + 1;
        put_meta_data(db_idx, KEY_TYPE_LIST, cmd_vec[1], mdata.ttl, mdata.count, mdata.head_seq, mdata.tail_seq,
                      mdata.current_seq, &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        conn->SendSimpleString("OK");
    }
}
