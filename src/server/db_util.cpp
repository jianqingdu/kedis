//
//  db_util.cpp
//  kedis
//
//  Created by ziteng on 17/9/21.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "db_util.h"
#include "server.h"
#include "encoding.h"

static void delete_range(int db_idx, const string& key, uint8_t key_type, rocksdb::WriteBatch& batch)
{
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
    
    // consider use rocksdb's experimental feature DeleteRange()
    EncodeKey meta_key(key_type, key);
    rocksdb::Slice key_prefix = meta_key.GetEncodeKey();
    for (it->Seek(key_prefix); it->Valid(); it->Next()) {
        if (memcmp((void*)key_prefix.data(), (void*)it->key().data(), key_prefix.size()) != 0) {
            break;
        } else {
            batch.Delete(cf_handle, it->key());
        }
    }
    
    delete it;
}

void delete_key(int db_idx, const string& key, uint64_t ttl, uint8_t key_type)
{
    EncodeKey meta_key(KEY_TYPE_META, key);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    rocksdb::WriteBatch batch;
    
    g_server.key_count_vec[db_idx]--;
    batch.Delete(cf_handle, meta_key.GetEncodeKey());
    if (ttl > 0) {
        g_server.ttl_key_count_vec[db_idx]--;
        EncodeKey ttl_key(KEY_TYPE_TTL_SORT, ttl, key);
        batch.Delete(cf_handle, ttl_key.GetEncodeKey());
    }
    
    if ((key_type == KEY_TYPE_HASH) || (key_type == KEY_TYPE_LIST) || (key_type == KEY_TYPE_SET) || (key_type == KEY_TYPE_ZSET)) {
        delete_range(db_idx, key, key_type + 1, batch);
        if (key_type == KEY_TYPE_ZSET) {
            delete_range(db_idx, key, KEY_TYPE_ZSET_SORT, batch);
        }
    }
    
    rocksdb::Status status = g_server.db->Write(g_server.write_option, &batch);
    if (!status.ok()) {
        log_message(kLogLevelError, "delete_key WriteBatch failed: %s\n", status.ToString().c_str());
    }
}

int expire_key_if_needed(int db_idx, const string& key, MetaData& mdata, string* raw_value)
{
    EncodeKey meta_key(KEY_TYPE_META, key);
    rocksdb::Slice encode_key = meta_key.GetEncodeKey();
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    
    string encode_value;
    rocksdb::Status status = g_server.db->Get(g_server.read_option, cf_handle, encode_key, &encode_value);
    
    if (status.ok()) {
        if (raw_value) {
            *raw_value = encode_value;
        }
        
        try {
            ByteStream stream((uchar_t*)encode_value.data(), (uint32_t)encode_value.size());
            stream >> mdata.type;
            stream >> mdata.ttl;
            if (mdata.type == KEY_TYPE_STRING) {
                stream >> mdata.value;
            } else if ((mdata.type == KEY_TYPE_HASH) || (mdata.type == KEY_TYPE_LIST) ||
                       (mdata.type == KEY_TYPE_SET) || (mdata.type == KEY_TYPE_ZSET)) {
                stream >> mdata.count;
                if (mdata.type == KEY_TYPE_LIST) {
                    stream >> mdata.head_seq;
                    stream >> mdata.tail_seq;
                    stream >> mdata.current_seq;
                }
            } else {
                log_message(kLogLevelError, "wrong key type\n");
                return kExpireDBError;
            }
            
            if (mdata.ttl && mdata.ttl <= get_tick_count()) {
                delete_key(db_idx, key, mdata.ttl, mdata.type);
                g_stat.keyspace_missed++;
                g_stat.expired_keys++;
                return kExpireKeyNotExist;
            } else {
                g_stat.keyspace_hits++;
                return kExpireKeyExist;
            }
        } catch (ParseException ex) {
            log_message(kLogLevelError, "parse byte stream failed\n");
            return kExpireDBError;
        }
    } else if (status.IsNotFound()) {
        g_stat.keyspace_missed++;
        return kExpireKeyNotExist;
    } else {
        log_message(kLogLevelError, "db error: %s", status.ToString().c_str());
        return kExpireDBError;
    }
    
    return 0;
}

// used for hash, set, zset
rocksdb::Status put_meta_data(int db_idx, uint8_t key_type, const string& key, uint64_t ttl, uint64_t count,
                              rocksdb::WriteBatch* batch)
{
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey meta_key(KEY_TYPE_META, key);
    rocksdb::Status s;
    EncodeValue meta_value(key_type, ttl, count);
    if (batch) {
        s = batch->Put(cf_handle, meta_key.GetEncodeKey(), meta_value.GetEncodeValue());
    } else {
        s = g_server.db->Put(g_server.write_option, cf_handle, meta_key.GetEncodeKey(), meta_value.GetEncodeValue());
    }
    
    if (!s.ok()) {
        log_message(kLogLevelError, "put_meta_data failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

// used for list
rocksdb::Status put_meta_data(int db_idx, uint8_t key_type, const string& key, uint64_t ttl, uint64_t count,
                              uint64_t head_seq, uint64_t tail_seq, uint64_t current_seq,
                              rocksdb::WriteBatch* batch)
{
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey meta_key(KEY_TYPE_META, key);
    EncodeValue meta_value(key_type, ttl, count, head_seq, tail_seq, current_seq);
    rocksdb::Status s;
    
    if (batch) {
        s = batch->Put(cf_handle, meta_key.GetEncodeKey(), meta_value.GetEncodeValue());
    } else {
        s = g_server.db->Put(g_server.write_option, cf_handle, meta_key.GetEncodeKey(), meta_value.GetEncodeValue());
    }
    if (!s.ok()) {
        log_message(kLogLevelError, "put_meta_data failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

rocksdb::Status put_ttl_data(int db_idx, uint64_t ttl, const string& key, uint8_t key_type, rocksdb::WriteBatch* batch)
{
    if (ttl == 0) {
        return rocksdb::Status();
    }
    
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey ttl_key(KEY_TYPE_TTL_SORT, ttl, key);
    EncodeValue ttl_value(key_type);
    rocksdb::Status s;
    
    if (batch) {
        s = batch->Put(cf_handle, ttl_key.GetEncodeKey(), ttl_value.GetEncodeValue());
    } else {
        s = g_server.db->Put(g_server.write_option, cf_handle, ttl_key.GetEncodeKey(), ttl_value.GetEncodeValue());
    }
    if (!s.ok()) {
        log_message(kLogLevelError, "put_ttl_data failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

rocksdb::Status del_ttl_data(int db_idx, uint64_t ttl, const string& key, rocksdb::WriteBatch* batch)
{
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey ttl_key(KEY_TYPE_TTL_SORT, ttl, key);
    rocksdb::Status s;
    
    if (batch) {
        s = batch->Delete(cf_handle, ttl_key.GetEncodeKey());
    } else {
        s = g_server.db->Delete(g_server.write_option, cf_handle, ttl_key.GetEncodeKey());
    }
    if (!s.ok()) {
        log_message(kLogLevelError, "put_ttl_data failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

void put_kv_data(int db_idx, const string& key, const string& value, uint64_t ttl, rocksdb::WriteBatch* batch)
{
    EncodeKey meta_key(KEY_TYPE_META, key);
    EncodeValue meta_value(KEY_TYPE_STRING, ttl, value);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    rocksdb::Status s;
    
    if (!batch) {
        s = g_server.db->Put(g_server.write_option, cf_handle, meta_key.GetEncodeKey(), meta_value.GetEncodeValue());
    } else {
        s = batch->Put(cf_handle, meta_key.GetEncodeKey(), meta_value.GetEncodeValue());
    }
    if (!s.ok()) {
        log_message(kLogLevelError, "put_kv_data failed, error_code=%d\n", (int)s.code());
    }
}

// use to remove trailing zeroes
string double_to_string(double value)
{
    char buf[256];
    int len;
    len = snprintf(buf,sizeof(buf),"%.17lf", value);
    // Now remove trailing zeroes after the '.'
    if (strchr(buf,'.') != NULL) {
        char *p = buf+len-1;
        while (*p == '0') {
            p--;
            len--;
        }
        if (*p == '.')
            len--;
    }
    
    return string(buf, len);
}

int parse_scan_param(ClientConn* conn, const vector<string>& cmd_vec, int start_index, string& pattern, long& count)
{
    int cmd_size = (int)cmd_vec.size();
    int i = start_index;
    while (i < cmd_size) {
        if (!strcasecmp(cmd_vec[i].c_str(), "count") && (i < cmd_size - 1)) {
            if (get_long_from_string(cmd_vec[i + 1], count) == CODE_ERROR) {
                conn->SendError("syntax error");
                return CODE_ERROR;
            }
            
            i += 2;
        } else if (!strcasecmp(cmd_vec[i].c_str(), "match") && (i < cmd_size - 1)) {
            pattern = cmd_vec[i + 1];
            i += 2;
        } else {
            conn->SendError("syntax error");
            return CODE_ERROR;
        }
    }
    
    return CODE_OK;
}

// Glob-style pattern matching
int stringmatchlen(const char *pattern, int patternLen, const char *str, int strLen, int nocase)
{
    while(patternLen) {
        switch(pattern[0]) {
            case '*':
                while (pattern[1] == '*') {
                    pattern++;
                    patternLen--;
                }
                if (patternLen == 1)
                    return 1; /* match */
                while(strLen) {
                    if (stringmatchlen(pattern+1, patternLen-1, str, strLen, nocase))
                        return 1; /* match */
                    str++;
                    strLen--;
                }
                return 0; /* no match */
                break;
            case '?':
                if (strLen == 0)
                    return 0; /* no match */
                str++;
                strLen--;
                break;
            case '[':
            {
                int exclusion, match;
                
                pattern++;
                patternLen--;
                exclusion = pattern[0] == '^';
                if (exclusion) {
                    pattern++;
                    patternLen--;
                }
                match = 0;
                while(1) {
                    if (pattern[0] == '\\') {
                        pattern++;
                        patternLen--;
                        if (pattern[0] == str[0])
                            match = 1;
                    } else if (pattern[0] == ']') {
                        break;
                    } else if (patternLen == 0) {
                        pattern--;
                        patternLen++;
                        break;
                    } else if (pattern[1] == '-' && patternLen >= 3) {
                        int start = pattern[0];
                        int end = pattern[2];
                        int c = str[0];
                        if (start > end) {
                            int t = start;
                            start = end;
                            end = t;
                        }
                        if (nocase) {
                            start = tolower(start);
                            end = tolower(end);
                            c = tolower(c);
                        }
                        pattern += 2;
                        patternLen -= 2;
                        if (c >= start && c <= end)
                            match = 1;
                    } else {
                        if (!nocase) {
                            if (pattern[0] == str[0])
                                match = 1;
                        } else {
                            if (tolower((int)pattern[0]) == tolower((int)str[0]))
                                match = 1;
                        }
                    }
                    pattern++;
                    patternLen--;
                }
                if (exclusion)
                    match = !match;
                if (!match)
                    return 0; /* no match */
                str++;
                strLen--;
                break;
            }
            case '\\':
                if (patternLen >= 2) {
                    pattern++;
                    patternLen--;
                }
                /* fall through */
            default:
                if (!nocase) {
                    if (pattern[0] != str[0])
                        return 0; /* no match */
                } else {
                    if (tolower((int)pattern[0]) != tolower((int)str[0]))
                        return 0; /* no match */
                }
                str++;
                strLen--;
                break;
        }
        pattern++;
        patternLen--;
        if (strLen == 0) {
            while(*pattern == '*') {
                pattern++;
                patternLen--;
            }
            break;
        }
    }
    if (patternLen == 0 && strLen == 0)
        return 1;
    return 0;
}
