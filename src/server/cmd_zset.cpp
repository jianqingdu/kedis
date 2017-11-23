//
//  cmd_zset.cpp
//  kedis
//
//  Created by ziteng on 17/7/20.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "cmd_zset.h"
#include "db_util.h"
#include "encoding.h"
#include "rocksdb/comparator.h"
#include <math.h>

// Struct to hold a inclusive/exclusive range spec by score comparison
typedef struct {
    double min, max;
    uint64_t encode_min, encode_max;
    int minex, maxex; // are min or max exclusive?
} zrangespec;

static bool parse_range(const string& min_obj, const string& max_obj, zrangespec& range)
{
    char *eptr;
    
    /* Parse the min-max interval, and convert to long. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
    if (min_obj.c_str()[0] == '(') {
        range.min = strtod(min_obj.c_str() + 1, &eptr);
        if (eptr[0] != '\0' || isnan(range.min)) {
            return false;
        }
        range.minex = 1;
    } else {
        range.min = strtod(min_obj.c_str(), &eptr);
        if (eptr[0] != '\0' || isnan(range.min)) {
            return false;
        }
        range.minex = 0;
    }

    if (max_obj.c_str()[0] == '(') {
        range.max = strtod(max_obj.c_str() + 1, &eptr);
        if (eptr[0] != '\0' || isnan(range.max)) {
            return false;
        }
        range.maxex = 1;
    } else {
        range.max = strtod(max_obj.c_str(), &eptr);
        if (eptr[0] != '\0' || isnan(range.max)) {
            return false;
        }
        range.maxex = 0;
    }
    
    range.encode_min = double_to_uint64(range.min);
    range.encode_max = double_to_uint64(range.max);
    return true;
}

bool value_gte_min(uint64_t value, const zrangespec& spec)
{
    return spec.minex ? (value > spec.encode_min) : (value >= spec.encode_min);
}

bool value_lte_max(uint64_t value, const zrangespec& spec)
{
    return spec.maxex ? (value < spec.encode_max) : (value <= spec.encode_max);
}

bool lex_value_gte_min(const string value, const string& min_key, bool exclusive, const rocksdb::Comparator* comparator)
{
    return exclusive ? (comparator->Compare(value, min_key) > 0) : (comparator->Compare(value, min_key) >= 0);
}

bool lex_value_lte_max(const string value, const string& max_key, bool exclusive, const rocksdb::Comparator* comparator)
{
    return exclusive ? (comparator->Compare(value, max_key) < 0) : (comparator->Compare(value, max_key) <= 0);
}

/* Parse max or min argument of ZRANGEBYLEX.
 * (foo means foo (open interval)
 * [foo means foo (closed interval)
 * - means the min string possible
 * + means the max string possible
 *
 * If the string is valid the *dest pointer is set to the redis object
 * that will be used for the comparision, and ex will be set to 0 or 1
 * respectively if the item is exclusive or inclusive. C_OK will be
 * returned.
 *
 * If the string is not a valid range C_ERR is returned, and the value
 * of *dest and *ex is undefined. */
bool parse_lex_range(const string& range_old, string& range_new, bool& exclusive)
{
    switch (range_old.at(0)) {
        case '+':
            if (range_old.size() != 1)
                return false;
            exclusive = false;
            range_new.append(128, 0xFF); // assume this is the maximum string
            return true;
        case '-':
            if (range_old.size() != 1)
                return false;
            exclusive = false;
            range_new = "";
            return true;
        case '(':
            exclusive = true;
            range_new = range_old.substr(1);
            return true;
        case '[':
            exclusive = false;
            range_new = range_old.substr(1);
            return true;
        default:
            return false;
    }
}

static rocksdb::Status put_zset_score(int db_idx, const string& key, const string& member, double score,
                                      rocksdb::WriteBatch* batch = NULL)
{
    uint64_t encode_score = double_to_uint64(score);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey member_key(KEY_TYPE_ZSET_SCORE, key, member);
    EncodeValue member_value(KEY_TYPE_ZSET_SCORE, encode_score);
    rocksdb::Status s;
    
    if (batch) {
        s = batch->Put(cf_handle, member_key.GetEncodeKey(), member_value.GetEncodeValue());
    } else {
        s = g_server.db->Put(g_server.write_option, cf_handle, member_key.GetEncodeKey(), member_value.GetEncodeValue());
    }
    if (!s.ok()) {
        log_message(kLogLevelError, "put_zset_score failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

static rocksdb::Status del_zset_score(int db_idx, const string& key, const string& member, rocksdb::WriteBatch* batch = NULL)
{
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey member_key(KEY_TYPE_ZSET_SCORE, key, member);
    rocksdb::Status s;
    
    if (batch) {
        s = batch->Delete(cf_handle, member_key.GetEncodeKey());
    } else {
        s = g_server.db->Delete(g_server.write_option, cf_handle, member_key.GetEncodeKey());
    }
    if (!s.ok()) {
        log_message(kLogLevelError, "del_zset_score failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

static int get_zset_score(int db_idx, const string& key, const string& member, double& score)
{
    EncodeKey member_key(KEY_TYPE_ZSET_SCORE, key, member);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    string encode_value;
    uint64_t encode_score;
    
    rocksdb::Status status = g_server.db->Get(g_server.read_option, cf_handle, member_key.GetEncodeKey(), &encode_value);
    if (status.ok()) {
        if (DecodeValue::Decode(encode_value, KEY_TYPE_ZSET_SCORE, encode_score) == kDecodeOK) {
            score = uint64_to_double(encode_score);
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

static rocksdb::Status put_zset_sort(int db_idx, const string& key, double score, const string& member,
                                     rocksdb::WriteBatch* batch = NULL)
{
    uint64_t encode_score = double_to_uint64(score);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey member_key(KEY_TYPE_ZSET_SORT, key, encode_score, member);
    EncodeValue member_value(KEY_TYPE_ZSET_SORT);
    rocksdb::Status s;
    
    if (batch) {
        s = batch->Put(cf_handle, member_key.GetEncodeKey(), member_value.GetEncodeValue());
    } else {
        s = g_server.db->Put(g_server.write_option, cf_handle, member_key.GetEncodeKey(), member_value.GetEncodeValue());
    }
    if (!s.ok()) {
        log_message(kLogLevelError, "put_zset_sort failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

static rocksdb::Status del_zset_sort(int db_idx, const string& key, double score, const string& member,
                                     rocksdb::WriteBatch* batch = NULL)
{
    uint64_t encode_score = double_to_uint64(score);
    rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
    EncodeKey member_key(KEY_TYPE_ZSET_SORT, key, encode_score, member);
    rocksdb::Status s;
    
    if (batch) {
        s = batch->Delete(cf_handle, member_key.GetEncodeKey());
    } else {
        s = g_server.db->Delete(g_server.write_option, cf_handle, member_key.GetEncodeKey());
    }
    if (!s.ok()) {
        log_message(kLogLevelError, "del_zset_sort failed, error_code=%d\n", (int)s.code());
    }
    return s;
}

static void generic_zadd_command(ClientConn* conn, const vector<string>& cmd_vec, bool incr)
{
    bool nx = false;
    bool xx = false;
    bool ch = false;
    
    int cmd_size = (int)cmd_vec.size();
    int score_idx = 2;
    for ( ; score_idx < cmd_size; score_idx++) {
        if (!strcasecmp(cmd_vec[score_idx].c_str(), "nx")) {
            nx = true;
        } else if (!strcasecmp(cmd_vec[score_idx].c_str(), "xx")) {
            xx = true;
        } else if (!strcasecmp(cmd_vec[score_idx].c_str(), "ch")) {
            ch = true;
        } else if (!strcasecmp(cmd_vec[score_idx].c_str(), "incr")) {
            incr = true;
        } else {
            break;
        }
    }
    
    int elements = cmd_size - score_idx;
    if (elements % 2 != 0) {
        conn->SendError("syntax error");
        return;
    }
    
    if (nx && xx) {
        conn->SendError("XX and NX options at the same time are not compatible");
        return;
    }
    
    elements /= 2;
    if (incr && elements > 1) {
        conn->SendError("INCR option supports a single increment-element pair");
        return;
    }
    
    for (int i = score_idx; i < cmd_size; i += 2) {
        double score;
        if (get_double_from_string(cmd_vec[i], score) == CODE_ERROR) {
            conn->SendError("value is not a valid float");
            return;
        }
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        if (xx) {
            conn->SendInteger(0);
            return;
        }
        
        for (int i = score_idx; i < cmd_size; i += 2) {
            double score = std::stod(cmd_vec[i]);
            const string& member = cmd_vec[i + 1];
            
            put_zset_score(db_idx, cmd_vec[1], member, score, &batch);
            put_zset_sort(db_idx, cmd_vec[1], score, member, &batch);
        }
        
        g_server.key_count_vec[db_idx]++;
        put_meta_data(db_idx, KEY_TYPE_ZSET, cmd_vec[1], 0, elements, &batch);
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        if (incr) {
            conn->SendBulkString(cmd_vec[score_idx]);
        } else {
            conn->SendInteger(elements);
        }
    } else {
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        int add_cnt = 0;
        int ch_cnt = 0;
        double incr_result_score = 0;
        bool processed = false;
        for (int i = score_idx; i < cmd_size; i += 2) {
            double score = std::stod(cmd_vec[i]);
            const string& member = cmd_vec[i + 1];
            
            double old_score;
            int result = get_zset_score(db_idx, cmd_vec[1], member, old_score);
            if (result == FIELD_EXIST) {
                if (nx) {
                    continue;
                }
                
                processed = true;
                if (incr) {
                    score += old_score;
                    if (isnan(score)) {
                        conn->SendError("resulting score is not a number (NaN)");
                        return;
                    }
                    incr_result_score = score;
                }
                
                if (score != old_score) {
                    ch_cnt++;
                    del_zset_score(db_idx, cmd_vec[1], member, &batch);
                    del_zset_sort(db_idx, cmd_vec[1], old_score, member, &batch);
                    
                    put_zset_score(db_idx, cmd_vec[1], member, score, &batch);
                    put_zset_sort(db_idx, cmd_vec[1], score, member, &batch);
                }
            } else if (result == FIELD_NOT_EXIST) {
                if (xx) {
                    continue;
                }
                
                processed = true;
                add_cnt++;
                ch_cnt++;
                put_zset_score(db_idx, cmd_vec[1], member, score, &batch);
                put_zset_sort(db_idx, cmd_vec[1], score, member, &batch);
                
                if (incr) {
                    incr_result_score = score;
                }
            } else {
                conn->SendError("db error");
                return;
            }
        }
        
        if (add_cnt > 0) {
            put_meta_data(db_idx, KEY_TYPE_ZSET, cmd_vec[1], mdata.ttl, mdata.count + add_cnt, &batch);
        }
        DB_BATCH_UPDATE(batch)
        g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        
        if (incr) {
            if (processed) {
                conn->SendBulkString(double_to_string(incr_result_score));
            } else {
                conn->SendRawResponse(kNullBulkString);
            }
        } else {
            if (ch) {
                conn->SendInteger(ch_cnt);
            } else {
                conn->SendInteger(add_cnt);
            }
        }
    }
}

void zadd_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_zadd_command(conn, cmd_vec, false);
}

void zincrby_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_zadd_command(conn, cmd_vec, true);
}

void zcard_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
        } else {
            conn->SendInteger(mdata.count);
        }
    }
}

void zcount_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    zrangespec range;
    if (!parse_range(cmd_vec[2], cmd_vec[3], range)) {
        conn->SendError("min or max is not a float");
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
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        EncodeKey start_key(KEY_TYPE_ZSET_SORT, cmd_vec[1], range.encode_min, "");
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
        
        uint64_t range_cnt = 0;
        for (it->Seek(start_key.GetEncodeKey()); it->Valid(); it->Next()) {
            string encode_key = it->key().ToString();
            string key, member;
            uint64_t cur_encode_score;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_ZSET_SORT, key, cur_encode_score, member) != kDecodeOK) {
                break;
            }
            
            if ((key != cmd_vec[1]) || !value_lte_max(cur_encode_score, range)) {
                break;
            }
            
            if (range.minex && (cur_encode_score == range.encode_min)) {
                continue;
            }
            
            range_cnt++;
        }
        
        delete it;
        conn->SendInteger(range_cnt);
    }
}

static void generic_zrange_command(ClientConn* conn, const vector<string>& cmd_vec, bool reverse)
{
    long start, stop;
    if (get_long_from_string(cmd_vec[2], start) == CODE_ERROR || get_long_from_string(cmd_vec[3], stop) == CODE_ERROR) {
        conn->SendError("value is not an integer or out of range");
        return;
    }
    
    bool withscores = false;
    int cmd_size = (int)cmd_vec.size();
    if (cmd_size == 5 && !strcasecmp(cmd_vec[4].c_str(), "withscores")) {
        withscores = true;
    } else if (cmd_size >= 5) {
        conn->SendError("syntax error");
        return;
    }
    
    vector<string> range_vec;
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendError("db error");
    } else if (ret == kExpireKeyNotExist) {
        conn->SendArray(range_vec);
    } else {
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        // Sanitize indexes.
		long zcount = (long)mdata.count;
        if (start < 0) start = zcount + start;
        if (stop < 0) stop = zcount + stop;
        if (start < 0) start = 0;
        
        // Invariant: start >= 0, so this test will be true when end < 0.
        // The range is empty when start > end or start >= length.
        if (start > stop || start >= zcount) {
            conn->SendArray(range_vec);
            return;
        }
        
        if (stop >= zcount) stop = zcount - 1;
        
        if (reverse) {
            long original_start = start;
            start = zcount - stop - 1;
            stop = zcount - original_start - 1;
        }
        
        EncodeKey prefix_key(KEY_TYPE_ZSET_SORT, cmd_vec[1]);
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
        
        list<string> range_list;
        long rank = 0;
        for (it->Seek(prefix_key.GetEncodeKey()); it->Valid(); it->Next()) {
            string encode_key = it->key().ToString();
            string encode_value = it->value().ToString();
            string key, member;
            uint64_t encode_score;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_ZSET_SORT, key, encode_score, member) != kDecodeOK) {
                conn->SendError("invalid zset key member");
                delete it;
                return;
            }
            
            if (DecodeValue::Decode(encode_value, KEY_TYPE_ZSET_SORT) != kDecodeOK) {
                conn->SendError("invalid zset key value");
                delete it;
                return;
            }
            
            if ((rank >= start) && (rank <= stop)) {
                double score = uint64_to_double(encode_score);
                if (!reverse) {
                    range_vec.push_back(member);
                    if (withscores) {
                        range_vec.push_back(double_to_string(score));
                    }
                } else {
                    if (withscores) {
                        range_list.push_front(double_to_string(score));
                    }
                    
                    range_list.push_front(member);
                }
            }
            
            rank++;
            if (rank > stop) {
                break;
            }
        }
        
        delete it;
        
        if (reverse) {
            for (const string& member: range_list) {
                range_vec.push_back(member);
            }
        }
        
        conn->SendArray(range_vec);
    }
}

void zrange_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_zrange_command(conn, cmd_vec, false);
}

void zrevrange_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_zrange_command(conn, cmd_vec, true);
}


// zrangebyscore key min max [withscores] [limit offset count]
// zrevrangebyscore key max min [withscores] [limit offset count]
static void generic_zrangebyscore_command(ClientConn* conn, const vector<string>& cmd_vec, bool reverse)
{
    long offset = 0, count = LONG_MAX;
    int withscores = 0;
    int minidx, maxidx;
    
    // Parse the range arguments.
    if (reverse) {
        // Range is given as [max,min]
        maxidx = 2;
        minidx = 3;
    } else {
        // Range is given as [min,max]
        minidx = 2;
        maxidx = 3;
    }
    
    zrangespec range;
    if (!parse_range(cmd_vec[minidx], cmd_vec[maxidx], range)) {
        conn->SendError("min or max is not a valid float");
        return;
    }
    
    // Parse optional extra arguments. Note that ZCOUNT will exactly have
    // 4 arguments, so we'll never enter the following code path.
    int cmd_size = (int)cmd_vec.size();
    if (cmd_size > 4) {
        int remaining = cmd_size - 4;
        int pos = 4;
        
        while (remaining) {
            if (remaining >= 1 && !strcasecmp(cmd_vec[pos].c_str(), "withscores")) {
                pos++;
                remaining--;
                withscores = 1;
            } else if (remaining >= 3 && !strcasecmp(cmd_vec[pos].c_str(), "limit")) {
                if ((get_long_from_string(cmd_vec[pos + 1], offset) == CODE_ERROR) ||
                    (get_long_from_string(cmd_vec[pos + 2], count) == CODE_ERROR) ) {
                    conn->SendError("not valid integer for offset or count");
                    return;
                }
                pos += 3;
                remaining -= 3;
            } else {
                conn->SendError("syntax error");
                return;
            }
        }
    }
    
    vector<string> zset_vec;
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendRawResponse(kNullBulkString);
    } else if (ret == kExpireKeyNotExist) {
        conn->SendArray(zset_vec);
    } else {
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
        if (reverse) {
            string max_key;
            max_key.append(128, 0xFF);
            EncodeKey prefix_key(KEY_TYPE_ZSET_SORT, cmd_vec[1], range.encode_max, max_key);
            it->SeekForPrev(prefix_key.GetEncodeKey());
        } else {
            EncodeKey prefix_key(KEY_TYPE_ZSET_SORT, cmd_vec[1], range.encode_min, "");
            it->Seek(prefix_key.GetEncodeKey());
        }
        
        while (it->Valid()) {
            string encode_key = it->key().ToString();
            string key, member;
            uint64_t encode_score;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_ZSET_SORT, key, encode_score, member) != kDecodeOK) {
                break; // pass through the zset keys
            }
            
            if ((key != cmd_vec[1]) || (reverse && !value_gte_min(encode_score, range)) ||
                (!reverse && !value_lte_max(encode_score, range)) ) {
                break;
            }
            
            if (reverse) {
                if (range.maxex && (encode_score == range.encode_max)) {
                    it->Prev();
                    continue;
                }
            } else {
                if (range.minex && (encode_score == range.encode_min)) {
                    it->Next();
                    continue;
                }
            }
                
            if (offset > 0) {
                offset--;
            } else {
                if (count == 0) {
                    break;
                }
                
                count--;
                zset_vec.push_back(member);
                if (withscores) {
                    double score = uint64_to_double(encode_score);
                    zset_vec.push_back(double_to_string(score));
                }
            }
            
            if (reverse) {
                it->Prev();
            } else {
                it->Next();
            }
        }
        
        delete it;
        conn->SendArray(zset_vec);
    }
}

void zrangebyscore_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_zrangebyscore_command(conn, cmd_vec, false);
}

void zrevrangebyscore_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_zrangebyscore_command(conn, cmd_vec, true);
}

static int get_first_zset_score(rocksdb::ColumnFamilyHandle* cf_handle, rocksdb::Iterator* it,
                                const string& key, uint64_t& encode_score)
{
    EncodeKey prefix_key(KEY_TYPE_ZSET_SORT, key);
    it->Seek(prefix_key.GetEncodeKey());
    if (!it->Valid()) {
        return CODE_ERROR;
    }
    
    string encode_key = it->key().ToString();
    string zkey;
    string member;
    if (DecodeKey::Decode(encode_key, KEY_TYPE_ZSET_SORT, zkey, encode_score, member) != kDecodeOK) {
        return CODE_ERROR;
    }
    
    return CODE_OK;
}

void zlexcount_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    string min_key, max_key;
    bool min_exclusive, max_exclusive;
    
    if (!parse_lex_range(cmd_vec[2], min_key, min_exclusive) || !parse_lex_range(cmd_vec[3], max_key, max_exclusive)) {
        conn->SendError("min or max is not valid");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendRawResponse(kNullBulkString);
    } else if (ret == kExpireKeyNotExist) {
        conn->SendInteger(0);
    } else {
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
        uint64_t encode_score;
        if (get_first_zset_score(cf_handle, it, cmd_vec[1], encode_score) == CODE_ERROR) {
            conn->SendError("db error");
            delete it;
            return;
        }
        
        // seek the min key
        EncodeKey prefix_key(KEY_TYPE_ZSET_SORT, cmd_vec[1], encode_score, min_key);
        const rocksdb::Comparator* comparator = cf_handle->GetComparator();
        int count = 0;
        for (it->Seek(prefix_key.GetEncodeKey()); it->Valid(); it->Next()) {
            string encode_key = it->key().ToString();
            string key, member;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_ZSET_SORT, key, encode_score, member) != kDecodeOK) {
                break; // pass through the zset keys
            }
            
            if ((key != cmd_vec[1]) || !lex_value_lte_max(member, max_key, max_exclusive, comparator)) {
                break;
            }
            
            if (min_exclusive && comparator->Equal(member, min_key)) {
                continue;
            }
            
            count++;
        }
        
        delete it;
        conn->SendInteger(count);
    }
}

// ZRANGEBYLEX key min max [LIMIT offset count]
// ZREVRANGEBYLEX key max min [LIMIT offset count]
void generic_zrangebylex_command(ClientConn* conn, const vector<string>& cmd_vec, bool reverse)
{
    long offset = 0, count = LONG_MAX;
    int minidx, maxidx;
    string min_key, max_key;
    bool min_exclusive, max_exclusive;
    
    // Parse the range arguments.
    if (reverse) {
        // Range is given as [max,min]
        maxidx = 2;
        minidx = 3;
    } else {
        // Range is given as [min,max]
        minidx = 2;
        maxidx = 3;
    }
    
    if (!parse_lex_range(cmd_vec[minidx], min_key, min_exclusive) || !parse_lex_range(cmd_vec[maxidx], max_key, max_exclusive)) {
        conn->SendError("min or max not valid string range item");
        return;
    }
    
    // Parse optional extra arguments. Note that ZCOUNT will exactly have
    // 4 arguments, so we'll never enter the following code path.
    int cmd_size = (int)cmd_vec.size();
    if (cmd_size > 4) {
        int remaining = cmd_size - 4;
        int pos = 4;
        
        while (remaining) {
            if (remaining >= 3 && !strcasecmp(cmd_vec[pos].c_str(), "limit")) {
                if ((get_long_from_string(cmd_vec[pos + 1], offset) == CODE_ERROR) ||
                    (get_long_from_string(cmd_vec[pos + 2], count) == CODE_ERROR)) {
                    conn->SendError("not valid integer for offset or count");
                    return;
                }
                pos += 3;
                remaining -= 3;
            } else {
                conn->SendError("syntax error");
                return;
            }
        }
    }
    
    vector<string> zset_vec;
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendRawResponse(kNullBulkString);
    } else if (ret == kExpireKeyNotExist) {
        conn->SendArray(zset_vec);
    } else {
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
        uint64_t encode_score;
        if (get_first_zset_score(cf_handle, it, cmd_vec[1], encode_score) == CODE_ERROR) {
            conn->SendError("db error");
            delete it;
            return;
        }
        
        if (reverse) {
            EncodeKey prefix_key(KEY_TYPE_ZSET_SORT, cmd_vec[1], encode_score, max_key);
            it->SeekForPrev(prefix_key.GetEncodeKey());
        } else {
            EncodeKey prefix_key(KEY_TYPE_ZSET_SORT, cmd_vec[1], encode_score, min_key);
            it->Seek(prefix_key.GetEncodeKey());
        }
        
        const rocksdb::Comparator* comparator = cf_handle->GetComparator();
        while (it->Valid()) {
            string encode_key = it->key().ToString();
            string key, member;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_ZSET_SORT, key, encode_score, member) != kDecodeOK) {
                break; // pass through the zset keys
            }
            
            if ((key != cmd_vec[1]) ||
                (reverse && !lex_value_gte_min(member, min_key, min_exclusive, comparator)) ||
                (!reverse && !lex_value_lte_max(member, max_key, max_exclusive, comparator)) ) {
                break;
            }
            
            if (reverse) {
                if (max_exclusive && comparator->Equal(member, max_key)) {
                    it->Prev();
                    continue;
                }
            } else {
                if (min_exclusive && comparator->Equal(member, min_key)) {
                    it->Next();
                    continue;
                }
            }
             
            if (offset > 0) {
                offset--;
            } else {
                if (count == 0) {
                    break;
                }
                
                count--;
                zset_vec.push_back(member);
            }
            
            if (reverse) {
                it->Prev();
            } else {
                it->Next();
            }
        }
        
        delete it;
        conn->SendArray(zset_vec);
    }
}

void zrangebylex_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    return generic_zrangebylex_command(conn, cmd_vec, false);
}

void zrevrangebylex_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    return generic_zrangebylex_command(conn, cmd_vec, true);
}

static void generic_zrank_command(ClientConn* conn, const vector<string>& cmd_vec, bool reverse)
{
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendRawResponse(kNullBulkString);
    } else if (ret == kExpireKeyNotExist) {
        conn->SendRawResponse(kNullBulkString);
    } else {
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        double score;
        int result = get_zset_score(db_idx, cmd_vec[1], cmd_vec[2], score);
        if (result == FIELD_NOT_EXIST) {
            conn->SendRawResponse(kNullBulkString);
            return;
        } else if (result == DB_ERROR) {
            conn->SendError("db error");
            return;
        }
        
        EncodeKey prefix_key(KEY_TYPE_ZSET_SORT, cmd_vec[1]);
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
        uint64_t seek_cnt = 0;
        for (it->Seek(prefix_key.GetEncodeKey()); it->Valid() && seek_cnt < mdata.count; it->Next(), seek_cnt++) {
            string encode_key = it->key().ToString();
            string encode_value = it->value().ToString();
            string key, member;
            uint64_t encode_score;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_ZSET_SORT, key, encode_score, member) != kDecodeOK) {
                conn->SendError("invalid set key member");
                delete it;
                return;
            }
            
            if (DecodeValue::Decode(encode_value, KEY_TYPE_ZSET_SORT) != kDecodeOK) {
                conn->SendError("invalid set key value");
                delete it;
                return;
            }
            
            if (member == cmd_vec[2]) {
                break;
            }
        }
        
        delete it;
        
        long rank = reverse ? (mdata.count - seek_cnt - 1) : seek_cnt;
        conn->SendInteger(rank);
    }
}

void zrank_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_zrank_command(conn, cmd_vec, false);
}

void zrevrank_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    generic_zrank_command(conn, cmd_vec, true);
}

void zrem_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        int del_cnt = 0;
        int cmd_size = (int)cmd_vec.size();
        for (int i = 2; i < cmd_size; i++) {
            double score;
            int result = get_zset_score(db_idx, cmd_vec[1], cmd_vec[i], score);
            if (result == FIELD_EXIST) {
                del_zset_score(db_idx, cmd_vec[1], cmd_vec[i], &batch);
                del_zset_sort(db_idx, cmd_vec[1], score, cmd_vec[i], &batch);
                del_cnt++;
            } else if (result == DB_ERROR) {
                conn->SendError("db error");
                return;
            }
        }
        
        if (del_cnt > 0) {
            mdata.count -= del_cnt;
            if (mdata.count == 0) {
                delete_key(db_idx, cmd_vec[1], mdata.ttl, KEY_TYPE_ZSET);
            } else {
                put_meta_data(db_idx, KEY_TYPE_ZSET, cmd_vec[1], mdata.ttl, mdata.count, &batch);
                DB_BATCH_UPDATE(batch)
            }
            
            g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        }
        
        conn->SendInteger(del_cnt);
    }
}

void zremrangebylex_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    string min_key, max_key;
    bool min_exclusive, max_exclusive;
    
    if (!parse_lex_range(cmd_vec[2], min_key, min_exclusive) || !parse_lex_range(cmd_vec[3], max_key, max_exclusive)) {
        conn->SendError("min or max is not valid");
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    MetaData mdata;
    rocksdb::WriteBatch batch;
    KeyLockGuard lock_guard(db_idx, cmd_vec[1]);
    int ret = expire_key_if_needed(db_idx, cmd_vec[1], mdata);
    if (ret == kExpireDBError) {
        conn->SendRawResponse(kNullBulkString);
    } else if (ret == kExpireKeyNotExist) {
        conn->SendInteger(0);
    } else {
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        // get the score by seek to the first element of this key
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
        uint64_t encode_score;
        if (get_first_zset_score(cf_handle, it, cmd_vec[1], encode_score) == CODE_ERROR) {
            conn->SendError("db error");
            delete it;
            return;
        }
        
        // seek the min key
        EncodeKey prefix_key(KEY_TYPE_ZSET_SORT, cmd_vec[1], encode_score, min_key);
        const rocksdb::Comparator* comparator = cf_handle->GetComparator();
        int count = 0;
        for (it->Seek(prefix_key.GetEncodeKey()); it->Valid(); it->Next()) {
            string encode_key = it->key().ToString();
            string key, member;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_ZSET_SORT, key, encode_score, member) != kDecodeOK) {
                break; // pass through the zset keys
            }
            
            if ((key != cmd_vec[1]) || !lex_value_lte_max(member, max_key, max_exclusive, comparator)) {
                break;
            }
            
            if (min_exclusive && comparator->Equal(member, min_key)) {
                continue;
            }
            
            double score = uint64_to_double(encode_score);
            del_zset_score(db_idx, key, member, &batch);
            del_zset_sort(db_idx, key, score, member, &batch);
            count++;
        }
        
        delete it;
        conn->SendInteger(count);
        
        if (count > 0) {
            mdata.count -= count;
            if (mdata.count == 0) {
                delete_key(db_idx, cmd_vec[1], mdata.ttl, KEY_TYPE_ZSET);
            } else {
                put_meta_data(db_idx, KEY_TYPE_ZSET, cmd_vec[1], mdata.ttl, mdata.count, &batch);
                DB_BATCH_UPDATE(batch)
            }
            
            g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        }
    }
}

void zremrangebyrank_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long start, stop;
    if (get_long_from_string(cmd_vec[2], start) == CODE_ERROR || get_long_from_string(cmd_vec[3], stop) == CODE_ERROR) {
        conn->SendError("not invalid number");
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
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        long member_count = (long)mdata.count;
        // Sanitize indexes.
        if (start < 0) start = member_count + start;
        if (stop < 0) stop = member_count + stop;
        if (start < 0) start = 0;
        
        // Invariant: start >= 0, so this test will be true when end < 0.
        // The range is empty when start > end or start >= length.
        if (start > stop || start >= member_count) {
            conn->SendInteger(0);
            return;
        }
        
        if (stop >= member_count) stop = member_count - 1;
        
        EncodeKey prefix_key(KEY_TYPE_ZSET_SORT, cmd_vec[1]);
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
        
        list<string> range_list;
        long rank = 0;
        for (it->Seek(prefix_key.GetEncodeKey()); it->Valid() && rank <= stop; it->Next(), rank++) {
            string encode_key = it->key().ToString();
            string encode_value = it->value().ToString();
            string key, member;
            uint64_t encode_score;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_ZSET_SORT, key, encode_score, member) != kDecodeOK) {
                conn->SendError("invalid set key member");
                delete it;
                return;
            }
            
            if (DecodeValue::Decode(encode_value, KEY_TYPE_ZSET_SORT) != kDecodeOK) {
                conn->SendError("invalid set key value");
                delete it;
                return;
            }
            
            if (rank >= start) {
                double score = uint64_to_double(encode_score);
                del_zset_score(db_idx, cmd_vec[1], member, &batch);
                del_zset_sort(db_idx, cmd_vec[1], score, member, &batch);
            }
        }
        
        delete it;
        uint64_t del_cnt = stop - start + 1;
        if (del_cnt > 0) {
            mdata.count -= del_cnt;
            if (mdata.count == 0) {
                delete_key(db_idx, cmd_vec[1], mdata.ttl, KEY_TYPE_ZSET);
            } else {
                put_meta_data(db_idx, KEY_TYPE_ZSET, cmd_vec[1], mdata.ttl, mdata.count, &batch);
                DB_BATCH_UPDATE(batch)
            }
            
            g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        }
        conn->SendInteger(stop - start + 1);
    }
}

void zremrangebyscore_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    zrangespec range;
    if (!parse_range(cmd_vec[2], cmd_vec[3], range)) {
        conn->SendError("min/max is not a valid float");
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
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        EncodeKey prefix_key(KEY_TYPE_ZSET_SORT, cmd_vec[1], range.encode_min, "");
        rocksdb::ColumnFamilyHandle* cf_handle = g_server.cf_handles_map[db_idx];
        rocksdb::Iterator* it = g_server.db->NewIterator(g_server.read_option, cf_handle);
        
        uint64_t del_cnt = 0;
        for (it->Seek(prefix_key.GetEncodeKey()); it->Valid(); it->Next()) {
            string encode_key = it->key().ToString();
            string key, member;
            uint64_t encode_score;
            
            if (DecodeKey::Decode(encode_key, KEY_TYPE_ZSET_SORT, key, encode_score, member) != kDecodeOK) {
                return;
            }
            
            if ((key != cmd_vec[1]) || !value_lte_max(encode_score, range)) {
                break;
            }
            
            if (range.minex && (encode_score == range.encode_min)) {
                continue;
            }
            
            double score = uint64_to_double(encode_score);
            del_zset_score(db_idx, cmd_vec[1], member, &batch);
            del_zset_sort(db_idx, cmd_vec[1], score, member, &batch);
            del_cnt++;
        }
        
        delete it;
        if (del_cnt > 0) {
            mdata.count -= del_cnt;
            if (mdata.count == 0) {
                delete_key(db_idx, cmd_vec[1], mdata.ttl, KEY_TYPE_ZSET);
            } else {
                put_meta_data(db_idx, KEY_TYPE_ZSET, cmd_vec[1], mdata.ttl, mdata.count, &batch);
                DB_BATCH_UPDATE(batch)
            }
            
            g_server.binlog.Store(db_idx, conn->GetCurReqCommand());
        }
        conn->SendInteger(del_cnt);
    }
}

void zscore_command(ClientConn* conn, const vector<string>& cmd_vec)
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
        if (mdata.type != KEY_TYPE_ZSET) {
            conn->SendRawResponse(kWrongTypeError);
            return;
        }
        
        double score;
        int result = get_zset_score(db_idx, cmd_vec[1], cmd_vec[2], score);
        if (result == FIELD_EXIST) {
            conn->SendBulkString(double_to_string(score));
        } else if (result == FIELD_NOT_EXIST) {
            conn->SendRawResponse(kNullBulkString);
        } else {
            conn->SendError("db error");
        }
    }
}

void zscan_command(ClientConn* conn, const vector<string>& cmd_vec)
{
    long count = 10;
    string pattern;
    if (parse_scan_param(conn, cmd_vec, 3, pattern, count) == CODE_ERROR) {
        return;
    }
    
    int db_idx = conn->GetDBIndex();
    EncodeKey prefix_key(KEY_TYPE_ZSET_SCORE, cmd_vec[1]);
    EncodeKey cursor_key(KEY_TYPE_ZSET_SCORE, cmd_vec[1], cmd_vec[2]);
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
        uint64_t encode_score;
        
        if (DecodeKey::Decode(encode_key, KEY_TYPE_ZSET_SCORE, key, member) != kDecodeOK) {
            conn->SendError("invalid zset score key");
            delete it;
            return;
        }
        
        if (DecodeValue::Decode(encode_value, KEY_TYPE_ZSET_SCORE, encode_score) != kDecodeOK) {
            conn->SendError("invalid zset score value");
            delete it;
            return;
        }
        
        if ((long)members.size() >= count * 2) {
            cursor = member;
            break;
        }
        
        double score = uint64_to_double(encode_score);
        if (pattern.empty()) {
            members.push_back(member);
            members.push_back(double_to_string(score));
        } else {
            if (stringmatchlen(pattern.c_str(), (int)pattern.size(), member.c_str(), (int)member.size(), 0)) {
                members.push_back(member);
                members.push_back(double_to_string(score));
            }
        }
    }
    
    delete it;
    
    string start_resp = "*2\r\n";
    conn->SendRawResponse(start_resp);
    conn->SendBulkString(cursor);
    conn->SendArray(members);
}
