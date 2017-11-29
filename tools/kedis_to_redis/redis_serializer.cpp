//
//  redis_serializer.cpp
//  kedis
//
//  Created by ziteng on 17/11/28.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "redis_serializer.h"
#include "byte_stream.h"
#include "simple_log.h"
#include "crc64.h"
#include <math.h>

const uint8_t KEY_TYPE_META         = 1;
const uint8_t KEY_TYPE_STRING       = 2;
const uint8_t KEY_TYPE_HASH         = 3;
const uint8_t KEY_TYPE_HASH_FIELD   = 4;
const uint8_t KEY_TYPE_LIST         = 5;
const uint8_t KEY_TYPE_LIST_ELEMENT = 6;
const uint8_t KEY_TYPE_SET          = 7;
const uint8_t KEY_TYPE_SET_MEMBER   = 8;
const uint8_t KEY_TYPE_ZSET         = 9;
const uint8_t KEY_TYPE_ZSET_SCORE   = 10;

#define REDIS_RDB_6BITLEN 0
#define REDIS_RDB_14BITLEN 1
#define REDIS_RDB_32BITLEN 2

#define REDIS_RDB_TYPE_STRING 0
#define REDIS_RDB_TYPE_LIST   1
#define REDIS_RDB_TYPE_SET    2
#define REDIS_RDB_TYPE_ZSET   3
#define REDIS_RDB_TYPE_HASH   4

#define REDIS_RDB_VERSION 6

string encode_len(uint32_t len) {
    unsigned char buf[5];
    size_t nwritten;
    
    if (len < (1<<6)) {
        // encode a 6 bit len
        buf[0] = (len & 0xFF) | (REDIS_RDB_6BITLEN << 6);
        nwritten = 1;
    } else if (len < (1<<14)) {
        // encode a 14 bit len
        buf[0] = ((len >> 8) & 0xFF) | (REDIS_RDB_14BITLEN << 6);
        buf[1] = len & 0xFF;
        nwritten = 2;
    } else {
        // encode a 32 bit len
        buf[0] = (REDIS_RDB_32BITLEN << 6);
        len = htonl(len);
        memcpy(buf + 1, &len, 4);
        nwritten = 5;
    }
    
    return string((char*)buf, nwritten);
}

void create_dump_payload(string& payload)
{
    /* Write the footer, this is how it looks like:
     * ----------------+---------------------+---------------+
     * ... RDB payload | 2 bytes RDB version | 8 bytes CRC64 |
     * ----------------+---------------------+---------------+
     * RDB version and CRC are both in little endian.
     */
    
    uchar_t buf[8];
    
    buf[0] = REDIS_RDB_VERSION & 0xFF;
    buf[1] = REDIS_RDB_VERSION >> 8;
    payload.append((char*)buf, 2);
    
    uint64_t crc = crc64(0, (const unsigned char*)payload.data(), (uint64_t)payload.size());
    for (int i = 0; i < 8; i++) {
        unsigned digit = (crc >> (i * 8)) & 0xFF;
        buf[i] = digit;
    }
    
    payload.append((char*)buf, 8);
}

void append_string(string& serialized_value, const string& str)
{
    string len_str = encode_len((uint32_t)str.size());
    serialized_value.append(len_str);
    serialized_value.append(str);
}

void serialize_string(const string& value, string& serialized_value)
{
    serialized_value.append(1, (char)REDIS_RDB_TYPE_STRING);
    append_string(serialized_value, value);
    
    create_dump_payload(serialized_value);
}

void serialize_hash(const map<string, string>& fv_map, string& serialized_value)
{
    serialized_value.append(1, (char)REDIS_RDB_TYPE_HASH);
    uint32_t fv_cnt = (uint32_t)fv_map.size();
    string cnt_str = encode_len(fv_cnt);
    serialized_value.append(cnt_str);
    for (auto it = fv_map.begin(); it != fv_map.end(); it++) {
        append_string(serialized_value, it->first);
        append_string(serialized_value, it->second);
    }
    
    create_dump_payload(serialized_value);
}

void serialize_list(const list<string>& element_list, string& serialized_value)
{
    serialized_value.append(1, (char)REDIS_RDB_TYPE_LIST);
    uint32_t list_cnt = (uint32_t)element_list.size();
    string cnt_str = encode_len(list_cnt);
    serialized_value.append(cnt_str);
    for (auto it = element_list.begin(); it != element_list.end(); it++) {
        append_string(serialized_value, *it);
    }
    
    create_dump_payload(serialized_value);
}

void serialize_set(const set<string>& member_set, string& serialized_value)
{
    serialized_value.append(1, (char)REDIS_RDB_TYPE_SET);
    uint32_t set_cnt = (uint32_t)member_set.size();
    string cnt_str = encode_len(set_cnt);
    serialized_value.append(cnt_str);
    for (auto it = member_set.begin(); it != member_set.end(); it++) {
        append_string(serialized_value, *it);
    }
    
    create_dump_payload(serialized_value);
}

/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifying the length of the representation.
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 * 253: not a number
 * 254: + inf
 * 255: - inf
 */
string rdb_save_double_value(double val) {
    unsigned char buf[128];
    int len;
    
    if (isnan(val)) {
        buf[0] = 253;
        return string((char*)buf, 1);
    } else if (!isfinite(val)) {
        buf[0] = (val < 0) ? 255 : 254;
        return string((char*)buf, 1);
    } else {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
        /* Check if the float is in a safe range to be casted into a
         * long long. We are assuming that long long is 64 bit here.
         * Also we are assuming that there are no implementations around where
         * double has precision < 52 bit.
         *
         * Under this assumptions we test if a double is inside an interval
         * where casting to long long is safe. Then using two castings we
         * make sure the decimal part is zero. If all this is true we use
         * integer printing function that is much faster. */
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if (val > min && val < max && val == ((double)((long long)val))) {
            string value = to_string((long long)val);
            strcpy(buf + 1, value.c_str());
        } else
#endif
            snprintf((char*)buf+1,sizeof(buf)-1,"%.17g",val);
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
        
        return string((char*)buf, len);
    }
}

void serialize_zset(const map<string, double>& score_map, string& serialized_value)
{
    serialized_value.append(1, (char)REDIS_RDB_TYPE_ZSET);
    uint32_t member_cnt = (uint32_t)score_map.size();
    string cnt_str = encode_len(member_cnt);
    serialized_value.append(cnt_str);
    for (auto it = score_map.begin(); it != score_map.end(); it++) {
        append_string(serialized_value, it->first);
        string double_str = rdb_save_double_value(it->second);
        serialized_value.append(double_str);
    }
    
    create_dump_payload(serialized_value);
}

int convert_kedis_serialized_value(const string& kedis_value, string& redis_value)
{
    ByteStream bs((uchar_t*)kedis_value.data(), (uint32_t)kedis_value.size());
    
    try {
        uint32_t count = bs.ReadVarUInt();
        string meta_key, meta_value;
        bs >> meta_key;
        bs >> meta_value;
        
        ByteStream meta_bs((uchar_t*)meta_value.data(), (uint32_t)meta_value.size());
        uint8_t key_type;
        uint64_t ttl;
        meta_bs >> key_type;
        meta_bs >> ttl;
        
        if (meta_key[0] != KEY_TYPE_META) {
            return CODE_ERROR;
        }
        
        if (key_type == KEY_TYPE_STRING) {
            string value;
            meta_bs >> value;
            serialize_string(value, redis_value);
        } else if (key_type == KEY_TYPE_HASH) {
            if (ttl) {
                // do not parse ttl kv pair
                count--;
            }
            
            map<string, string> fv_map;
            for (uint32_t i = 1; i < count; i++) {
                string field_key, field_value;
                bs >> field_key;
                bs >> field_value;
                
                ByteStream field_key_stream((uchar_t*)field_key.data(), (uint32_t)field_key.size());
                ByteStream field_value_stream((uchar_t*)field_value.data(), (uint32_t)field_value.size());
                uint8_t type;
                string key;
                string field;
                string value;
                
                field_key_stream >> type;
                field_key_stream >> key;
                field_key_stream >> field;
                
                field_value_stream >> type;
                field_value_stream >> value;
                
                if (type != KEY_TYPE_HASH_FIELD) {
                    return CODE_ERROR;
                }
                fv_map[field] = value;
            }
            
            serialize_hash(fv_map, redis_value);
        } else if (key_type == KEY_TYPE_LIST) {
            if (ttl) {
                // do not parse ttl kv pair
                count--;
            }
            
            list<string> value_list;
            for (uint32_t i = 1; i < count; i++) {
                string list_key, list_value;
                bs >> list_key;
                bs >> list_value;
                
                ByteStream list_value_stream((uchar_t*)list_value.data(), (uint32_t)list_value.size());
                uint8_t type;
                uint64_t prev_seq, next_seq;
                string value;
                list_value_stream >> type;
                list_value_stream >> prev_seq;
                list_value_stream >> next_seq;
                list_value_stream >> value;
                
                if (type != KEY_TYPE_LIST_ELEMENT) {
                    return CODE_ERROR;
                }
                value_list.push_back(value);
            }
            
            serialize_list(value_list, redis_value);
        } else if (key_type == KEY_TYPE_SET) {
            if (ttl) {
                // do not parse ttl kv pair
                count--;
            }
            
            set<string> member_set;
            for (uint32_t i = 1; i < count; i++) {
                string member_key, member_value;
                bs >> member_key;
                bs >> member_value;
                
                ByteStream member_key_stream((uchar_t*)member_key.data(), (uint32_t)member_key.size());
                uint8_t type;
                string key;
                string member;
                
                member_key_stream >> type;
                member_key_stream >> key;
                member_key_stream >> member;
                
                if (type != KEY_TYPE_SET_MEMBER) {
                    return CODE_ERROR;
                }
                member_set.insert(member);
            }
            
            serialize_set(member_set, redis_value);
        } else if (key_type == KEY_TYPE_ZSET) {
            count--;
            if (ttl) {
                // do not parse ttl kv pair
                count--;
            }
            count /= 2; // do not parse KEY_TYPE_ZSET_SORT
            
            map<string, double> score_map;
            for (uint32_t i = 0; i < count; i++) {
                string zset_key, zset_value;
                bs >> zset_key;
                bs >> zset_value;
                
                ByteStream zset_key_stream((uchar_t*)zset_key.data(), (uint32_t)zset_key.size());
                ByteStream zset_value_stream((uchar_t*)zset_value.data(), (uint32_t)zset_value.size());
                uint8_t type;
                string key, member;
                uint64_t encode_score;
                
                zset_key_stream >> type;
                zset_key_stream >> key;
                zset_key_stream >> member;
                
                zset_value_stream >> type;
                zset_value_stream >> encode_score;
                double score = uint64_to_double(encode_score);
                
                if (type != KEY_TYPE_ZSET_SCORE) {
                    return CODE_ERROR;
                }
                score_map[member] = score;
            }
            
            serialize_zset(score_map, redis_value);
        } else {
            log_message(kLogLevelError, "no such key_type=%d\n", key_type);
            return CODE_ERROR;
        }
    
        return CODE_OK;
    } catch (ParseException& ex) {
        log_message(kLogLevelError, "deserialize failed: %s\n", ex.GetErrorMsg());
        return CODE_ERROR;
    }
    return CODE_OK;
}
