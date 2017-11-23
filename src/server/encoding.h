//
//  encoding.h
//  kedis
//
//  Created by ziteng on 17/7/24.
//  Copyright © 2017年 mgj. All rights reserved.
//

#ifndef __ENCODING_H__
#define __ENCODING_H__

#include "ostype.h"
#include "util.h"
#include "byte_stream.h"
#include "rocksdb/slice.h"

/*
 used to encode and decode simple kv api to Redis data structure: string, hash, list, set, sorted set
 
 string:
    KEY_TYPE_META <key> ,    KEY_TYPE_STRING TTL <value>
 hash:
    KEY_TYPE_META <key> ,    KEY_TYPE_HASH TTL hash_count
    KEY_TYPE_HASH_FIELD <key> <field>,    KEY_TYPE_HASH_FIELD <value>
 list:
    KEY_TYPE_META <key> ,    KEY_TYPE_LIST TTL list_count head_seq tail_seq current_seq
    KEY_TYPE_LIST_MEMBER <key> seq ,     KEY_TYPE_LIST_MEMBER prev_seq next_seq <value>
 set:
    KEY_TYPE_META <key> ,    KEY_TYPE_SET TTL set_count
    KEY_TYPE_SET_MEMBER <key> <member> ,   KEY_TYPE_SET_MEMBER
 zset:
    KEY_TYPE_META <key> ,    KEY_TYPE_ZSET TTL zset_count
    KEY_TYPE_ZSET_SCORE <key> <member> ,     KEY_TYPE_ZSET_SCORE encode-score
    KEY_TYPE_ZSET_SORT <key> <encode-score> <member> ,    KEY_TYPE_ZSET_SORT
 
    for encode-score details, see doc/zset-double-score-design.md
 
 # ttl key is used for a background thread to expire keys
 ttl:
    KEY_TYPE_TTL_SORT TTL <key>,     key_type
 */


const uint8_t KEY_TYPE_UNKNOWN      = 0;
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
const uint8_t KEY_TYPE_ZSET_SORT    = 11;
const uint8_t KEY_TYPE_TTL_SORT     = 12;
const uint8_t KEY_TYPE_STATS        = 13;

string get_type_name(uint8_t key_type);

class EncodeKey {
public:
    EncodeKey(uint8_t type, const string& key);
    EncodeKey(uint8_t type, const string& key, const string& member);
    EncodeKey(uint8_t type, const string& key, uint64_t seq);
    EncodeKey(uint8_t type, const string& key, uint64_t score, const string& member);
    EncodeKey(uint8_t type, uint64_t ttl, const string& key);
    ~EncodeKey() {}
    
    rocksdb::Slice GetEncodeKey() const { return rocksdb::Slice((char*)buffer_.GetBuffer(), buffer_.GetWriteOffset()); }
private:
    SimpleBuffer    buffer_;
    ByteStream      stream_;
};

class EncodeValue {
public:
    EncodeValue(uint8_t type, uint64_t ttl, const string& value);
    EncodeValue(uint8_t type, uint64_t ttl, uint64_t count);
    EncodeValue(uint8_t type, const string& value);
    EncodeValue(uint8_t type, uint64_t ttl, uint64_t count, uint64_t head_seq, uint64_t tail_seq, uint64_t current_seq);
    EncodeValue(uint8_t type, uint64_t prev_seq, uint64_t next_seq, const string& value);
    EncodeValue(uint8_t type);
    EncodeValue(uint8_t type, uint64_t score);
    ~EncodeValue() {}
    
    rocksdb::Slice GetEncodeValue() const { return rocksdb::Slice((char*)buffer_.GetBuffer(), buffer_.GetWriteOffset()); }
private:
    SimpleBuffer    buffer_;
    ByteStream      stream_;
};


const int kDecodeOK = 0;
const int kDecodeErrorType = 1;
const int kDecodeErrorFormat = 2;

class DecodeKey {
public:
    static int Decode(const string& encode_key, uint8_t expect_type, string& key);
    static int Decode(const string& encode_key, uint8_t expect_type, string& key, string& member);
    static int Decode(const string& encode_key, uint8_t expect_type, string& key, uint64_t& seq);
    static int Decode(const string& encode_key, uint8_t expect_type, string& key, uint64_t& score, string& member);
    static int Decode(const string& encode_key, uint8_t expect_type, uint64_t& ttl, string& key);
};

class DecodeValue {
public:
    static int Decode(const string& encode_value, uint8_t expect_type, uint64_t& ttl, string& value);
    static int Decode(const string& encode_value, uint8_t expect_type, uint64_t& ttl, uint64_t& count);
    static int Decode(const string& encode_value, uint8_t expect_type, string& value);
    static int Decode(const string& encode_value, uint8_t expect_type, uint64_t& ttl, uint64_t& count,
                      uint64_t& head_seq, uint64_t& tail_seq, uint64_t& current_seq);
    static int Decode(const string& encode_value, uint8_t expect_type, uint64_t& prev_seq, uint64_t& next_seq, string& value);
    static int Decode(const string& encode_value, uint8_t expect_type);
    static int Decode(const string& encode_value, uint8_t expect_type, uint64_t& score);
};

#endif /* __ENCODING_H__ */
