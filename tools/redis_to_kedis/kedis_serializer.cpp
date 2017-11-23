//
//  kedis_serializer.cpp
//  kedis
//
//  Created by ziteng on 17/11/21.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "kedis_serializer.h"
#include "byte_stream.h"

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

class EncodeKey {
public:
    EncodeKey(uint8_t type, const string& key);
    EncodeKey(uint8_t type, const string& key, const string& member);
    EncodeKey(uint8_t type, const string& key, uint64_t seq);
    EncodeKey(uint8_t type, const string& key, uint64_t score, const string& member);
    EncodeKey(uint8_t type, uint64_t ttl, const string& key);
    ~EncodeKey() {}
    
    string GetEncodeKey() const { return buffer_; }
private:
    string      buffer_;
    ByteStream  stream_;
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
    
    string GetEncodeValue() const { return buffer_; }
private:
    string      buffer_;
    ByteStream  stream_;
};

EncodeKey::EncodeKey(uint8_t type, const string& key)
{
    stream_.SetString(&buffer_);
    stream_ << type;
    if (type == KEY_TYPE_META) {
        stream_.WriteStringWithoutLen(key);
    } else {
        stream_ << key;
    }
}

EncodeKey::EncodeKey(uint8_t type, const string& key, const string& member)
{
    stream_.SetString(&buffer_);
    stream_ << type;
    stream_ << key;
    stream_ << member;
}

EncodeKey::EncodeKey(uint8_t type, const string& key, uint64_t seq)
{
    stream_.SetString(&buffer_);
    stream_ << type;
    stream_ << key;
    stream_ << seq;
}

EncodeKey::EncodeKey(uint8_t type, const string& key, uint64_t score, const string& member)
{
    stream_.SetString(&buffer_);
    stream_ << type;
    stream_ << key;
    stream_ << score;
    stream_.WriteStringWithoutLen(member);  // member will be sorted by lexicographical ordering without a heading length
}

EncodeKey::EncodeKey(uint8_t type, uint64_t ttl, const string& key)
{
    stream_.SetString(&buffer_);
    stream_ << type;
    stream_ << ttl;
    stream_ << key;
}

//======
EncodeValue::EncodeValue(uint8_t type, uint64_t ttl, const string& value)
{
    stream_.SetString(&buffer_);
    stream_ << type;
    stream_ << ttl;
    stream_ << value;
}

EncodeValue::EncodeValue(uint8_t type, uint64_t ttl, uint64_t count)
{
    stream_.SetString(&buffer_);
    stream_ << type;
    stream_ << ttl;
    stream_ << count;
}

EncodeValue::EncodeValue(uint8_t type, const string& value)
{
    stream_.SetString(&buffer_);
    stream_ << type;
    stream_ << value;
}

EncodeValue::EncodeValue(uint8_t type, uint64_t ttl, uint64_t count, uint64_t head_seq, uint64_t tail_seq, uint64_t current_seq)
{
    stream_.SetString(&buffer_);
    stream_ << type;
    stream_ << ttl;
    stream_ << count;
    stream_ << head_seq;
    stream_ << tail_seq;
    stream_ << current_seq;
}

EncodeValue::EncodeValue(uint8_t type, uint64_t prev_seq, uint64_t next_seq, const string& value)
{
    stream_.SetString(&buffer_);
    stream_ << type;
    stream_ << prev_seq;
    stream_ << next_seq;
    stream_ << value;
}

EncodeValue::EncodeValue(uint8_t type)
{
    stream_.SetString(&buffer_);
    stream_ << type;
}

EncodeValue::EncodeValue(uint8_t type, uint64_t score)
{
    stream_.SetString(&buffer_);
    stream_ << type;
    stream_ << score;
}

void serialize_ttl(const string& key, uint64_t ttl, uint8_t type, ByteStream& bs)
{
    if (ttl) {
        EncodeKey ttl_key(KEY_TYPE_TTL_SORT, ttl, key);
        EncodeValue ttl_value(type);
        
        bs << ttl_key.GetEncodeKey();
        bs << ttl_value.GetEncodeValue();
    }
}

string serialize_string(uint64_t ttl, const string& key, const string& value)
{
    string serialize_value;
    ByteStream stream(&serialize_value);
    uint32_t count = ttl ? 2 : 1;
    
    stream.WriteVarUInt(count);
    
    // serialize key
    EncodeKey kedis_key(KEY_TYPE_META, key);
    EncodeValue kedis_value(KEY_TYPE_STRING, ttl, value);
    
    stream << kedis_key.GetEncodeKey();
    stream << kedis_value.GetEncodeValue();
    serialize_ttl(key, ttl, KEY_TYPE_STRING, stream);
    
    return serialize_value;
}

string serialize_list(uint64_t ttl, const string& key, const list<string>& value_list)
{
    string serialize_value;
    ByteStream stream(&serialize_value);
    uint32_t list_cnt = (uint32_t)value_list.size();
    uint32_t count = ttl ? 2 + list_cnt : 1 + list_cnt;
    
    stream.WriteVarUInt(count);
    
    // serialize meta key
    EncodeKey meta_key(KEY_TYPE_META, key);
    uint64_t head_seq = 1;
    uint64_t tail_seq = list_cnt;
    uint64_t incr_seq = list_cnt + 1;
    EncodeValue meta_value(KEY_TYPE_LIST, ttl, list_cnt, head_seq, tail_seq, incr_seq);
    stream << meta_key.GetEncodeKey();
    stream << meta_value.GetEncodeValue();
    
    // serialize list element
    uint64_t cur_seq = 1;
    for (auto it = value_list.begin(); it != value_list.end(); it++) {
        EncodeKey element_key(KEY_TYPE_LIST_ELEMENT, key, cur_seq);
        uint64_t prev_seq = cur_seq - 1;
        uint64_t next_seq = (cur_seq == list_cnt) ? 0 : cur_seq + 1;
        EncodeValue element_value(KEY_TYPE_LIST_ELEMENT, prev_seq, next_seq, *it);
        
        stream << element_key.GetEncodeKey();
        stream << element_value.GetEncodeValue();
        cur_seq++;
    }
    
    serialize_ttl(key, ttl, KEY_TYPE_LIST, stream);
    
    return serialize_value;
}

string serialize_hash(uint64_t ttl, const string& key, const map<string, string>& fv_map)
{
    string serialize_value;
    ByteStream stream(&serialize_value);
    uint32_t hash_cnt = (uint32_t)fv_map.size();
    uint32_t count = ttl ? 2 + hash_cnt : 1 + hash_cnt;
    
    stream.WriteVarUInt(count);
    
    // serialize meta key
    EncodeKey meta_key(KEY_TYPE_META, key);
    EncodeValue meta_value(KEY_TYPE_HASH, ttl, hash_cnt);
    stream << meta_key.GetEncodeKey();
    stream << meta_value.GetEncodeValue();
    
    // serialize hash field value map
    for (auto it = fv_map.begin(); it != fv_map.end(); it++) {
        EncodeKey hash_key(KEY_TYPE_HASH_FIELD, key, it->first);
        EncodeValue hash_value(KEY_TYPE_HASH_FIELD, it->second);
        
        stream << hash_key.GetEncodeKey();
        stream << hash_value.GetEncodeValue();
    }
    
    serialize_ttl(key, ttl, KEY_TYPE_HASH, stream);
    return serialize_value;
}

string serialize_set(uint64_t ttl, const string& key, const set<string>& member_set)
{
    string serialize_value;
    ByteStream stream(&serialize_value);
    uint32_t member_cnt = (uint32_t)member_set.size();
    uint32_t count = ttl ? 2 + member_cnt : 1 + member_cnt;

    stream.WriteVarUInt(count);
    
    // serialize meta key
    EncodeKey meta_key(KEY_TYPE_META, key);
    EncodeValue meta_value(KEY_TYPE_SET, ttl, member_cnt);
    stream << meta_key.GetEncodeKey();
    stream << meta_value.GetEncodeValue();
    
    for (const string& member : member_set) {
        EncodeKey member_key(KEY_TYPE_SET_MEMBER, key, member);
        EncodeValue member_value(KEY_TYPE_SET_MEMBER);
        
        stream << member_key.GetEncodeKey();
        stream << member_value.GetEncodeValue();
    }
    
    serialize_ttl(key, ttl, KEY_TYPE_SET, stream);
    
    return serialize_value;
}

string serialize_zset(uint64_t ttl, const string& key, const map<string, double>& score_map)
{
    string serialize_value;
    ByteStream stream(&serialize_value);
    uint32_t zset_cnt = (uint32_t)score_map.size();
    uint32_t count = ttl ? 2 + zset_cnt * 2 : 1 + zset_cnt * 2;
    
    stream.WriteVarUInt(count);
    
    // serialize meta key
    EncodeKey meta_key(KEY_TYPE_META, key);
    EncodeValue meta_value(KEY_TYPE_ZSET, ttl, zset_cnt);
    stream << meta_key.GetEncodeKey();
    stream << meta_value.GetEncodeValue();
    
    // serialize score map
    for (auto it = score_map.begin(); it != score_map.end(); it++) {
        uint64_t encode_score = double_to_uint64(it->second);
        EncodeKey member_key(KEY_TYPE_ZSET_SCORE, key, it->first);
        EncodeValue member_value(KEY_TYPE_ZSET_SCORE, encode_score);
        
        stream << member_key.GetEncodeKey();
        stream << member_value.GetEncodeValue();
        
        EncodeKey sort_key(KEY_TYPE_ZSET_SORT, key, encode_score, it->first);
        EncodeValue sort_vale(KEY_TYPE_ZSET_SORT);
        stream << sort_key.GetEncodeKey();
        stream << sort_vale.GetEncodeValue();
    }
    
    serialize_ttl(key, ttl, KEY_TYPE_ZSET, stream);
    return serialize_value;
}

string serialize_list_vec(uint64_t ttl, const string& key, const vector<string>& vec)
{
    string serialize_value;
    ByteStream stream(&serialize_value);
    uint32_t list_cnt = (uint32_t)vec.size();
    uint32_t count = ttl ? 2 + list_cnt : 1 + list_cnt;
    
    stream.WriteVarUInt(count);
    
    // serialize meta key
    EncodeKey meta_key(KEY_TYPE_META, key);
    uint64_t head_seq = 1;
    uint64_t tail_seq = list_cnt;
    uint64_t incr_seq = list_cnt + 1;
    EncodeValue meta_value(KEY_TYPE_LIST, ttl, list_cnt, head_seq, tail_seq, incr_seq);
    stream << meta_key.GetEncodeKey();
    stream << meta_value.GetEncodeValue();
    
    // serialize list element
    uint64_t cur_seq = 1;
    for (uint32_t i = 0; i < list_cnt; i++) {
        EncodeKey element_key(KEY_TYPE_LIST_ELEMENT, key, cur_seq);
        uint64_t prev_seq = cur_seq - 1;
        uint64_t next_seq = (cur_seq == list_cnt) ? 0 : cur_seq + 1;
        EncodeValue element_value(KEY_TYPE_LIST_ELEMENT, prev_seq, next_seq, vec[i]);
        
        stream << element_key.GetEncodeKey();
        stream << element_value.GetEncodeValue();
        cur_seq++;
    }
    
    serialize_ttl(key, ttl, KEY_TYPE_LIST, stream);
    
    return serialize_value;
}

string serialize_hash_vec(uint64_t ttl, const string& key, const vector<string>& vec)
{
    string serialize_value;
    ByteStream stream(&serialize_value);
    uint32_t hash_cnt = (uint32_t)vec.size() / 2;
    uint32_t count = ttl ? 2 + hash_cnt : 1 + hash_cnt;
    
    stream.WriteVarUInt(count);
    
    // serialize meta key
    EncodeKey meta_key(KEY_TYPE_META, key);
    EncodeValue meta_value(KEY_TYPE_HASH, ttl, hash_cnt);
    stream << meta_key.GetEncodeKey();
    stream << meta_value.GetEncodeValue();
    
    // serialize hash field value map
    uint32_t vec_cnt = (uint32_t)vec.size();
    for (uint32_t i = 0; i < vec_cnt; i += 2) {
        EncodeKey hash_key(KEY_TYPE_HASH_FIELD, key, vec[i]);
        EncodeValue hash_value(KEY_TYPE_HASH_FIELD, vec[i + 1]);
        
        stream << hash_key.GetEncodeKey();
        stream << hash_value.GetEncodeValue();
    }
    
    serialize_ttl(key, ttl, KEY_TYPE_HASH, stream);
    return serialize_value;
}

string serialize_zset_vec(uint64_t ttl, const string& key, const vector<string>& vec)
{
    string serialize_value;
    ByteStream stream(&serialize_value);
    uint32_t zset_cnt = (uint32_t)vec.size() / 2;
    uint32_t count = ttl ? 2 + zset_cnt * 2 : 1 + zset_cnt * 2;
    
    stream.WriteVarUInt(count);
    
    // serialize meta key
    EncodeKey meta_key(KEY_TYPE_META, key);
    EncodeValue meta_value(KEY_TYPE_ZSET, ttl, zset_cnt);
    stream << meta_key.GetEncodeKey();
    stream << meta_value.GetEncodeValue();
    
    // serialize score map
    uint32_t vec_cnt = (uint32_t)vec.size();
    for (uint32_t i = 0; i < vec_cnt; i += 2) {
        double score = 0;
        get_double_from_string(vec[i + 1], score);
        uint64_t encode_score = double_to_uint64(score);
        EncodeKey member_key(KEY_TYPE_ZSET_SCORE, key, vec[i]);
        EncodeValue member_value(KEY_TYPE_ZSET_SCORE, encode_score);
        
        stream << member_key.GetEncodeKey();
        stream << member_value.GetEncodeValue();
        
        EncodeKey sort_key(KEY_TYPE_ZSET_SORT, key, encode_score, vec[i]);
        EncodeValue sort_vale(KEY_TYPE_ZSET_SORT);
        stream << sort_key.GetEncodeKey();
        stream << sort_vale.GetEncodeValue();
    }
    
    serialize_ttl(key, ttl, KEY_TYPE_ZSET, stream);
    return serialize_value;
}
