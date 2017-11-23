//
//  encoding.cpp
//  kedis
//
//  Created by ziteng on 17/7/24.
//  Copyright © 2017年 mgj. All rights reserved.
//

#include "encoding.h"

string get_type_name(uint8_t key_type)
{
    switch (key_type) {
        case KEY_TYPE_STRING: return "string";
        case KEY_TYPE_HASH: return "hash";
        case KEY_TYPE_LIST: return "list";
        case KEY_TYPE_SET: return "set";
        case KEY_TYPE_ZSET: return "zset";
        default: return "unknown";
    }
}

EncodeKey::EncodeKey(uint8_t type, const string& key)
{
    stream_.SetSimpleBuffer(&buffer_);
    stream_ << type;
    if (type == KEY_TYPE_META) {
        stream_.WriteStringWithoutLen(key);
    } else {
        stream_ << key;
    }
}

EncodeKey::EncodeKey(uint8_t type, const string& key, const string& member)
{
    stream_.SetSimpleBuffer(&buffer_);
    stream_ << type;
    stream_ << key;
    stream_ << member;
}

EncodeKey::EncodeKey(uint8_t type, const string& key, uint64_t seq)
{
    stream_.SetSimpleBuffer(&buffer_);
    stream_ << type;
    stream_ << key;
    stream_ << seq;
}

EncodeKey::EncodeKey(uint8_t type, const string& key, uint64_t score, const string& member)
{
    stream_.SetSimpleBuffer(&buffer_);
    stream_ << type;
    stream_ << key;
    stream_ << score;
    stream_.WriteStringWithoutLen(member);  // member will be sorted by lexicographical ordering without a heading length
}

EncodeKey::EncodeKey(uint8_t type, uint64_t ttl, const string& key)
{
    stream_.SetSimpleBuffer(&buffer_);
    stream_ << type;
    stream_ << ttl;
    stream_ << key;
}

//======
EncodeValue::EncodeValue(uint8_t type, uint64_t ttl, const string& value)
{
    stream_.SetSimpleBuffer(&buffer_);
    stream_ << type;
    stream_ << ttl;
    stream_ << value;
}

EncodeValue::EncodeValue(uint8_t type, uint64_t ttl, uint64_t count)
{
    stream_.SetSimpleBuffer(&buffer_);
    stream_ << type;
    stream_ << ttl;
    stream_ << count;
}

EncodeValue::EncodeValue(uint8_t type, const string& value)
{
    stream_.SetSimpleBuffer(&buffer_);
    stream_ << type;
    stream_ << value;
}

EncodeValue::EncodeValue(uint8_t type, uint64_t ttl, uint64_t count, uint64_t head_seq, uint64_t tail_seq, uint64_t current_seq)
{
    stream_.SetSimpleBuffer(&buffer_);
    stream_ << type;
    stream_ << ttl;
    stream_ << count;
    stream_ << head_seq;
    stream_ << tail_seq;
    stream_ << current_seq;
}

EncodeValue::EncodeValue(uint8_t type, uint64_t prev_seq, uint64_t next_seq, const string& value)
{
    stream_.SetSimpleBuffer(&buffer_);
    stream_ << type;
    stream_ << prev_seq;
    stream_ << next_seq;
    stream_ << value;
}

EncodeValue::EncodeValue(uint8_t type)
{
    stream_.SetSimpleBuffer(&buffer_);
    stream_ << type;
}

EncodeValue::EncodeValue(uint8_t type, uint64_t score)
{
    stream_.SetSimpleBuffer(&buffer_);
    stream_ << type;
    stream_ << score;
}

//========
int DecodeKey::Decode(const string& encode_key, uint8_t expect_type, string& key)
{
    ByteStream stream((uchar_t*)encode_key.data(), (uint32_t)encode_key.size());
    
    try {
        uint8_t type;
        stream >> type;
        if (type != expect_type) {
            return kDecodeErrorType;
        }
        
        if (type == KEY_TYPE_META) {
            stream.ReadStringWithoutLen(key);
        } else {
            stream >> key;
        }
        return kDecodeOK;
    } catch (ParseException ex) {
        return kDecodeErrorFormat;
    }
}

int DecodeKey::Decode(const string& encode_key, uint8_t expect_type, string& key, string& member)
{
    ByteStream stream((uchar_t*)encode_key.data(), (uint32_t)encode_key.size());
    
    try {
        uint8_t type;
        stream >> type;
        if (type != expect_type) {
            return kDecodeErrorType;
        }
        
        stream >> key;
        stream >> member;
        return kDecodeOK;
    } catch (ParseException ex) {
        return kDecodeErrorFormat;
    }
}

int DecodeKey::Decode(const string& encode_key, uint8_t expect_type, string& key, uint64_t& seq)
{
    ByteStream stream((uchar_t*)encode_key.data(), (uint32_t)encode_key.size());
    
    try {
        uint8_t type;
        stream >> type;
        if (type != expect_type) {
            return kDecodeErrorType;
        }
        
        stream >> key;
        stream >> seq;
        return kDecodeOK;
    } catch (ParseException ex) {
        return kDecodeErrorFormat;
    }
}

int DecodeKey::Decode(const string& encode_key, uint8_t expect_type, string& key, uint64_t& score, string& member)
{
    ByteStream stream((uchar_t*)encode_key.data(), (uint32_t)encode_key.size());
    
    try {
        uint8_t type;
        stream >> type;
        if (type != expect_type) {
            return kDecodeErrorType;
        }
        
        stream >> key;
        stream >> score;
        stream.ReadStringWithoutLen(member);
        return kDecodeOK;
    } catch (ParseException ex) {
        return kDecodeErrorFormat;
    }
}

int DecodeKey::Decode(const string& encode_key, uint8_t expect_type, uint64_t& ttl, string& key)
{
    ByteStream stream((uchar_t*)encode_key.data(), (uint32_t)encode_key.size());
    
    try {
        uint8_t type;
        stream >> type;
        if (type != expect_type) {
            return kDecodeErrorType;
        }
        
        stream >> ttl;
        stream >> key;
        return kDecodeOK;
    } catch (ParseException ex) {
        return kDecodeErrorFormat;
    }
}

//========
int DecodeValue::Decode(const string& encode_value, uint8_t expect_type, uint64_t& ttl, string& value)
{
    ByteStream stream((uchar_t*)encode_value.data(), (uint32_t)encode_value.size());
    
    try {
        uint8_t type;
        stream >> type;
        if (type != expect_type) {
            return kDecodeErrorType;
        }
        
        stream >> ttl;
        stream >> value;
        return kDecodeOK;
    } catch (ParseException ex) {
        return kDecodeErrorFormat;
    }
}

int DecodeValue::Decode(const string& encode_value, uint8_t expect_type, uint64_t& ttl, uint64_t& count)
{
    ByteStream stream((uchar_t*)encode_value.data(), (uint32_t)encode_value.size());
    
    try {
        uint8_t type;
        stream >> type;
        if (type != expect_type) {
            return kDecodeErrorType;
        }
        
        stream >> ttl;
        stream >> count;
        return kDecodeOK;
    } catch (ParseException ex) {
        return kDecodeErrorFormat;
    }
}

int DecodeValue::Decode(const string& encode_value, uint8_t expect_type, string& value)
{
    ByteStream stream((uchar_t*)encode_value.data(), (uint32_t)encode_value.size());
    
    try {
        uint8_t type;
        stream >> type;
        if (type != expect_type) {
            return kDecodeErrorType;
        }
        
        stream >> value;
        return kDecodeOK;
    } catch (ParseException ex) {
        return kDecodeErrorFormat;
    }
}

int DecodeValue::Decode(const string& encode_value, uint8_t expect_type, uint64_t& ttl, uint64_t& count,
                  uint64_t& head_seq, uint64_t& tail_seq, uint64_t& current_seq)
{
    ByteStream stream((uchar_t*)encode_value.data(), (uint32_t)encode_value.size());
    
    try {
        uint8_t type;
        stream >> type;
        if (type != expect_type) {
            return kDecodeErrorType;
        }
        
        stream >> ttl;
        stream >> count;
        stream >> head_seq;
        stream >> tail_seq;
        stream >> current_seq;
        return kDecodeOK;
    } catch (ParseException ex) {
        return kDecodeErrorFormat;
    }
}

int DecodeValue::Decode(const string& encode_value, uint8_t expect_type, uint64_t& prev_seq, uint64_t& next_seq, string& value)
{
    ByteStream stream((uchar_t*)encode_value.data(), (uint32_t)encode_value.size());
    
    try {
        uint8_t type;
        stream >> type;
        if (type != expect_type) {
            return kDecodeErrorType;
        }
        
        stream >> prev_seq;
        stream >> next_seq;
        stream >> value;
        return kDecodeOK;
    } catch (ParseException ex) {
        return kDecodeErrorFormat;
    }
}

int DecodeValue::Decode(const string& encode_value, uint8_t expect_type)
{
    ByteStream stream((uchar_t*)encode_value.data(), (uint32_t)encode_value.size());
    
    try {
        uint8_t type;
        stream >> type;
        if (type != expect_type) {
            return kDecodeErrorType;
        }
        
        return kDecodeOK;
    } catch (ParseException ex) {
        return kDecodeErrorFormat;
    }
}

int DecodeValue::Decode(const string& encode_value, uint8_t expect_type, uint64_t& score)
{
    ByteStream stream((uchar_t*)encode_value.data(), (uint32_t)encode_value.size());
    
    try {
        uint8_t type;
        stream >> type;
        if (type != expect_type) {
            return kDecodeErrorType;
        }
        
        stream >> score;
        return kDecodeOK;
    } catch (ParseException ex) {
        return kDecodeErrorFormat;
    }
}
