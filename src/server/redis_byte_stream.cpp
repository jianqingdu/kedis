//
//  redis_byte_stream.cpp
//  kv-store
//
//  Created by ziteng on 16-6-14.
//  Copyright (c) 2016年 mgj. All rights reserved.
//

#include "redis_byte_stream.h"

RedisByteStream::RedisByteStream(char* data, int len)
{
    data_ = data;
    len_ = len;
    offset_ = 0;
}

RedisByteStream::~RedisByteStream()
{
    
}

int RedisByteStream::ReadByte()
{
    if (offset_ >= len_) {
        throw ParseRedisException(PARSE_REDIS_ERROR_NO_MORE_DATA, "no more data");
    }
    
    char ch = data_[offset_];
    offset_++;
    return ch;
}

void RedisByteStream::ReadLine(string& line)
{
    char* start_pos = data_ + offset_;
    char* pos = strstr(start_pos, "\r\n");
    if (!pos) {
        throw ParseRedisException(PARSE_REDIS_ERROR_NO_MORE_DATA, "no more data");
    }
    
    int size = (int)(pos - start_pos);
    line.append(start_pos, size);
    offset_ += size + 2;
}

void RedisByteStream::ReadBytes(int size, string& value)
{
    if (offset_ + size + 2 > len_) {
        throw ParseRedisException(PARSE_REDIS_ERROR_NO_MORE_DATA, "no more data");
    }
    
    char* start_pos = data_ + offset_;
    value.append(start_pos, size);
    offset_ += size + 2;
}
